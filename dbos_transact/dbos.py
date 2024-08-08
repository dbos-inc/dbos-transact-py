import os
import sys
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import (
    Any,
    Callable,
    List,
    Optional,
    Protocol,
    Tuple,
    TypedDict,
    TypeVar,
    cast,
)

if sys.version_info < (3, 10):
    from typing_extensions import ParamSpec, TypeAlias
else:
    from typing import ParamSpec, TypeAlias

import dbos_transact.utils as utils
from dbos_transact.admin_sever import AdminServer
from dbos_transact.context import (
    DBOSContext,
    DBOSContextSwap,
    EnterDBOSWorkflow,
    SetWorkflowUUID,
    assertCurrentDBOSContext,
    getThreadLocalDBOSContext,
)
from dbos_transact.error import DBOSRecoveryError, DBOSWorkflowConflictUUIDError
from dbos_transact.workflow import WorkflowHandle

from .application_database import ApplicationDatabase, TransactionResultInternal
from .dbos_config import ConfigFile, load_config
from .logger import config_logger, dbos_logger
from .system_database import (
    OperationResultInternal,
    SystemDatabase,
    WorkflowInputs,
    WorkflowStatusInternal,
)

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


class WorkflowProtocol(Protocol[P, R]):
    __qualname__: str

    def __call__(self, *args: P.args, **kwargs: P.kwargs) -> R: ...


Workflow: TypeAlias = WorkflowProtocol[P, R]


class TransactionProtocol(Protocol):
    __qualname__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


Transaction = TypeVar("Transaction", bound=TransactionProtocol)


class CommunicatorProtocol(Protocol):
    __qualname__: str

    def __call__(self, *args: Any, **kwargs: Any) -> Any: ...


Communicator = TypeVar("Communicator", bound=CommunicatorProtocol)


class WorkflowInputContext(TypedDict):
    workflow_uuid: str


class DBOS:
    def __init__(self, config: Optional[ConfigFile] = None) -> None:
        if config is None:
            config = load_config()
        config_logger(config)
        dbos_logger.info("Initializing DBOS!")
        self.config = config
        self.sys_db = SystemDatabase(config)
        self.app_db = ApplicationDatabase(config)
        self.workflow_info_map: dict[str, WorkflowProtocol[Any, Any]] = {}
        self.executor = ThreadPoolExecutor(max_workers=64)
        self.admin_server = AdminServer(dbos=self)

    def destroy(self) -> None:
        self.sys_db.destroy()
        self.app_db.destroy()
        self.admin_server.stop()

    def workflow(self) -> Callable[[Workflow[P, R]], Workflow[P, R]]:
        def decorator(func: Workflow[P, R]) -> Workflow[P, R]:
            func.__orig_func = func  # type: ignore

            @wraps(func)
            def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
                inputs: WorkflowInputs = {
                    "args": args,
                    "kwargs": kwargs,
                }
                with EnterDBOSWorkflow():
                    ctx = assertCurrentDBOSContext()
                    status = self._init_workflow(
                        ctx,
                        inputs=inputs,
                        wf_name=func.__qualname__,
                    )

                    return self._execute_workflow(status, func, *args, **kwargs)

            wrapped_func = cast(Workflow[P, R], wrapper)
            self.workflow_info_map[func.__qualname__] = wrapped_func
            return wrapped_func

        return decorator

    def start_workflow(
        self,
        func: Workflow[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]:
        func = cast(Workflow[P, R], func.__orig_func)  # type: ignore
        inputs: WorkflowInputs = {
            "args": args,
            "kwargs": kwargs,
        }

        cur_ctx = getThreadLocalDBOSContext()
        new_wf_ctx = DBOSContext() if cur_ctx is None else cur_ctx.create_child()
        new_wf_id = new_wf_ctx.assign_workflow_id()

        status = self._init_workflow(
            new_wf_ctx,
            inputs=inputs,
            wf_name=func.__qualname__,
        )
        future = self.executor.submit(
            cast(Callable[..., R], self._execute_workflow_wthread),
            status,
            func,
            new_wf_ctx,
            *args,
            **kwargs,
        )
        return WorkflowHandle(new_wf_ctx.next_workflow_uuid, future)

    def _init_workflow(
        self, ctx: DBOSContext, inputs: WorkflowInputs, wf_name: str
    ) -> WorkflowStatusInternal:
        status: WorkflowStatusInternal = {
            "workflow_uuid": ctx.workflow_uuid,
            "status": "PENDING",
            "name": wf_name,
            "output": None,
            "error": None,
            "app_id": ctx.app_id,
            "app_version": ctx.app_version,
            "executor_id": ctx.executor_id,
        }
        self.sys_db.update_workflow_status(status)

        self.sys_db.update_workflow_inputs(ctx.workflow_uuid, utils.serialize(inputs))

        return status

    def _execute_workflow_wthread(
        self,
        status: WorkflowStatusInternal,
        func: Workflow[P, R],
        ctx: DBOSContext,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        with DBOSContextSwap(ctx):
            with EnterDBOSWorkflow():
                return self._execute_workflow(status, func, *args, **kwargs)

    def _execute_workflow(
        self,
        status: WorkflowStatusInternal,
        func: Workflow[P, R],
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> R:
        try:
            output = func(*args, **kwargs)
        except DBOSWorkflowConflictUUIDError as wferror:
            # TODO: handle this properly by waiting/returning the output
            raise wferror
        except Exception as error:
            status["status"] = "ERROR"
            status["error"] = utils.serialize(error)
            self.sys_db.update_workflow_status(status)
            raise error

        status["status"] = "SUCCESS"
        status["output"] = utils.serialize(output)
        self.sys_db.update_workflow_status(status)
        return output

    def transaction(self) -> Callable[[Transaction], Transaction]:
        def decorator(func: Transaction) -> Transaction:
            @wraps(func)
            def wrapper(_ctxt: DBOSContext, *args: Any, **kwargs: Any) -> Any:
                input_ctxt = _ctxt
                input_ctxt.function_id += 1
                with self.app_db.sessionmaker() as session:
                    txn_output: TransactionResultInternal = {
                        "workflow_uuid": input_ctxt.workflow_uuid,
                        "function_id": input_ctxt.function_id,
                        "output": None,
                        "error": None,
                        "txn_snapshot": "",  # TODO: add actual snapshot
                        "executor_id": None,
                        "txn_id": None,
                    }
                    has_recorded_error = False
                    try:
                        # TODO: support multiple isolation levels
                        # TODO: handle serialization errors properly
                        with session.begin():
                            # This must be the first statement in the transaction!
                            session.connection(
                                execution_options={"isolation_level": "REPEATABLE READ"}
                            )
                            # TODO txn_ctxt = TransactionContext(session, input_ctxt.function_id)
                            txn_ctxt = input_ctxt
                            txn_ctxt.start_transaction(session, input_ctxt.function_id)
                            # Check recorded output for OAOO
                            recorded_output = (
                                ApplicationDatabase.check_transaction_execution(
                                    session,
                                    input_ctxt.workflow_uuid,
                                    txn_ctxt.function_id,
                                )
                            )
                            if recorded_output:
                                if recorded_output["error"]:
                                    deserialized_error = utils.deserialize(
                                        recorded_output["error"]
                                    )
                                    has_recorded_error = True
                                    raise deserialized_error
                                elif recorded_output["output"]:
                                    return utils.deserialize(recorded_output["output"])
                                else:
                                    raise Exception("Output and error are both None")
                            output = func(txn_ctxt, *args, **kwargs)
                            txn_output["output"] = utils.serialize(output)
                            assert txn_ctxt.sql_session is not None
                            ApplicationDatabase.record_transaction_output(
                                txn_ctxt.sql_session, txn_output
                            )

                    except Exception as error:
                        # Don't record the error if it was already recorded
                        if not has_recorded_error:
                            txn_output["error"] = utils.serialize(error)
                            self.app_db.record_transaction_error(txn_output)
                        raise error
                return output

            return cast(Transaction, wrapper)

        return decorator

    def communicator(self) -> Callable[[Communicator], Communicator]:
        def decorator(func: Communicator) -> Communicator:
            @wraps(func)
            def wrapper(_ctxt: DBOSContext, *args: Any, **kwargs: Any) -> Any:
                input_ctxt = _ctxt
                input_ctxt.function_id += 1
                # TODO comm_ctxt = CommunicatorContext(input_ctxt.function_id)
                comm_ctxt = input_ctxt
                comm_ctxt.start_communicator(input_ctxt.function_id)
                comm_output: OperationResultInternal = {
                    "workflow_uuid": input_ctxt.workflow_uuid,
                    "function_id": input_ctxt.function_id,
                    "output": None,
                    "error": None,
                }
                recorded_output = self.sys_db.check_operation_execution(
                    input_ctxt.workflow_uuid, comm_ctxt.function_id
                )
                if recorded_output:
                    if recorded_output["error"]:
                        deserialized_error = utils.deserialize(recorded_output["error"])
                        raise deserialized_error
                    elif recorded_output["output"]:
                        return utils.deserialize(recorded_output["output"])
                    else:
                        raise Exception("Output and error are both None")
                output = None
                try:
                    # TODO: support configurable retries
                    output = func(comm_ctxt, *args, **kwargs)
                    comm_output["output"] = utils.serialize(output)
                except Exception as error:
                    comm_output["error"] = utils.serialize(error)
                    raise error
                finally:
                    self.sys_db.record_operation_result(comm_output)
                return output

            return cast(Communicator, wrapper)

        return decorator

    def execute_workflow_uuid(self, workflow_uuid: str) -> WorkflowHandle[Any]:
        """
        This function is used to execute a workflow by a UUID for recovery.
        """
        status = self.sys_db.get_workflow_status(workflow_uuid)
        if not status:
            raise DBOSRecoveryError(workflow_uuid, "Workflow status not found")
        inputs = self.sys_db.get_workflow_inputs(workflow_uuid)
        if not inputs:
            raise DBOSRecoveryError(workflow_uuid, "Workflow inputs not found")
        wf_func = self.workflow_info_map[status["name"]]
        if not wf_func:
            raise DBOSRecoveryError(workflow_uuid, "Workflow function not found")
        with SetWorkflowUUID(workflow_uuid):
            return self.start_workflow(wf_func, *inputs["args"], **inputs["kwargs"])

    def recover_pending_workflows(
        self, executor_ids: List[str] = ["local"]
    ) -> List[WorkflowHandle[Any]]:
        """
        Find all PENDING workflows and execute them.
        """
        workflow_handles: List[WorkflowHandle[Any]] = []
        for executor_id in executor_ids:
            if executor_id == "local" and os.environ.get("DBOS__VMID"):
                dbos_logger.debug(
                    f"Skip local recovery because it's running in a VM: {os.environ.get('DBOS__VMID')}"
                )
            dbos_logger.debug(
                f"Recovering pending workflows for executor: {executor_id}"
            )
            workflow_ids = self.sys_db.get_pending_workflows(executor_id)
            dbos_logger.debug(f"Pending workflows: {workflow_ids}")

            for workflowID in workflow_ids:
                handle = self.execute_workflow_uuid(workflowID)
                workflow_handles.append(handle)
        # TODO: need to run this in a background thread, after everything is initialized and all functions are properly decorated.
        # Therefore, cannot run in the constructor

        dbos_logger.info("Recovered pending workflows")
        return workflow_handles
