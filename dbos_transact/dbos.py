import uuid
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import (
    Any,
    Callable,
    Optional,
    ParamSpec,
    Protocol,
    Tuple,
    TypeAlias,
    TypedDict,
    TypeVar,
    cast,
)

import dbos_transact.utils as utils
from dbos_transact.error import DBOSRecoveryError, DBOSWorkflowConflictUUIDError
from dbos_transact.transaction import TransactionContext
from dbos_transact.workflows import WorkflowContext, WorkflowHandle

from .application_database import ApplicationDatabase, TransactionResultInternal
from .dbos_config import ConfigFile, load_config
from .logger import config_logger, dbos_logger
from .system_database import SystemDatabase, WorkflowInputs, WorkflowStatusInternal

P = ParamSpec("P")
R = TypeVar("R", covariant=True)


class WorkflowProtocol(Protocol[P, R]):
    __qualname__: str

    def __call__(
        self, ctx: WorkflowContext, *args: P.args, **kwargs: P.kwargs
    ) -> R: ...


Workflow: TypeAlias = WorkflowProtocol[P, R]


class TransactionProtocol(Protocol):
    __qualname__: str

    def __call__(self, ctx: TransactionContext, *args: Any, **kwargs: Any) -> Any: ...


Transaction = TypeVar("Transaction", bound=TransactionProtocol)


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

    def destroy(self) -> None:
        self.sys_db.destroy()
        self.app_db.destroy()

    def workflow(self) -> Callable[[Workflow[P, R]], Workflow[P, R]]:
        def decorator(func: Workflow[P, R]) -> Workflow[P, R]:
            func.__orig_func = func  # type: ignore

            @wraps(func)
            def wrapper(
                input_ctxt: WorkflowContext, *args: P.args, **kwargs: P.kwargs
            ) -> R:
                inputs: WorkflowInputs = {
                    "args": args,
                    "kwargs": kwargs,
                }
                ctx, status = self._init_workflow(
                    input_ctxt=cast(WorkflowInputContext, input_ctxt),
                    inputs=inputs,
                    wf_name=func.__qualname__,
                )

                try:
                    output = func(ctx, *args, **kwargs)
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

            wrapped_func = cast(Workflow[P, R], wrapper)
            self.workflow_info_map[func.__qualname__] = wrapped_func
            return wrapped_func

        return decorator

    def start_workflow(
        self,
        func: Workflow[P, R],
        input_ctxt: WorkflowContext,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> WorkflowHandle[R]:
        func = cast(Workflow[P, R], func.__orig_func)  # type: ignore
        inputs: WorkflowInputs = {
            "args": args,
            "kwargs": kwargs,
        }
        ctx, status = self._init_workflow(
            input_ctxt=cast(WorkflowInputContext, input_ctxt),
            inputs=inputs,
            wf_name=func.__qualname__,
        )

        def execute_workflow() -> R:
            try:
                output = func(ctx, *args, **kwargs)
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

        future = self.executor.submit(execute_workflow)
        return WorkflowHandle(status["workflow_uuid"], future)

    def wf_ctx(self, workflow_uuid: Optional[str] = None) -> WorkflowContext:
        workflow_uuid = workflow_uuid if workflow_uuid else str(uuid.uuid4())
        input_ctxt: WorkflowInputContext = {"workflow_uuid": workflow_uuid}
        return cast(WorkflowContext, input_ctxt)

    def _init_workflow(
        self, input_ctxt: WorkflowInputContext, inputs: WorkflowInputs, wf_name: str
    ) -> Tuple[WorkflowContext, WorkflowStatusInternal]:
        workflow_uuid = input_ctxt["workflow_uuid"]
        status: WorkflowStatusInternal = {
            "workflow_uuid": workflow_uuid,
            "status": "PENDING",
            "name": wf_name,
            "output": None,
            "error": None,
        }
        self.sys_db.update_workflow_status(status)

        self.sys_db.update_workflow_inputs(workflow_uuid, utils.serialize(inputs))

        ctx = WorkflowContext(workflow_uuid)
        return ctx, status

    def transaction(self) -> Callable[[Transaction], Transaction]:
        def decorator(func: Transaction) -> Transaction:
            @wraps(func)
            def wrapper(_ctxt: TransactionContext, *args: Any, **kwargs: Any) -> Any:
                input_ctxt = cast(WorkflowContext, _ctxt)
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
                            txn_ctxt = TransactionContext(
                                session, input_ctxt.function_id
                            )
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
                            ApplicationDatabase.record_transaction_output(
                                txn_ctxt.session, txn_output
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

    def execute_workflow_uuid(self, workflow_uuid: str) -> None:
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
        ctx = self.wf_ctx(workflow_uuid)
        try:
            wf_func(ctx, *inputs["args"], **inputs["kwargs"])
        except Exception as error:
            # Don't raise the error because it's in recovery mode
            dbos_logger.error(f"Error executing workflow by UUID: {error}")

    def recover_pending_workflows(self) -> None:
        """
        Find all PENDING workflows and execute them.
        """
        # TODO: need to run this in a background thread, after everything is initialized and all functions are properly decorated.
        # Therefore, cannot run in the constructor
        workflowIDs = self.sys_db.get_pending_workflows()
        dbos_logger.debug(f"Pending workflows: {workflowIDs}")
        for workflowID in workflowIDs:
            self.execute_workflow_uuid(workflowID)

        dbos_logger.info("Recovered pending workflows")
