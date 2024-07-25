import uuid
from functools import wraps
from typing import Any, Callable, Optional, Protocol, TypedDict, TypeVar, cast

import dbos_transact.utils as utils
from dbos_transact.transaction import TransactionContext
from dbos_transact.workflows import WorkflowContext

from .application_database import ApplicationDatabase, TransactionResultInternal
from .dbos_config import ConfigFile, load_config
from .logger import config_logger, dbos_logger
from .system_database import (
    SystemDatabase,
    WorkflowInputs,
    WorkflowStatusInternal,
    WorkflowStatusString,
)


class WorkflowProtocol(Protocol):
    __qualname__: str

    def __call__(self, ctx: WorkflowContext, *args: Any, **kwargs: Any) -> Any: ...


Workflow = TypeVar("Workflow", bound=WorkflowProtocol)


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

    def destroy(self) -> None:
        self.sys_db.destroy()
        self.app_db.destroy()

    def workflow(self) -> Callable[[Workflow], Workflow]:
        def decorator(func: Workflow) -> Workflow:
            @wraps(func)
            def wrapper(_ctxt: WorkflowContext, *args: Any, **kwargs: Any) -> Any:
                input_ctxt = cast(WorkflowInputContext, _ctxt)
                workflow_uuid = input_ctxt["workflow_uuid"]
                status: WorkflowStatusInternal = {
                    "workflow_uuid": workflow_uuid,
                    "status": WorkflowStatusString.PENDING.value,
                    "name": func.__qualname__,
                    "output": None,
                    "error": None,
                }
                self.sys_db.update_workflow_status(status)

                inputs: WorkflowInputs = {
                    "args": args,
                    "kwargs": kwargs,
                }
                self.sys_db.update_workflow_inputs(
                    workflow_uuid, utils.serialize(inputs)
                )

                ctx = WorkflowContext(workflow_uuid)

                try:
                    output = func(ctx, *args, **kwargs)
                except Exception as error:
                    status["status"] = WorkflowStatusString.ERROR.value
                    status["error"] = utils.serialize(error)
                    self.sys_db.update_workflow_status(status)
                    raise error

                status["status"] = WorkflowStatusString.SUCCESS.value
                status["output"] = utils.serialize(output)
                self.sys_db.update_workflow_status(status)
                return output

            return cast(Workflow, wrapper)

        return decorator

    def wf_ctx(self, workflow_uuid: Optional[str] = None) -> WorkflowContext:
        workflow_uuid = workflow_uuid if workflow_uuid else str(uuid.uuid4())
        input_ctxt: WorkflowInputContext = {"workflow_uuid": workflow_uuid}
        return cast(WorkflowContext, input_ctxt)

    def transaction(self) -> Callable[[Transaction], Transaction]:
        def decorator(func: Transaction) -> Transaction:
            @wraps(func)
            def wrapper(_ctxt: TransactionContext, *args: Any, **kwargs: Any) -> Any:
                input_ctxt = cast(WorkflowContext, _ctxt)
                input_ctxt.function_id += 1
                with self.app_db.sessionmaker() as session:
                    try:
                        # TODO: support multiple isolation levels
                        with session.begin():
                            session.connection(
                                execution_options={"isolation_level": "SERIALIZABLE"}
                            )
                            txn_ctxt = TransactionContext(
                                session, input_ctxt.function_id
                            )
                            # TODO: Check transaction output for OAOO
                            txn_output: TransactionResultInternal = {
                                "workflow_uuid": input_ctxt.workflow_uuid,
                                "function_id": input_ctxt.function_id,
                                "output": None,
                                "error": None,
                                "txn_snapshot": "",
                                "executor_id": None,
                                "txn_id": None,
                            }
                            output = func(txn_ctxt, *args, **kwargs)
                            ApplicationDatabase.record_transaction_output(
                                txn_ctxt.session, txn_output
                            )
                    except Exception as error:
                        # TODO: handle serialization errors properly
                        txn_output["error"] = utils.serialize(error)
                        self.app_db.record_transaction_error(txn_output)
                        raise error
                return output

            return cast(Transaction, wrapper)

        return decorator
