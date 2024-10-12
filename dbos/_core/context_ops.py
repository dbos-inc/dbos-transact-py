from typing import TYPE_CHECKING, Any, Optional

from ..context import EnterDBOSStep, TracedAttributes, get_local_dbos_context
from ..error import DBOSException
from ..registrations import set_dbos_func_name, set_temp_workflow_type
from ..system_database import GetEventWorkflowContext
from .decorator_ops import workflow_wrapper

if TYPE_CHECKING:
    from ..dbos import DBOS, _DBOSRegistry


TEMP_SEND_WF_NAME = "<temp>.temp_send_workflow"


def register_send_workflow(dbos: "DBOS", registry: "_DBOSRegistry") -> None:
    async def send_temp_workflow(
        destination_id: str, message: Any, topic: Optional[str]
    ) -> None:
        await dbos.send_async(destination_id, message, topic)

    temp_send_wf = workflow_wrapper(registry, send_temp_workflow)
    set_dbos_func_name(send_temp_workflow, TEMP_SEND_WF_NAME)
    set_temp_workflow_type(send_temp_workflow, "send")
    registry.register_wf_function(TEMP_SEND_WF_NAME, temp_send_wf)


async def send(
    dbos: "DBOS", destination_id: str, message: Any, topic: Optional[str] = None
) -> None:
    async def do_send(destination_id: str, message: Any, topic: Optional[str]) -> None:
        attributes: TracedAttributes = {
            "name": "send",
        }
        with EnterDBOSStep(attributes) as ctx:
            await dbos._sys_db.send(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                destination_id,
                message,
                topic,
            )

    ctx = get_local_dbos_context()
    if ctx and ctx.is_within_workflow():
        assert ctx.is_workflow(), "send() must be called from within a workflow"
        return await do_send(destination_id, message, topic)
    else:
        wffn = dbos._registry.workflow_info_map.get(TEMP_SEND_WF_NAME)
        assert wffn
        await wffn(destination_id, message, topic)


async def receive(
    dbos: "DBOS", topic: Optional[str] = None, timeout_seconds: float = 60
) -> Any:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None:
        # Must call it within a workflow
        assert cur_ctx.is_workflow(), "recv() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "recv",
        }
        with EnterDBOSStep(attributes) as ctx:
            ctx.function_id += 1  # Reserve for the sleep
            timeout_function_id = ctx.function_id
            return await dbos._sys_db.recv(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                timeout_function_id,
                topic,
                timeout_seconds,
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("recv() must be called from within a workflow")


async def set_event(dbos: "DBOS", key: str, value: Any) -> None:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None:
        # Must call it within a workflow
        assert (
            cur_ctx.is_workflow()
        ), "set_event() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "set_event",
        }
        with EnterDBOSStep(attributes) as ctx:
            await dbos._sys_db.set_event(
                ctx.workflow_id, ctx.curr_step_function_id, key, value
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("set_event() must be called from within a workflow")


async def get_event(
    dbos: "DBOS", workflow_id: str, key: str, timeout_seconds: float = 60
) -> Any:
    cur_ctx = get_local_dbos_context()
    if cur_ctx is not None and cur_ctx.is_within_workflow():
        # Call it within a workflow
        assert (
            cur_ctx.is_workflow()
        ), "get_event() must be called from within a workflow"
        attributes: TracedAttributes = {
            "name": "get_event",
        }
        with EnterDBOSStep(attributes) as ctx:
            ctx.function_id += 1
            timeout_function_id = ctx.function_id
            caller_ctx: GetEventWorkflowContext = {
                "workflow_uuid": ctx.workflow_id,
                "function_id": ctx.curr_step_function_id,
                "timeout_function_id": timeout_function_id,
            }
            return await dbos._sys_db.get_event(
                workflow_id, key, timeout_seconds, caller_ctx
            )
    else:
        # Directly call it outside of a workflow
        return await dbos._sys_db.get_event(workflow_id, key, timeout_seconds)
