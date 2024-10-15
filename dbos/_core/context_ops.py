import asyncio
from typing import TYPE_CHECKING, Any, Optional

from ..context import EnterDBOSStep, TracedAttributes, get_local_dbos_context
from ..error import DBOSException
from ..registrations import set_dbos_func_name, set_temp_workflow_type
from .decorator_ops import workflow_wrapper
from .types import GetEventWorkflowContext

if TYPE_CHECKING:
    from ..dbos import DBOS, _DBOSRegistry


TEMP_SEND_WF_NAME = "<temp>.temp_send_workflow"


def register_send_workflow(dbos: "DBOS", registry: "_DBOSRegistry") -> None:
    def send_temp_workflow(
        destination_id: str, message: Any, topic: Optional[str]
    ) -> None:
        dbos.send(destination_id, message, topic)

    temp_send_wf = workflow_wrapper(registry, send_temp_workflow)
    set_dbos_func_name(send_temp_workflow, TEMP_SEND_WF_NAME)
    set_temp_workflow_type(send_temp_workflow, "send")
    registry.register_wf_function(TEMP_SEND_WF_NAME, temp_send_wf)


def send_sync(
    dbos: "DBOS", destination_id: str, message: Any, topic: Optional[str] = None
) -> None:
    def do_send(destination_id: str, message: Any, topic: Optional[str]) -> None:
        attributes: TracedAttributes = {
            "name": "send",
        }
        with EnterDBOSStep(attributes) as ctx:
            dbos._sys_db.send_sync(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                destination_id,
                message,
                topic,
            )

    ctx = get_local_dbos_context()
    if ctx and ctx.is_within_workflow():
        assert ctx.is_workflow(), "send() must be called from within a workflow"
        return do_send(destination_id, message, topic)
    else:
        wffn = dbos._registry.workflow_info_map.get(TEMP_SEND_WF_NAME)
        assert wffn
        wffn(destination_id, message, topic)


async def send_async(
    dbos: "DBOS", destination_id: str, message: Any, topic: Optional[str] = None
) -> None:
    async def do_send(destination_id: str, message: Any, topic: Optional[str]) -> None:
        attributes: TracedAttributes = {
            "name": "send",
        }
        with EnterDBOSStep(attributes) as ctx:
            await dbos._sys_db.send_async(
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
        await asyncio.to_thread(wffn, destination_id, message, topic)


def receive_sync(
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
            return dbos._sys_db.recv_sync(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                timeout_function_id,
                topic,
                timeout_seconds,
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("recv() must be called from within a workflow")


async def receive_async(
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
            return await dbos._sys_db.recv_async(
                ctx.workflow_id,
                ctx.curr_step_function_id,
                timeout_function_id,
                topic,
                timeout_seconds,
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("recv() must be called from within a workflow")


def set_event_sync(dbos: "DBOS", key: str, value: Any) -> None:
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
            dbos._sys_db.set_event_sync(
                ctx.workflow_id, ctx.curr_step_function_id, key, value
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("set_event() must be called from within a workflow")


async def set_event_async(dbos: "DBOS", key: str, value: Any) -> None:
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
            await dbos._sys_db.set_event_async(
                ctx.workflow_id, ctx.curr_step_function_id, key, value
            )
    else:
        # Cannot call it from outside of a workflow
        raise DBOSException("set_event() must be called from within a workflow")


def get_event_sync(
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
            return dbos._sys_db.get_event_sync(
                workflow_id, key, timeout_seconds, caller_ctx
            )
    else:
        # Directly call it outside of a workflow
        return dbos._sys_db.get_event_sync(workflow_id, key, timeout_seconds)


async def get_event_async(
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
            return await dbos._sys_db.get_event_async(
                workflow_id, key, timeout_seconds, caller_ctx
            )
    else:
        # Directly call it outside of a workflow
        return await dbos._sys_db.get_event_async(workflow_id, key, timeout_seconds)
