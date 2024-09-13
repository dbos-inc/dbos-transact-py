from typing import TYPE_CHECKING, Any, Optional, Tuple

from dbos.context import DBOSContext, get_local_dbos_context
from dbos.core import P, R, _init_workflow, _WorkflowHandlePolling
from dbos.dbos import WorkflowHandle
from dbos.error import DBOSWorkflowFunctionNotFoundError
from dbos.registrations import (
    get_config_name,
    get_dbos_class_name,
    get_dbos_func_name,
    get_func_info,
    get_temp_workflow_type,
)
from dbos.system_database import WorkflowInputs

if TYPE_CHECKING:
    from dbos.dbos import DBOS, Workflow, WorkflowHandle


class Queue:

    def __init__(self, dbos: "DBOS", name: str, concurrency: Optional[int]) -> None:
        self.dbos = dbos
        self.name = name
        self.concurrency = concurrency

    def enqueue(
        self, func: "Workflow[P, R]", *args: P.args, **kwargs: P.kwargs
    ) -> WorkflowHandle[R]:
        fself: Optional[object] = None
        if hasattr(func, "__self__"):
            fself = func.__self__

        fi = get_func_info(func)
        if fi is None:
            raise DBOSWorkflowFunctionNotFoundError(
                "<NONE>", f"start_workflow: function {func.__name__} is not registered"
            )

        inputs: WorkflowInputs = {
            "args": args,
            "kwargs": kwargs,
        }

        cur_ctx = get_local_dbos_context()
        if cur_ctx is not None and cur_ctx.is_within_workflow():
            assert cur_ctx.is_workflow()  # Not in a step
            cur_ctx.function_id += 1
            if len(cur_ctx.id_assigned_for_next_workflow) == 0:
                cur_ctx.id_assigned_for_next_workflow = (
                    cur_ctx.workflow_id + "-" + str(cur_ctx.function_id)
                )

        new_wf_ctx = DBOSContext() if cur_ctx is None else cur_ctx.create_child()
        new_wf_ctx.id_assigned_for_next_workflow = new_wf_ctx.assign_workflow_id()
        new_wf_id = new_wf_ctx.id_assigned_for_next_workflow

        gin_args: Tuple[Any, ...] = args
        if fself is not None:
            gin_args = (fself,)

        _init_workflow(
            self.dbos,
            new_wf_ctx,
            inputs=inputs,
            wf_name=get_dbos_func_name(func),
            class_name=get_dbos_class_name(fi, func, gin_args),
            config_name=get_config_name(fi, func, gin_args),
            temp_wf_type=get_temp_workflow_type(func),
            queue=self.name,
        )
        return _WorkflowHandlePolling(new_wf_id, self.dbos)
