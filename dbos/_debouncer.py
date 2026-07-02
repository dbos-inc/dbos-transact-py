import asyncio
import time
import types
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Coroutine,
    Dict,
    Generic,
    Literal,
    Optional,
    ParamSpec,
    Tuple,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

from dbos._client import (
    DBOSClient,
    EnqueueOptions,
    WorkflowHandleClientAsyncPolling,
    WorkflowHandleClientPolling,
)
from dbos._context import (
    SetWorkflowDebounce,
    get_local_dbos_context,
    snapshot_step_context,
)
from dbos._core import WorkflowHandleAsyncPolling, WorkflowHandlePolling
from dbos._error import DBOSException, DBOSQueueDeduplicatedError
from dbos._queue import Queue
from dbos._registrations import get_dbos_func_name, get_func_info
from dbos._serialization import PortableWorkflowError, serialize_args
from dbos._sys_db import DebounceResult

if TYPE_CHECKING:
    from dbos._dbos import WorkflowHandle, WorkflowHandleAsync

P = ParamSpec("P")  # A generic type for workflow parameters
R = TypeVar("R", covariant=True)  # A generic type for workflow return values


def _resolve_queue_name(queue: Optional[Union[Queue, str]]) -> Optional[str]:
    if queue is None:
        return None
    if isinstance(queue, Queue):
        return queue.name
    return queue


def _is_queue_deduplicated_error(e: BaseException) -> bool:
    """True if ``e`` is a queue-deduplication error, including its replay form.

    When an in-workflow debounce's fresh enqueue loses the dedup race, the
    DBOSQueueDeduplicatedError is checkpointed at the parent's function ID. On
    replay it is re-raised from that checkpoint, but a workflow using portable
    (cross-language JSON) serialization deserializes it to a PortableWorkflowError
    (which carries the original type name), not the original type. Match both so
    the retry loop still recognizes the collision on replay instead of erroring.
    """
    if isinstance(e, DBOSQueueDeduplicatedError):
        return True
    if isinstance(e, PortableWorkflowError):
        return e.name == DBOSQueueDeduplicatedError.__name__
    return False


def _reject_conflicting_options(*, has_deduplication_id: bool, has_delay: bool) -> None:
    """A debounce owns the workflow's deduplication ID (the debounce key) and its
    delay (the debounce period), so a caller must not also set them."""
    if has_deduplication_id:
        raise DBOSException(
            "Cannot debounce a workflow with a deduplication_id set: the debounce "
            "key is used as the workflow's deduplication ID."
        )
    if has_delay:
        raise DBOSException(
            "Cannot debounce a workflow with a delay set: the debounce period "
            "controls the workflow's delay."
        )


# The action a debounce caller should take after a bounce attempt.
_BounceAction = Literal["return", "enqueue", "raise", "retry"]


def _classify_bounce(result: DebounceResult) -> _BounceAction:
    """Decide what a debounce caller should do after a bounce attempt.

    - "return": an existing debounced workflow was extended; return a handle to
      ``result["bounced_workflow_id"]``.
    - "enqueue": the key is unheld; enqueue a fresh debounced workflow.
    - "raise": the key is held by a non-debounced workflow (a caller's own
      deduplicated workflow); surface the deduplication conflict.
    - "retry": a debounced holder flipped out of DELAYED mid-bounce (a rare
      race); retry the bounce.
    """
    if result["bounced_workflow_id"] is not None:
        return "return"
    if result["holder_workflow_id"] is None:
        return "enqueue"
    if not result["holder_is_debounced"]:
        return "raise"
    return "retry"


# Parameters for the debouncer
class DebouncerOptions(TypedDict):
    workflow_name: str
    debounce_timeout_sec: Optional[float]
    queue_name: Optional[str]


class Debouncer(Generic[P, R]):

    def __init__(
        self,
        workflow_name: str,
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ):
        self.func_name = workflow_name
        self.options: DebouncerOptions = {
            "debounce_timeout_sec": debounce_timeout_sec,
            "queue_name": _resolve_queue_name(queue),
            "workflow_name": workflow_name,
        }

    @staticmethod
    def create(
        workflow: Callable[P, R],
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ) -> "Debouncer[P, R]":

        if isinstance(workflow, (types.MethodType)):
            raise TypeError("Only workflow functions may be debounced, not methods")
        return Debouncer[P, R](
            get_dbos_func_name(workflow),
            debounce_timeout_sec=debounce_timeout_sec,
            queue=queue,
        )

    @staticmethod
    def create_async(
        workflow: Callable[P, Coroutine[Any, Any, R]],
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ) -> "Debouncer[P, R]":

        if isinstance(workflow, (types.MethodType)):
            raise TypeError("Only workflow functions may be debounced, not methods")
        return Debouncer[P, R](
            get_dbos_func_name(workflow),
            debounce_timeout_sec=debounce_timeout_sec,
            queue=queue,
        )

    def _bounce(
        self,
        dbos: Any,
        func: Callable[..., Any],
        queue_name: str,
        deduplication_id: str,
        debounce_period_sec: float,
        args: Tuple[Any, ...],
        kwargs: Dict[str, Any],
    ) -> DebounceResult:
        # Serialize new inputs with the workflow's format so a bounce stays consistent with the initial enqueue.
        fi = get_func_info(func)
        serialization_type = fi.serialization_type if fi is not None else None
        inputs, serialization = serialize_args(
            args, kwargs, serialization_type, dbos._serializer
        )
        delay_until_epoch_ms = int(time.time() * 1000 + debounce_period_sec * 1000)
        result: DebounceResult = dbos._sys_db.debounce_delayed_workflow(
            queue_name=queue_name,
            deduplication_id=deduplication_id,
            delay_until_epoch_ms=delay_until_epoch_ms,
            inputs=inputs,
            serialization=serialization,
        )
        return result

    def debounce(
        self,
        debounce_key: str,
        debounce_period_sec: float,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "WorkflowHandle[R]":
        from dbos._dbos import DBOS, _get_dbos_instance

        dbos = _get_dbos_instance()

        # The caller must not set a deduplication_id or delay: the debounce owns both.
        ctx = get_local_dbos_context()
        if ctx is not None:
            _reject_conflicting_options(
                has_deduplication_id=ctx.deduplication_id is not None,
                has_delay=ctx.delay_until_epoch_ms is not None,
            )

        # Resolve the queue the debounced workflow will run on.
        queue_name = self.options["queue_name"]
        if queue_name:
            queue: Optional[Queue] = dbos._registry.queue_info_map.get(queue_name)
            if queue is None:
                queue = DBOS.retrieve_queue(queue_name)
            if queue is None:
                raise Exception(
                    f"Invalid queue name provided to debouncer: {queue_name}"
                )
        else:
            queue = dbos._registry.get_internal_queue()
        assert queue is not None

        # Resolve the workflow function so bounced inputs serialize identically.
        func = dbos._registry.workflow_info_map.get(self.options["workflow_name"])
        if func is None:
            raise Exception(
                f"Invalid workflow name provided to debouncer: {self.options['workflow_name']}"
            )

        deduplication_id = f"{self.options['workflow_name']}-{debounce_key}"
        timeout_sec = self.options["debounce_timeout_sec"]

        while True:
            # Try to extend an existing debounced workflow for this key first (hot path, sole coalescing mechanism); a checkpointed step so an in-workflow debounce replays deterministically.
            result = dbos._sys_db.call_function_as_step(
                lambda: self._bounce(
                    dbos,
                    func,
                    queue.name,
                    deduplication_id,
                    debounce_period_sec,
                    args,
                    kwargs,
                ),
                "DBOS.debounce_delayed_workflow",
                snapshot_step_context(reserve_sleep_id=False),
            )
            action = _classify_bounce(result)
            if action == "return":
                bounced_wfid = result["bounced_workflow_id"]
                assert bounced_wfid is not None
                return WorkflowHandlePolling(bounced_wfid, dbos)
            if action == "raise":
                assert result["holder_workflow_id"] is not None
                raise DBOSQueueDeduplicatedError(
                    result["holder_workflow_id"], queue.name, deduplication_id
                )
            if action == "retry":
                continue

            # action == "enqueue": key is free, create a fresh debounced workflow (these times are observed only on fresh creation; replay short-circuits through the checkpoint).
            now_ms = int(time.time() * 1000)
            deadline_ms = int(now_ms + timeout_sec * 1000) if timeout_sec else None
            delay_until_ms = now_ms + int(debounce_period_sec * 1000)
            if deadline_ms is not None:
                delay_until_ms = min(delay_until_ms, deadline_ms)
            try:
                with SetWorkflowDebounce(
                    deduplication_id=deduplication_id,
                    delay_until_epoch_ms=delay_until_ms,
                    debounce_deadline_epoch_ms=deadline_ms,
                ):
                    return queue.enqueue(func, *args, **kwargs)
            except (DBOSQueueDeduplicatedError, PortableWorkflowError) as e:
                # A concurrent debounce grabbed the key between bounce and enqueue; loop to bounce that workflow instead.
                # (Also catches the portable-serialization replay form of the dedup error; see _is_queue_deduplicated_error.)
                if not _is_queue_deduplicated_error(e):
                    raise
                continue

    async def debounce_async(
        self,
        debounce_key: str,
        debounce_period_sec: float,
        *args: P.args,
        **kwargs: P.kwargs,
    ) -> "WorkflowHandleAsync[R]":
        from dbos._dbos import _get_dbos_instance

        dbos = _get_dbos_instance()
        handle = await asyncio.to_thread(
            self.debounce, debounce_key, debounce_period_sec, *args, **kwargs
        )
        return WorkflowHandleAsyncPolling(handle.workflow_id, dbos)


class DebouncerClient:

    def __init__(
        self,
        client: DBOSClient,
        workflow_options: EnqueueOptions,
        *,
        debounce_timeout_sec: Optional[float] = None,
        queue: Optional[Union[Queue, str]] = None,
    ):
        self.workflow_options = workflow_options
        self.debouncer_options: DebouncerOptions = {
            "debounce_timeout_sec": debounce_timeout_sec,
            "queue_name": _resolve_queue_name(queue),
            "workflow_name": workflow_options["workflow_name"],
        }
        self.client = client

    def debounce(
        self, debounce_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any
    ) -> "WorkflowHandle[R]":
        # The workflow options must not set a deduplication_id or delay: the debounce owns both.
        _reject_conflicting_options(
            has_deduplication_id=self.workflow_options.get("deduplication_id")
            is not None,
            has_delay=self.workflow_options.get("delay_seconds") is not None,
        )

        # Run on the debouncer's queue if one was given, else the queue named in the workflow options.
        queue_name = (
            self.debouncer_options["queue_name"] or self.workflow_options["queue_name"]
        )
        deduplication_id = f"{self.debouncer_options['workflow_name']}-{debounce_key}"
        timeout_sec = self.debouncer_options["debounce_timeout_sec"]
        serialization_type = self.workflow_options.get("serialization_type")

        while True:
            # Try to extend an existing debounced workflow for this key first, so the dedup key is the sole coalescing mechanism.
            inputs, serialization = serialize_args(
                args, kwargs, serialization_type, self.client._serializer
            )
            bounce_delay_ms = int(time.time() * 1000 + debounce_period_sec * 1000)
            result = self.client._sys_db.debounce_delayed_workflow(
                queue_name=queue_name,
                deduplication_id=deduplication_id,
                delay_until_epoch_ms=bounce_delay_ms,
                inputs=inputs,
                serialization=serialization,
            )
            action = _classify_bounce(result)
            if action == "return":
                bounced_wfid = result["bounced_workflow_id"]
                assert bounced_wfid is not None
                return WorkflowHandleClientPolling[R](bounced_wfid, self.client._sys_db)
            if action == "raise":
                assert result["holder_workflow_id"] is not None
                raise DBOSQueueDeduplicatedError(
                    result["holder_workflow_id"], queue_name, deduplication_id
                )
            if action == "retry":
                continue

            # action == "enqueue": the key is free, so create a fresh debounced workflow.
            now_ms = int(time.time() * 1000)
            deadline_ms = int(now_ms + timeout_sec * 1000) if timeout_sec else None
            delay_sec = debounce_period_sec
            if deadline_ms is not None:
                delay_sec = min(delay_sec, max((deadline_ms - now_ms) / 1000.0, 0.0))
            options = cast(
                EnqueueOptions,
                {
                    **self.workflow_options,
                    "queue_name": queue_name,
                    "deduplication_id": deduplication_id,
                    "delay_seconds": delay_sec,
                    "debounce_deadline_epoch_ms": deadline_ms,
                    "is_debounced": True,
                },
            )
            try:
                return self.client.enqueue(options, *args, **kwargs)
            except DBOSQueueDeduplicatedError:
                # A concurrent debounce grabbed the key between bounce and enqueue; loop to bounce that workflow instead.
                continue

    async def debounce_async(
        self, deboucne_key: str, debounce_period_sec: float, *args: Any, **kwargs: Any
    ) -> "WorkflowHandleAsync[R]":
        handle: "WorkflowHandle[R]" = await asyncio.to_thread(
            self.debounce, deboucne_key, debounce_period_sec, *args, **kwargs
        )
        return WorkflowHandleClientAsyncPolling[R](
            handle.workflow_id, self.client._sys_db
        )
