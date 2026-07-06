import re
import threading
import warnings
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Literal, Optional, TypeVar

from confluent_kafka import Consumer, KafkaError, KafkaException

from ._queue import Queue

if TYPE_CHECKING:
    from ._dbos import DBOS, DBOSRegistry
    from ._sys_db import WorkflowStatusInternal

from ._core import prepare_enqueued_workflow
from ._error import DBOSInitializationError
from ._kafka_message import KafkaMessage
from ._logger import dbos_logger
from ._registrations import get_dbos_func_name

_KafkaConsumerWorkflow = (
    Callable[[KafkaMessage], None] | Callable[[KafkaMessage], Coroutine[Any, Any, None]]
)

KafkaOrdering = Literal["none", "partition", "topic"]

# Queue for ordering="none" consumers without a custom queue
KAFKA_QUEUE_NAME = "_dbos_kafka_queue"
# Shared partitioned queue for ordered consumers
KAFKA_ORDERED_QUEUE_NAME = "_dbos_kafka_ordered_queue"
# Pre-redesign per-topic in-order queues, declared drain-only for compatibility
_LEGACY_TOPIC_QUEUE_PREFIX = "_dbos_kafka_queue_topic_"

_MIN_RETRY_WAIT_SEC = 1.0
_MAX_RETRY_WAIT_SEC = 60.0

T = TypeVar("T")


@dataclass
class KafkaConsumerRegistration:
    func_name: str
    group_id: str
    topics: list[str]
    ordering: KafkaOrdering


def safe_group_name(method_name: str, topics: list[str]) -> str:
    safe_group_id = "-".join(
        re.sub(r"[^a-zA-Z0-9\-]", "", str(r)) for r in [method_name, *topics]
    )

    return f"dbos-kafka-group-{safe_group_id}"[:255]


def _get_or_create_queue(dbosreg: "DBOSRegistry", name: str, **kwargs: Any) -> Queue:
    queue = dbosreg.queue_info_map.get(name)
    if queue is None:
        queue = Queue(name, **kwargs)
    return queue


def _retry_until_success(
    stop_event: threading.Event, operation: Callable[[], T], description: str
) -> Optional[T]:
    """Run operation until it succeeds, backing off between attempts.

    Returns None (abandoning the operation) only once stop_event is set.
    """
    wait_sec = _MIN_RETRY_WAIT_SEC
    while not stop_event.is_set():
        try:
            return operation()
        except Exception as e:
            dbos_logger.error(
                f"Kafka consumer failed to {description}: {e}. Retrying in {wait_sec:.0f}s."
            )
            if stop_event.wait(timeout=wait_sec):
                return None
            wait_sec = min(wait_sec * 2.0, _MAX_RETRY_WAIT_SEC)
    return None


def _build_status(
    dbos: "DBOS",
    func: _KafkaConsumerWorkflow,
    cmsg: Any,
    group_id: str,
    ordering: KafkaOrdering,
    queue_name: str,
) -> "WorkflowStatusInternal":
    msg = KafkaMessage(
        headers=cmsg.headers(),
        key=cmsg.key(),
        latency=cmsg.latency(),
        leader_epoch=cmsg.leader_epoch(),
        offset=cmsg.offset(),
        partition=cmsg.partition(),
        timestamp=cmsg.timestamp(),
        topic=cmsg.topic(),
        value=cmsg.value(),
    )
    # This ID format is the dedup key for redelivered messages; never change it.
    workflow_id = f"kafka-unique-id-{msg.topic}-{msg.partition}-{group_id}-{msg.offset}"
    if ordering == "partition":
        partition_key = f"{group_id}:{msg.topic}:{msg.partition}"
    elif ordering == "topic":
        partition_key = f"{group_id}:{msg.topic}"
    else:
        partition_key = None
    return prepare_enqueued_workflow(
        dbos,
        func,
        (msg,),
        {},
        queue_name=queue_name,
        workflow_id=workflow_id,
        queue_partition_key=partition_key,
    )


def _last_message_per_partition(cmsgs: list[Any]) -> list[Any]:
    # consume() preserves per-partition order, so the last message seen per
    # partition carries its highest offset.
    last: dict[tuple[str, int], Any] = {}
    for cmsg in cmsgs:
        last[(cmsg.topic(), cmsg.partition())] = cmsg
    return list(last.values())


def _kafka_consumer_loop(
    func: _KafkaConsumerWorkflow,
    config: dict[str, Any],
    topics: list[str],
    stop_event: threading.Event,
    ordering: KafkaOrdering,
    batch_size: int,
    queue_name: str,
) -> None:
    from ._dbos import _get_dbos_instance

    dbos = _get_dbos_instance()
    group_id: str = config["group.id"]

    def make_consumer() -> Consumer:
        consumer = Consumer(config)
        consumer.subscribe(topics)
        return consumer

    def replace_consumer(old: Optional[Consumer]) -> Optional[Consumer]:
        if old is not None:
            try:
                old.close()
            except Exception as e:
                dbos_logger.warning(f"Error closing Kafka consumer: {e}")
        return _retry_until_success(
            stop_event, make_consumer, "create the Kafka consumer"
        )

    consumer = replace_consumer(None)
    if consumer is None:
        return
    retry_wait = _MIN_RETRY_WAIT_SEC
    try:
        while not stop_event.is_set():
            try:
                cmsgs = consumer.consume(batch_size, timeout=1.0)

                if stop_event.is_set():
                    # Safe to drop: offsets weren't stored, so Kafka redelivers.
                    return

                fatal_error = False
                valid: list[Any] = []
                for cmsg in cmsgs:
                    err = cmsg.error()
                    if err is None:
                        valid.append(cmsg)
                        continue
                    dbos_logger.error(
                        f"Kafka error {err.code()} ({err.name()}): {err.str()}"
                    )
                    # fatal errors require an updated consumer instance
                    if err.code() == KafkaError._FATAL or err.fatal():
                        fatal_error = True

                if valid:
                    statuses = [
                        _build_status(dbos, func, cmsg, group_id, ordering, queue_name)
                        for cmsg in valid
                    ]
                    # Retry this same batch until durable: consume() advances the
                    # fetch position regardless of offset storage, so dropping the
                    # batch and continuing would lose these messages until the next
                    # rebalance.
                    if (
                        _retry_until_success(
                            stop_event,
                            lambda: dbos._sys_db.init_workflows(statuses),
                            "durably enqueue consumed messages",
                        )
                        is None
                    ):
                        # Stopped before the batch was durable; offsets weren't
                        # stored, so Kafka redelivers.
                        return
                    # Workflows are durable; advance the offsets (auto-commit
                    # flushes them later).
                    for cmsg in _last_message_per_partition(valid):
                        try:
                            consumer.store_offsets(message=cmsg)
                        except KafkaException as e:
                            # Partition revoked, etc.; offsets stay put and the
                            # messages are redelivered.
                            dbos_logger.warning(f"Failed to store Kafka offset: {e}")

                if fatal_error:
                    consumer = replace_consumer(consumer)
                    if consumer is None:
                        return

                retry_wait = _MIN_RETRY_WAIT_SEC
            except Exception as e:
                dbos_logger.error(
                    f"Unexpected error in Kafka consumer loop: {e}. Retrying in {retry_wait:.0f}s."
                )
                if stop_event.wait(timeout=retry_wait):
                    return
                retry_wait = min(retry_wait * 2.0, _MAX_RETRY_WAIT_SEC)
                # Recreate the consumer to rewind its fetch position to the stored
                # offsets: anything consumed but not durably enqueued is redelivered.
                consumer = replace_consumer(consumer)
                if consumer is None:
                    return
    finally:
        if consumer is not None:
            consumer.close()


def kafka_consumer(
    dbosreg: "DBOSRegistry",
    config: dict[str, Any],
    topics: list[str],
    in_order: bool = False,
    *,
    ordering: Optional[KafkaOrdering] = None,
    batch_size: int = 250,
    queue: Optional[Queue] = None,
) -> Callable[[_KafkaConsumerWorkflow], _KafkaConsumerWorkflow]:
    if ordering is not None and in_order:
        raise DBOSInitializationError(
            "Error: specify either in_order or ordering, not both"
        )
    resolved_ordering: KafkaOrdering
    if in_order:
        warnings.warn(
            'in_order=True is deprecated; use ordering="partition" '
            '(or ordering="topic" for the same per-topic serialization)',
            DeprecationWarning,
            stacklevel=3,
        )
        resolved_ordering = "topic"
    elif ordering is None:
        resolved_ordering = "none"
    elif ordering in ("none", "partition", "topic"):
        resolved_ordering = ordering
    else:
        raise DBOSInitializationError(
            f'Error: invalid Kafka ordering "{ordering}": '
            'must be "none", "partition", or "topic"'
        )
    if batch_size < 1:
        raise DBOSInitializationError("Error: Kafka batch_size must be positive")
    if queue is not None and resolved_ordering != "none":
        raise DBOSInitializationError(
            'Error: a custom queue is only supported with ordering="none"; '
            "ordered consumers share an internal partitioned queue"
        )

    def decorator(func: _KafkaConsumerWorkflow) -> _KafkaConsumerWorkflow:
        func_name = get_dbos_func_name(func)
        cfg = dict(config)  # copy: never mutate the caller's dict

        def on_error(err: KafkaError) -> None:
            dbos_logger.error(f"Exception in Kafka consumer: {err}")

        cfg["error_cb"] = on_error
        if "auto.offset.reset" not in cfg:
            cfg["auto.offset.reset"] = "earliest"

        # Store offsets ourselves after durable enqueue, so commits never outrun durable state.
        if cfg.get("enable.auto.offset.store", True) is not False:
            if cfg.get("enable.auto.offset.store") is True:
                dbos_logger.warning(
                    "Overriding enable.auto.offset.store=True: DBOS manages Kafka "
                    "offset storage to avoid committing past durable workflow state."
                )
            cfg["enable.auto.offset.store"] = False

        if cfg.get("group.id") is None:
            cfg["group.id"] = safe_group_name(func_name, topics)
            dbos_logger.warning(
                f"Consumer group ID not found. Using generated group.id {cfg['group.id']}"
            )
        group_id: str = cfg["group.id"]

        for reg in dbosreg.kafka_registrations:
            if reg.group_id != group_id:
                continue
            shared = sorted(set(reg.topics) & set(topics))
            if shared:
                raise DBOSInitializationError(
                    f"Error: Kafka consumers {reg.func_name} and {func_name} share "
                    f"group.id {group_id} and topic(s) {shared}, so each message "
                    "would be delivered to only one of them. Use distinct group IDs."
                )
            dbos_logger.warning(
                f"Kafka consumers {reg.func_name} and {func_name} share group.id "
                f"{group_id} with different topics. This can cause rebalance churn; "
                "consider using distinct group IDs."
            )
        dbosreg.kafka_registrations.append(
            KafkaConsumerRegistration(
                func_name=func_name,
                group_id=group_id,
                topics=list(topics),
                ordering=resolved_ordering,
            )
        )

        if resolved_ordering == "none":
            consumer_queue = (
                queue
                if queue is not None
                else _get_or_create_queue(dbosreg, KAFKA_QUEUE_NAME)
            )
        else:
            # One shared partitioned queue: concurrency=1 is enforced per partition
            # key, so execution is serial per key and parallel across keys.
            consumer_queue = _get_or_create_queue(
                dbosreg,
                KAFKA_ORDERED_QUEUE_NAME,
                partition_queue=True,
                concurrency=1,
            )
            if resolved_ordering == "topic":
                # Drain-only: pre-redesign releases enqueued in-order workflows on
                # per-topic queues. Declare them so any leftover rows still execute.
                for topic in topics:
                    if not topic.startswith("^"):
                        _get_or_create_queue(
                            dbosreg,
                            f"{_LEGACY_TOPIC_QUEUE_PREFIX}{topic}",
                            concurrency=1,
                        )

        stop_event = threading.Event()
        dbosreg.register_poller(
            stop_event,
            _kafka_consumer_loop,
            func,
            cfg,
            topics,
            stop_event,
            resolved_ordering,
            batch_size,
            consumer_queue.name,
        )
        return func

    return decorator
