import re
import threading
from typing import TYPE_CHECKING, Any, Callable, NoReturn

from confluent_kafka import Consumer, KafkaError, KafkaException

from ._queue import Queue

if TYPE_CHECKING:
    from ._dbos import DBOSRegistry

from ._context import SetWorkflowID
from ._error import DBOSInitializationError
from ._kafka_message import KafkaMessage
from ._logger import dbos_logger
from ._registrations import get_dbos_func_name

_KafkaConsumerWorkflow = Callable[[KafkaMessage], None]

_kafka_queue: Queue
_in_order_kafka_queues: dict[str, Queue] = {}


def safe_group_name(method_name: str, topics: list[str]) -> str:
    safe_group_id = "-".join(
        re.sub(r"[^a-zA-Z0-9\-]", "", str(r)) for r in [method_name, *topics]
    )

    return f"dbos-kafka-group-{safe_group_id}"[:255]


def _kafka_consumer_loop(
    func: _KafkaConsumerWorkflow,
    config: dict[str, Any],
    topics: list[str],
    stop_event: threading.Event,
    in_order: bool,
) -> None:

    def on_error(err: KafkaError) -> NoReturn:
        raise KafkaException(err)

    config["error_cb"] = on_error
    if "auto.offset.reset" not in config:
        config["auto.offset.reset"] = "earliest"

    if config.get("group.id") is None:
        config["group.id"] = safe_group_name(get_dbos_func_name(func), topics)
        dbos_logger.warning(
            f"Consumer group ID not found. Using generated group.id {config['group.id']}"
        )

    consumer = Consumer(config)
    try:
        consumer.subscribe(topics)
        while not stop_event.is_set():
            cmsg = consumer.poll(1.0)

            if stop_event.is_set():
                return

            if cmsg is None:
                continue

            err = cmsg.error()
            if err is not None:
                dbos_logger.error(
                    f"Kafka error {err.code()} ({err.name()}): {err.str()}"
                )
                # fatal errors require an updated consumer instance
                if err.code() == KafkaError._FATAL or err.fatal():
                    original_consumer = consumer
                    try:
                        consumer = Consumer(config)
                        consumer.subscribe(topics)
                    finally:
                        original_consumer.close()
            else:
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
                groupID = config.get("group.id")
                with SetWorkflowID(
                    f"kafka-unique-id-{msg.topic}-{msg.partition}-{groupID}-{msg.offset}"
                ):
                    if in_order:
                        assert msg.topic is not None
                        queue = _in_order_kafka_queues[msg.topic]
                        queue.enqueue(func, msg)
                    else:
                        _kafka_queue.enqueue(func, msg)

    finally:
        consumer.close()


def kafka_consumer(
    dbosreg: "DBOSRegistry", config: dict[str, Any], topics: list[str], in_order: bool
) -> Callable[[_KafkaConsumerWorkflow], _KafkaConsumerWorkflow]:
    def decorator(func: _KafkaConsumerWorkflow) -> _KafkaConsumerWorkflow:
        if in_order:
            for topic in topics:
                if topic.startswith("^"):
                    raise DBOSInitializationError(
                        f"Error: in-order processing is not supported for regular expression topic selectors ({topic})"
                    )
                queue = Queue(f"_dbos_kafka_queue_topic_{topic}", concurrency=1)
                _in_order_kafka_queues[topic] = queue
        else:
            global _kafka_queue
            _kafka_queue = dbosreg.get_internal_queue()
        stop_event = threading.Event()
        dbosreg.register_poller(
            stop_event, _kafka_consumer_loop, func, config, topics, stop_event, in_order
        )
        return func

    return decorator
