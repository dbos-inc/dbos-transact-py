import threading
from typing import TYPE_CHECKING, Any, Callable, NoReturn

from confluent_kafka import Consumer, KafkaError, KafkaException

from dbos.queue import Queue

if TYPE_CHECKING:
    from dbos.dbos import _DBOSRegistry

from .context import SetWorkflowID
from .error import DBOSInitializationError
from .kafka_message import KafkaMessage
from .logger import dbos_logger

KafkaConsumerWorkflow = Callable[[KafkaMessage], None]

kafka_queue: Queue
in_order_kafka_queues: dict[str, Queue] = {}


def _kafka_consumer_loop(
    func: KafkaConsumerWorkflow,
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
                with SetWorkflowID(
                    f"kafka-unique-id-{msg.topic}-{msg.partition}-{msg.offset}"
                ):
                    if in_order:
                        assert msg.topic is not None
                        queue = in_order_kafka_queues[msg.topic]
                        queue.enqueue(func, msg)
                    else:
                        kafka_queue.enqueue(func, msg)

    finally:
        consumer.close()


def kafka_consumer(
    dbosreg: "_DBOSRegistry", config: dict[str, Any], topics: list[str], in_order: bool
) -> Callable[[KafkaConsumerWorkflow], KafkaConsumerWorkflow]:
    def decorator(func: KafkaConsumerWorkflow) -> KafkaConsumerWorkflow:
        if in_order:
            for topic in topics:
                if topic.startswith("^"):
                    raise DBOSInitializationError(
                        f"Error: in-order processing is not supported for regular expression topic selectors ({topic})"
                    )
                queue = Queue(f"_dbos_kafka_queue_topic_{topic}", concurrency=1)
                in_order_kafka_queues[topic] = queue
        else:
            global kafka_queue
            kafka_queue = Queue("_dbos_internal_queue")
        stop_event = threading.Event()
        dbosreg.register_poller(
            stop_event, _kafka_consumer_loop, func, config, topics, stop_event, in_order
        )
        return func

    return decorator
