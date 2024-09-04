import threading
import traceback
from typing import TYPE_CHECKING, Any, Callable

from confluent_kafka import Consumer, Message

if TYPE_CHECKING:
    from dbos.dbos import _DBOSRegistry

from .context import SetWorkflowID
from .logger import dbos_logger

KafkaConsumerWorkflow = Callable[[Message], None]


def kafka_consumer_loop(
    func: KafkaConsumerWorkflow,
    config: dict[str, Any],
    topics: list[str],
    stop_event: threading.Event,
):
    consumer = Consumer(config)
    consumer.subscribe(topics)
    while not stop_event.is_set():
        if stop_event.wait(timeout=1):
            return

        msg = consumer.poll(0)
        if msg is None:
            continue
        elif msg.error():
            dbos_logger.error(f"Kafka consumer error: {msg.error()}")
            continue
        else:
            with SetWorkflowID(
                f"kafka-unique-id-${msg.topic()}-${msg.partition()}-${msg.offset()}"
            ):
                try:
                    func(msg)
                except Exception as e:
                    dbos_logger.error(
                        f"Exception encountered in Kafka consumer workflow: {traceback.format_exc()}"
                    )


def kafka_consumer(
    dbosreg: "_DBOSRegistry", config: dict[str, Any], topics: list[str]
) -> Callable[[KafkaConsumerWorkflow], KafkaConsumerWorkflow]:
    def decorator(func: KafkaConsumerWorkflow) -> KafkaConsumerWorkflow:
        stop_event = threading.Event()
        dbosreg.register_poller(
            stop_event, kafka_consumer_loop, func, config, topics, stop_event
        )
        return func

    return decorator
