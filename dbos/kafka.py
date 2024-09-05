import threading
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generator

if TYPE_CHECKING:
    from dbos.dbos import _DBOSRegistry
    from confluent_kafka import Message as CTypeMessage, Consumer

from .context import SetWorkflowID
from .logger import dbos_logger


@dataclass
class KafkaMessage:
    headers: list[tuple[str, str | bytes]] | None
    key: str | bytes | None
    latency: float | None
    leader_epoch: int | None
    offset: int | None
    partition: int | None
    timestamp: tuple[int, int]
    topic: str | None
    value: str | bytes | None


def _from_kafka_message(kafka_message: "CTypeMessage") -> KafkaMessage:
    return KafkaMessage(
        headers=kafka_message.headers(),
        key=kafka_message.key(),
        latency=kafka_message.latency(),
        leader_epoch=kafka_message.leader_epoch(),
        offset=kafka_message.offset(),
        partition=kafka_message.partition(),
        timestamp=kafka_message.timestamp(),
        topic=kafka_message.topic(),
        value=kafka_message.value(),
    )


KafkaConsumerWorkflow = Callable[[KafkaMessage], None]

from contextlib import contextmanager


@contextmanager
def _make_kafka_consumer(
    config: dict[str, Any],
    topics: list[str],
) -> Generator["Consumer", Any, None]:
    from confluent_kafka import Consumer

    consumer = Consumer(config)
    try:
        consumer.subscribe(topics)
        yield consumer
    finally:
        consumer.close()


def _kafka_consumer_loop(
    func: KafkaConsumerWorkflow,
    config: dict[str, Any],
    topics: list[str],
    stop_event: threading.Event,
) -> None:

    with _make_kafka_consumer(config, topics) as consumer:
        while not stop_event.is_set():
            if stop_event.wait(timeout=1):
                return

            cmsg = consumer.poll(0)
            if cmsg is None:
                continue
            elif cmsg.error():
                dbos_logger.error(f"Kafka consumer error: {cmsg.error()}")
                continue
            else:
                msg = _from_kafka_message(cmsg)
                with SetWorkflowID(
                    f"kafka-unique-id-{msg.topic}-{msg.partition}-{msg.offset}"
                ):
                    try:
                        func(msg)
                    except Exception as e:
                        dbos_logger.error(
                            f"Exception encountered in scheduled workflow: {traceback.format_exc()}"
                        )


def kafka_consumer(
    dbosreg: "_DBOSRegistry", config: dict[str, Any], topics: list[str]
) -> Callable[[KafkaConsumerWorkflow], KafkaConsumerWorkflow]:
    def decorator(func: KafkaConsumerWorkflow) -> KafkaConsumerWorkflow:
        stop_event = threading.Event()
        dbosreg.register_poller(
            stop_event, _kafka_consumer_loop, func, config, topics, stop_event
        )
        return func

    return decorator
