import threading
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generator, NoReturn, Optional, Union

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka import Message as CTypeMessage

if TYPE_CHECKING:
    from dbos.dbos import _DBOSRegistry

from .context import SetWorkflowID
from .kafka_message import KafkaMessage
from .logger import dbos_logger

KafkaConsumerWorkflow = Callable[[KafkaMessage], None]


def _kafka_consumer_loop(
    func: KafkaConsumerWorkflow,
    config: dict[str, Any],
    topics: list[str],
    stop_event: threading.Event,
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
                    try:
                        func(msg)
                    except Exception as e:
                        dbos_logger.error(
                            f"Exception encountered in Kafka consumer: {traceback.format_exc()}"
                        )

    finally:
        consumer.close()


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
