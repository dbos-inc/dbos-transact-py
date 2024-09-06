import threading
import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Generator, Optional, Union

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka import Message as CTypeMessage

if TYPE_CHECKING:
    from dbos.dbos import _DBOSRegistry

from .context import SetWorkflowID
from .kafka_message import KafkaMessage
from .logger import dbos_logger


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


def _kafka_consumer_loop(
    func: KafkaConsumerWorkflow,
    config: dict[str, Any],
    topics: list[str],
    stop_event: threading.Event,
) -> None:

    def on_error(err: KafkaError):
        if err:
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
            elif cmsg.error():
                if cmsg.error().code() == KafkaError._PARTITION_EOF:
                    dbos_logger.warning(
                        f"{cmsg.topic()} [{cmsg.partition()}] readed end at offset {cmsg.offset()}"
                    )
                    continue
                else:
                    raise KafkaException(cmsg.error())
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
