import random
import threading
import uuid
from typing import Any, List

import pytest
from confluent_kafka import KafkaError, Producer

from dbos import DBOS, KafkaMessage


def send_test_messages(server: str, topic: str):

    try:

        def on_error(err: KafkaError):
            if err:
                raise Exception(err)

        producer = Producer({"bootstrap.servers": server, "error_cb": on_error})

        producer.produce(topic, key=f"test message key", value=f"test message value")

        producer.poll(10)
        producer.flush(10)
        return True
    except Exception as e:
        return False
    finally:
        pass


def test_kafka(dbos: DBOS):
    event = threading.Event()
    server = "localhost:9092"
    topic = f"dbos-kafka-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    messages: List[KafkaMessage] = []

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test",
            "auto.offset.reset": "earliest",
        },
        [topic],
    )
    @DBOS.workflow()
    def test_kafka_workflow(msg: KafkaMessage):
        print(msg)
        messages.append(msg)
        event.set()

    wait = event.wait(timeout=10)
    assert wait
    assert len(messages) > 0


# from dbos import DBOS, SetWorkflowID


# @DBOS.kafka_consumer(
#     {
#         "bootstrap.servers": "localhost:9092",
#         "group.id": "dbos-test",
#     },
#     ["dbos-test-topic"],
# )
# @DBOS.workflow()
# def foo(msg: Message):
#     DBOS.logger.info(f"Received message: {msg.key()} {msg.value()}")
