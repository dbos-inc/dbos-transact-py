import random
import threading
import uuid
from typing import Any, List, NoReturn

import pytest
from confluent_kafka import KafkaError, Producer

from dbos import DBOS, KafkaMessage

# These tests require local Kafka to run.
# Without it, they're automatically skipped.
# Here's a docker-compose script you can use to set up local Kafka:

# version: "3.7"
# services:
#   broker:
#       image: bitnami/kafka:latest
#       hostname: broker
#       container_name: broker
#       ports:
#         - '9092:9092'
#       environment:
#         KAFKA_CFG_NODE_ID: 1
#         KAFKA_CFG_LISTENER_SECURITY_PROTOCOL_MAP: 'CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT'
#         KAFKA_CFG_ADVERTISED_LISTENERS: 'PLAINTEXT_HOST://localhost:9092,PLAINTEXT://broker:19092'
#         KAFKA_CFG_PROCESS_ROLES: 'broker,controller'
#         KAFKA_CFG_CONTROLLER_QUORUM_VOTERS: '1@broker:29093'
#         KAFKA_CFG_LISTENERS: 'CONTROLLER://:29093,PLAINTEXT_HOST://:9092,PLAINTEXT://:19092'
#         KAFKA_CFG_INTER_BROKER_LISTENER_NAME: 'PLAINTEXT'
#         KAFKA_CFG_CONTROLLER_LISTENER_NAMES: 'CONTROLLER'


def send_test_messages(server: str, topic: str) -> bool:

    try:

        def on_error(err: KafkaError) -> NoReturn:
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


def test_kafka(dbos: DBOS) -> None:
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
    def test_kafka_workflow(msg: KafkaMessage) -> None:
        print(msg)
        messages.append(msg)
        event.set()

    wait = event.wait(timeout=10)
    assert wait
    assert len(messages) > 0
