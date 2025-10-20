import random
import threading
import time
from typing import NoReturn

import pytest
from confluent_kafka import KafkaError, Producer

from dbos import DBOS, KafkaMessage

# These tests require local Kafka to run.
# Without it, they're automatically skipped.
# Here's a docker-compose script you can use to set up local Kafka:

# services:
#   broker:
#       image: apache/kafka:latest
#       hostname: broker
#       container_name: broker
#       ports:
#         - '9092:9092'
#       environment:
#           KAFKA_NODE_ID: 1
#           KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
#           KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://127.0.0.1:9092
#           KAFKA_PROCESS_ROLES: broker,controller
#           KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
#           KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
#           KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,CONTROLLER:PLAINTEXT
#           KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
#           KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
#           KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
#           CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk

NUM_EVENTS = 3


def send_test_messages(server: str, topic: str) -> bool:

    try:

        def on_error(err: KafkaError) -> NoReturn:
            raise Exception(err)

        producer = Producer({"bootstrap.servers": server, "error_cb": on_error})

        for i in range(NUM_EVENTS):
            producer.produce(
                topic, key=f"test message key {i}", value=f"test message value {i}"
            )

        producer.poll(10)
        producer.flush(10)
        return True
    except Exception as e:
        return False
    finally:
        pass


def test_kafka(dbos: DBOS) -> None:
    event = threading.Event()
    kafka_count = 0
    server = "localhost:9092"
    topic = f"dbos-kafka-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

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
        nonlocal kafka_count
        kafka_count += 1
        assert b"test message key" in msg.key  # type: ignore
        assert b"test message value" in msg.value  # type: ignore
        print(msg)
        if kafka_count == NUM_EVENTS:
            event.set()

    wait = event.wait(timeout=10)
    assert wait
    assert kafka_count == NUM_EVENTS


def test_kafka_async(dbos: DBOS) -> None:
    event = threading.Event()
    kafka_count = 0
    server = "localhost:9092"
    topic = f"dbos-kafka-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test",
            "auto.offset.reset": "earliest",
        },
        [topic],
    )
    @DBOS.workflow()
    async def test_kafka_workflow(msg: KafkaMessage) -> None:
        nonlocal kafka_count
        kafka_count += 1
        assert b"test message key" in msg.key  # type: ignore
        assert b"test message value" in msg.value  # type: ignore
        print(msg)
        if kafka_count == NUM_EVENTS:
            event.set()

    wait = event.wait(timeout=10)
    assert wait
    assert kafka_count == NUM_EVENTS


def test_kafka_in_order(dbos: DBOS) -> None:
    event = threading.Event()
    kafka_count = 0
    server = "localhost:9092"
    topic = f"dbos-kafka-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test",
            "auto.offset.reset": "earliest",
        },
        [topic],
        in_order=True,
    )
    @DBOS.workflow()
    def test_kafka_workflow(msg: KafkaMessage) -> None:
        time.sleep(random.uniform(0, 2))
        nonlocal kafka_count
        kafka_count += 1
        assert f"test message key {kafka_count - 1}".encode() == msg.key
        print(msg)
        if kafka_count == NUM_EVENTS:
            event.set()

    wait = event.wait(timeout=15)
    assert wait
    assert kafka_count == NUM_EVENTS
    time.sleep(2)  # Wait for things to clean up


def test_kafka_no_groupid(dbos: DBOS) -> None:
    event = threading.Event()
    kafka_count = 0
    server = "localhost:9092"
    topic1 = f"dbos-kafka-{random.randrange(1_000_000_000, 2_000_000_000)}"
    topic2 = f"dbos-kafka-{random.randrange(2_000_000_000, 3_000_000_000)}"

    if not send_test_messages(server, topic1):
        pytest.skip("Kafka not available")

    if not send_test_messages(server, topic2):
        pytest.skip("Kafka not available")

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "auto.offset.reset": "earliest",
        },
        [topic1, topic2],
    )
    @DBOS.workflow()
    def test_kafka_workflow(msg: KafkaMessage) -> None:
        nonlocal kafka_count
        kafka_count += 1
        assert b"test message key" in msg.key  # type: ignore
        assert b"test message value" in msg.value  # type: ignore
        print(msg)
        if kafka_count == NUM_EVENTS * 2:
            event.set()

    wait = event.wait(timeout=10)
    assert wait
    assert kafka_count == NUM_EVENTS * 2
