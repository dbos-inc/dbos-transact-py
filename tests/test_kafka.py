import random
import threading
import time
from typing import Any, NoReturn, Optional

import pytest
from confluent_kafka import Consumer, KafkaError, Producer

from dbos import DBOS, DBOSConfig, KafkaMessage

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


def produce_one_message(server: str, topic: str) -> bool:
    try:

        def on_error(err: KafkaError) -> NoReturn:
            raise Exception(err)

        producer = Producer({"bootstrap.servers": server, "error_cb": on_error})
        producer.produce(topic, key=b"offset-loss-key", value=b"offset-loss-value")
        producer.poll(10)
        producer.flush(10)
        return True
    except Exception:
        return False


def test_kafka_no_offset_loss_on_relaunch(
    config: DBOSConfig, cleanup_test_databases: None, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Regression test for https://github.com/dbos-inc/dbos-transact-py/issues/733
    #
    # A message returned by consumer.poll() must not have its Kafka offset
    # committed until the corresponding DBOS workflow is durably enqueued.
    # Otherwise, a shutdown in the post-poll/pre-enqueue window commits the
    # offset (losing the message) while DBOS has no durable record of it.
    #
    # We amplify that window by pausing inside poll() after a real message is
    # returned, relaunch DBOS while parked there, then assert the message is
    # still delivered (redelivered from Kafka) after the relaunch.
    import dbos._kafka as dbos_kafka_module

    server = "localhost:9092"
    suffix = random.randrange(1_000_000_000)
    topic = f"dbos-kafka-offsetloss-{suffix}"
    group_id = f"dbos-kafka-offsetloss-{suffix}"

    if not produce_one_message(server, topic):
        pytest.skip("Kafka not available")

    # confluent_kafka.Consumer is what dbos._kafka imports; patch the module's
    # reference and delegate to the real class from here.
    original_consumer_cls = Consumer
    first_poll_returned = threading.Event()
    poll_delay_seconds = 3.0

    class PausingConsumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            self._inner = original_consumer_cls(conf)

        def poll(self, timeout: Optional[float] = None) -> Any:
            msg = self._inner.poll(timeout)
            if msg is not None and msg.error() is None:
                # Park in the post-poll/pre-enqueue window so the relaunch
                # (shutdown) lands here, before the workflow is enqueued.
                first_poll_returned.set()
                time.sleep(poll_delay_seconds)
            return msg

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    monkeypatch.setattr(dbos_kafka_module, "Consumer", PausingConsumer)

    processed = threading.Event()

    def register() -> None:
        @DBOS.kafka_consumer(
            {
                "bootstrap.servers": server,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
            },
            [topic],
        )
        @DBOS.workflow()
        def offset_loss_workflow(msg: KafkaMessage) -> None:
            processed.set()

    # Launch #1: consume up to the post-poll window, then shut down before the
    # workflow is durably enqueued.
    DBOS.destroy(destroy_registry=True)
    register()
    DBOS(config=config)
    DBOS.launch()
    try:
        assert first_poll_returned.wait(
            timeout=30
        ), "Kafka consumer never returned the produced message"
        # Relaunch while the consumer is parked after poll() but before enqueue.
        DBOS.destroy(destroy_registry=True)
        assert (
            not processed.is_set()
        ), "Workflow was enqueued before relaunch; the shutdown window was missed"

        # Launch #2: a correct integration redelivers offset 0 because its
        # workflow was never durably enqueued. The buggy integration committed
        # the offset during the launch #1 shutdown, so the message is lost here.
        register()
        DBOS(config=config)
        DBOS.launch()
        assert processed.wait(timeout=45), (
            "Kafka message was lost after relaunch: the offset was committed "
            "before the durable workflow was created"
        )
    finally:
        DBOS.destroy(destroy_registry=True)


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
