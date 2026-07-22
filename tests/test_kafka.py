import logging
import random
import threading
import time
from typing import Any, NoReturn, Optional

import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Producer

from dbos import DBOS, DBOSConfig, KafkaMessage, Queue
from dbos._kafka import _describe_kafka_error, _make_error_cb
from dbos._logger import dbos_logger

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
    # Regression for #733: relaunch while parked after poll() but before enqueue, then assert no message loss.
    import dbos._kafka as dbos_kafka_module

    server = "localhost:9092"
    suffix = random.randrange(1_000_000_000)
    topic = f"dbos-kafka-offsetloss-{suffix}"
    group_id = f"dbos-kafka-offsetloss-{suffix}"

    if not produce_one_message(server, topic):
        pytest.skip("Kafka not available")

    # Wrap the real Consumer that dbos._kafka uses so we can pause inside consume().
    original_consumer_cls = Consumer
    first_poll_returned = threading.Event()
    poll_delay_seconds = 3.0

    class PausingConsumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            self._inner = original_consumer_cls(conf)

        def consume(self, num_messages: int = 1, timeout: float = -1) -> Any:
            msgs = self._inner.consume(num_messages, timeout)
            if any(m.error() is None for m in msgs):
                # Park in the post-consume/pre-enqueue window so the relaunch lands here.
                first_poll_returned.set()
                time.sleep(poll_delay_seconds)
            return msgs

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

    # Launch #1: consume up to the post-poll window, then shut down before enqueue.
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

        # Launch #2: correct code redelivers offset 0; buggy code already committed it (lost).
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
            "group.id": f"dbos-test-{random.randrange(1_000_000_000)}",
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

    wait = event.wait(timeout=45)
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
            "group.id": f"dbos-test-{random.randrange(1_000_000_000)}",
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

    wait = event.wait(timeout=45)
    assert wait
    assert kafka_count == NUM_EVENTS


def test_kafka_in_order(dbos: DBOS) -> None:
    event = threading.Event()
    kafka_count = 0
    server = "localhost:9092"
    topic = f"dbos-kafka-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    with pytest.warns(DeprecationWarning):

        @DBOS.kafka_consumer(
            {
                "bootstrap.servers": server,
                "group.id": f"dbos-test-{random.randrange(1_000_000_000)}",
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

    wait = event.wait(timeout=45)
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

    wait = event.wait(timeout=45)
    assert wait
    assert kafka_count == NUM_EVENTS * 2


def test_kafka_partition_ordering(dbos: DBOS) -> None:
    # ordering="partition": serial per partition, parallel across partitions.
    from confluent_kafka.admin import AdminClient, NewTopic

    server = "localhost:9092"
    topic = f"dbos-kafka-part-{random.randrange(1_000_000_000)}"
    num_partitions = 4
    num_per_partition = 5

    admin = AdminClient({"bootstrap.servers": server})
    try:
        admin.create_topics([NewTopic(topic, num_partitions=num_partitions)])[
            topic
        ].result()
    except Exception:
        pytest.skip("Kafka not available")

    producer = Producer({"bootstrap.servers": server})
    # Wait for the new topic's partitions to propagate to the producer
    for _ in range(30):
        metadata = producer.list_topics(topic, timeout=5)
        if len(metadata.topics[topic].partitions) == num_partitions:
            break
        time.sleep(0.5)
    for p in range(num_partitions):
        for i in range(num_per_partition):
            producer.produce(topic, partition=p, value=f"{i}")
    producer.flush(10)

    lock = threading.Lock()
    seen: dict[int, list[int]] = {p: [] for p in range(num_partitions)}
    active = 0
    max_active = 0
    done = threading.Event()

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-partition-ordering",
            "auto.offset.reset": "earliest",
        },
        [topic],
        ordering="partition",
    )
    @DBOS.workflow()
    def partition_workflow(msg: KafkaMessage) -> None:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.5)  # widen the window to observe cross-partition parallelism
        with lock:
            active -= 1
            assert msg.partition is not None
            seen[msg.partition].append(int(msg.value))  # type: ignore
            if sum(len(v) for v in seen.values()) == num_partitions * num_per_partition:
                done.set()

    assert done.wait(timeout=90)
    # Kafka's guarantee: per-partition order is preserved exactly
    for p in range(num_partitions):
        assert seen[p] == list(range(num_per_partition))
    # Partitions were processed in parallel
    assert max_active >= 2


def test_kafka_db_outage(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    # The consumer must survive transient enqueue failures with no message loss and no duplicate execution: a failed init_workflows retries the same batch, and the idempotent insert runs each message exactly once.
    event = threading.Event()
    lock = threading.Lock()
    processed: list[int] = []
    server = "localhost:9092"
    topic = f"dbos-kafka-outage-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    fail_remaining = 3
    failures_injected = 0
    inserted_total = 0
    original_init_workflows = dbos._sys_db.init_workflows

    def flaky_init_workflows(statuses: Any) -> Any:
        nonlocal fail_remaining, failures_injected, inserted_total
        if fail_remaining > 0:
            fail_remaining -= 1
            failures_injected += 1
            raise Exception("Simulated database outage")
        result = original_init_workflows(statuses)
        inserted_total += len(result)
        return result

    monkeypatch.setattr(dbos._sys_db, "init_workflows", flaky_init_workflows)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-db-outage",
            "auto.offset.reset": "earliest",
        },
        [topic],
    )
    @DBOS.workflow()
    def outage_workflow(msg: KafkaMessage) -> None:
        assert msg.value is not None
        with lock:
            processed.append(int(msg.value.decode().split()[-1]))  # type: ignore
            if len(processed) == NUM_EVENTS:
                event.set()

    # Backoff between the injected failures is 1s + 2s + 4s.
    assert event.wait(timeout=60)
    # The retry path was actually exercised: the first three enqueue attempts failed.
    assert failures_injected == 3
    # No message loss and no duplicate execution despite the retried batch: every message ran exactly once.
    assert sorted(processed) == list(range(NUM_EVENTS))
    # Idempotent insert: exactly NUM_EVENTS rows were durably created; the retried batch didn't double-insert or lose any.
    assert inserted_total == NUM_EVENTS


def test_kafka_listen_queues_still_polls_consumer(
    dbos: DBOS, config: DBOSConfig
) -> None:
    # Regression: a Kafka consumer's queue must be polled even when listen_queues names only unrelated queues, else its messages would sit ENQUEUED forever.
    server = "localhost:9092"
    topic = f"dbos-kafka-listen-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    event = threading.Event()
    lock = threading.Lock()
    seen: set[int] = set()

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-listen-queues",
            "auto.offset.reset": "earliest",
        },
        [topic],
    )
    @DBOS.workflow()
    def listen_workflow(msg: KafkaMessage) -> None:
        assert msg.value is not None
        with lock:
            seen.add(int(msg.value.decode().split()[-1]))  # type: ignore
            if len(seen) == NUM_EVENTS:
                event.set()

    # Listen only to an unrelated queue — deliberately NOT the internal Kafka queue.
    DBOS.listen_queues(["dbos-test-unrelated-queue"])
    DBOS.launch()

    assert event.wait(timeout=30)
    assert seen == set(range(NUM_EVENTS))


def test_kafka_listen_queues_polls_db_backed_consumer_queue(
    dbos: DBOS, config: DBOSConfig
) -> None:
    # Regression (M1): a consumer's custom queue must be polled even when it is
    # database-backed — i.e. absent from the in-memory registry — and listen_queues
    # names only unrelated queues. The poller must resolve it from the DB, else its
    # messages sit ENQUEUED forever.
    server = "localhost:9092"
    topic = f"dbos-kafka-dbq-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    # Persist a database-backed queue via the public API: it lives in the DB, not the
    # in-memory registry. The fixture instance is launched, so _sys_db is available.
    queue_name = f"dbos-test-kafka-dbq-{random.randrange(1_000_000_000)}"
    registered = DBOS.register_queue(queue_name, concurrency=10)
    assert registered.database_backed_queue is True

    # Fresh, un-launched instance so we control decoration, listen_queues, and launch.
    # The queue row survives destroy (only connections are torn down).
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    event = threading.Event()
    lock = threading.Lock()
    seen: set[int] = set()

    # The queue is named, and nothing adds it to the in-memory registry, so the poller can only reach it by resolving it from the DB.
    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-dbq-listen",
            "auto.offset.reset": "earliest",
        },
        [topic],
        queue_name=queue_name,
    )
    @DBOS.workflow()
    def db_queue_workflow(msg: KafkaMessage) -> None:
        assert msg.value is not None
        with lock:
            seen.add(int(msg.value.decode().split()[-1]))  # type: ignore
            if len(seen) == NUM_EVENTS:
                event.set()

    # Listen only to an unrelated queue — deliberately NOT the consumer's queue.
    DBOS.listen_queues(["dbos-test-unrelated-queue"])
    DBOS.launch()

    assert event.wait(timeout=30)
    assert seen == set(range(NUM_EVENTS))


def test_kafka_throughput(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch, skip_with_sqlite: None
) -> None:
    # Ingest throughput: batched enqueue must far exceed the old one-txn-per-message path.
    server = "localhost:9092"
    topic = f"dbos-kafka-bench-{random.randrange(1_000_000_000)}"
    num_messages = 1000

    try:
        producer = Producer({"bootstrap.servers": server})
        for i in range(num_messages):
            producer.produce(topic, value=f"message-{i}")
        producer.poll(10)
        if producer.flush(10) > 0:
            pytest.skip("Kafka not available")
    except Exception:
        pytest.skip("Kafka not available")

    inserted_total = 0
    first_insert_at: Optional[float] = None
    last_insert_at: Optional[float] = None
    done = threading.Event()
    original_init_workflows = dbos._sys_db.init_workflows

    def counting_init_workflows(statuses: Any) -> Any:
        nonlocal inserted_total, first_insert_at, last_insert_at
        if first_insert_at is None:
            first_insert_at = time.monotonic()
        result = original_init_workflows(statuses)
        inserted_total += len(result)
        if inserted_total >= num_messages:
            last_insert_at = time.monotonic()
            done.set()
        return result

    monkeypatch.setattr(dbos._sys_db, "init_workflows", counting_init_workflows)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-throughput",
            "auto.offset.reset": "earliest",
        },
        [topic],
    )
    @DBOS.workflow()
    def bench_workflow(msg: KafkaMessage) -> None:
        pass

    assert done.wait(timeout=60)
    assert first_insert_at is not None and last_insert_at is not None
    elapsed = max(last_insert_at - first_insert_at, 0.001)
    rate = num_messages / elapsed
    print(
        f"Kafka ingest throughput: {rate:.0f} msg/s ({num_messages} in {elapsed:.2f}s)"
    )
    # Conservative floor: the pre-redesign serial path measured ~315 msg/s on a
    # local database, and batching should exceed that
    assert rate > 500


def test_kafka_duplicate_group_validation(dbos: DBOS) -> None:
    # Two consumers sharing a group.id and topic would split messages between them.
    from dbos._error import DBOSInitializationError

    config = {"bootstrap.servers": "localhost:9092", "group.id": "dbos-test-dup-group"}
    topic = "dbos-test-dup-topic"

    @DBOS.kafka_consumer(dict(config), [topic])
    @DBOS.workflow()
    def consumer_one(msg: KafkaMessage) -> None:
        pass

    with pytest.raises(DBOSInitializationError, match="share"):

        @DBOS.kafka_consumer(dict(config), [topic])
        @DBOS.workflow()
        def consumer_two(msg: KafkaMessage) -> None:
            pass


def test_kafka_two_groups_same_topic(dbos: DBOS) -> None:
    # Distinct group IDs fan out: each consumer group independently sees every
    # message on the shared topic (workflow IDs are namespaced by group.id).
    server = "localhost:9092"
    topic = f"dbos-kafka-fanout-{random.randrange(1_000_000_000)}"

    if not send_test_messages(server, topic):
        pytest.skip("Kafka not available")

    lock = threading.Lock()
    seen: dict[str, set[int]] = {"a": set(), "b": set()}
    done = threading.Event()

    def record(group: str, msg: KafkaMessage) -> None:
        with lock:
            seen[group].add(int(msg.value.decode().split()[-1]))  # type: ignore
            if len(seen["a"]) == NUM_EVENTS and len(seen["b"]) == NUM_EVENTS:
                done.set()

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-fanout-a",
            "auto.offset.reset": "earliest",
        },
        [topic],
    )
    @DBOS.workflow()
    def consumer_a(msg: KafkaMessage) -> None:
        record("a", msg)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-fanout-b",
            "auto.offset.reset": "earliest",
        },
        [topic],
    )
    @DBOS.workflow()
    def consumer_b(msg: KafkaMessage) -> None:
        record("b", msg)

    assert done.wait(timeout=30)
    # Both groups saw every message, not a split of the topic between them.
    assert seen["a"] == set(range(NUM_EVENTS))
    assert seen["b"] == set(range(NUM_EVENTS))


def test_kafka_two_ordered_consumers_same_topic(dbos: DBOS) -> None:
    # Pre-overhaul, ordered ("in_order") consumers got a topic-named queue, so a
    # second consumer on the same topic crashed with "already been declared". The
    # shared partitioned queue removes that limit: two ordered consumers with
    # distinct group IDs now coexist, each seeing every message in offset order.
    from confluent_kafka.admin import AdminClient, NewTopic

    server = "localhost:9092"
    topic = f"dbos-kafka-ordered2-{random.randrange(1_000_000_000)}"
    num_messages = 8

    admin = AdminClient({"bootstrap.servers": server})
    try:
        # One partition so each group's messages have a single, well-defined order.
        admin.create_topics([NewTopic(topic, num_partitions=1)])[topic].result()
    except Exception:
        pytest.skip("Kafka not available")

    producer = Producer({"bootstrap.servers": server})
    for i in range(num_messages):
        producer.produce(topic, partition=0, value=f"{i}")
    if producer.flush(10) > 0:
        pytest.skip("Kafka not available")

    lock = threading.Lock()
    seen: dict[str, list[int]] = {"a": [], "b": []}
    done = threading.Event()

    def record(group: str, msg: KafkaMessage) -> None:
        with lock:
            seen[group].append(int(msg.value))  # type: ignore
            if len(seen["a"]) == num_messages and len(seen["b"]) == num_messages:
                done.set()

    # Both decorators registering without raising is itself the regression check.
    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-ordered2-a",
            "auto.offset.reset": "earliest",
        },
        [topic],
        ordering="topic",
    )
    @DBOS.workflow()
    def ordered_a(msg: KafkaMessage) -> None:
        record("a", msg)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-ordered2-b",
            "auto.offset.reset": "earliest",
        },
        [topic],
        ordering="topic",
    )
    @DBOS.workflow()
    def ordered_b(msg: KafkaMessage) -> None:
        record("b", msg)

    assert done.wait(timeout=60)
    # Each group independently saw every message in exact offset order.
    assert seen["a"] == list(range(num_messages))
    assert seen["b"] == list(range(num_messages))


def test_kafka_config_not_mutated(dbos: DBOS) -> None:
    config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "dbos-test-no-mutate",
    }
    snapshot = dict(config)

    @DBOS.kafka_consumer(config, ["dbos-test-no-mutate-topic"])
    @DBOS.workflow()
    def no_mutate_workflow(msg: KafkaMessage) -> None:
        pass

    assert config == snapshot


def test_describe_kafka_error_formats_real_error() -> None:
    described = _describe_kafka_error(KafkaError(KafkaError._ALL_BROKERS_DOWN))
    assert "_ALL_BROKERS_DOWN" in described
    assert "broker" in described.lower()


def test_describe_kafka_error_never_raises() -> None:
    """librdkafka's callback dispatch can leave CPython's error indicator set, which makes
    every C call on the KafkaError -- including formatting -- raise SystemError."""

    class Poisoned:
        def name(self) -> NoReturn:
            raise SystemError("returned a result with an exception set")

        code = str = name

    # Falls back to repr when the accessors fail.
    assert "Poisoned" in _describe_kafka_error(Poisoned())  # type: ignore[arg-type]

    class FullyPoisoned(Poisoned):
        def __repr__(self) -> NoReturn:
            raise SystemError("returned a result with an exception set")

    # Degrades to a placeholder rather than propagating into librdkafka.
    assert _describe_kafka_error(FullyPoisoned()) == (  # type: ignore[arg-type]
        "<KafkaError that could not be formatted>"
    )


def test_kafka_error_cb_reports_and_contains_user_callback() -> None:
    seen: list[KafkaError] = []

    def user_error_cb(err: KafkaError) -> NoReturn:
        seen.append(err)
        raise RuntimeError("user callback blew up")

    on_error = _make_error_cb(
        "my_consumer", "my-group", ["topic-a", "topic-b"], user_error_cb
    )
    err = KafkaError(KafkaError._ALL_BROKERS_DOWN)
    # Attach to the dbos logger directly: DBOS sets propagate=False, so caplog's root handler may see nothing.
    records: list[logging.LogRecord] = []

    class Capture(logging.Handler):
        def emit(self, record: logging.LogRecord) -> None:
            records.append(record)

    handler = Capture()
    dbos_logger.addHandler(handler)
    old_level = dbos_logger.level
    dbos_logger.setLevel(logging.WARNING)
    try:
        on_error(
            err
        )  # a failing user callback must not escape into librdkafka's dispatch
    finally:
        dbos_logger.removeHandler(handler)
        dbos_logger.setLevel(old_level)

    # The caller's callback is chained, not silently dropped.
    assert seen == [err]
    logged = "\n".join(r.getMessage() for r in records)
    # The error is identifiable: which consumer, which group, which topics, what failed.
    for expected in (
        "my_consumer",
        "my-group",
        "topic-a",
        "topic-b",
        "_ALL_BROKERS_DOWN",
    ):
        assert expected in logged
    assert "user callback blew up" in logged


def test_kafka_queue_polling_interval(
    config: DBOSConfig, cleanup_test_databases: None
) -> None:
    # The internal Kafka queues take their polling interval from DBOSConfig, whether a
    # consumer is declared before DBOS is constructed (queue created before the config is
    # known, so launch applies it) or after (queue created with it).
    from dbos._kafka import KAFKA_ORDERED_QUEUE_NAME, KAFKA_QUEUE_NAME

    DBOS.destroy(destroy_registry=True)
    config["kafka_queue_polling_interval_sec"] = 7.5
    server = "localhost:9092"
    suffix = random.randrange(1_000_000_000)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": f"dbos-kafka-interval-early-{suffix}",
        },
        [f"dbos-kafka-interval-early-{suffix}"],
        ordering="partition",
    )
    @DBOS.workflow()
    def early_consumer(msg: KafkaMessage) -> None:
        pass

    dbos = DBOS(config=config)
    try:

        @DBOS.kafka_consumer(
            {
                "bootstrap.servers": server,
                "group.id": f"dbos-kafka-interval-late-{suffix}",
            },
            [f"dbos-kafka-interval-late-{suffix}"],
        )
        @DBOS.workflow()
        def late_consumer(msg: KafkaMessage) -> None:
            pass

        queues = dbos._registry.queue_info_map
        # The late consumer's queue was created after the config was known.
        assert queues[KAFKA_QUEUE_NAME].polling_interval_sec == 7.5
        DBOS.launch()
        assert queues[KAFKA_ORDERED_QUEUE_NAME].polling_interval_sec == 7.5
        assert queues[KAFKA_QUEUE_NAME].polling_interval_sec == 7.5
    finally:
        DBOS.destroy(destroy_registry=True)


def test_kafka_queue_polling_interval_default(
    config: DBOSConfig, cleanup_test_databases: None
) -> None:
    # Unconfigured, launch applies the Queue default. Registered pre-launch so
    # configure_kafka_queues actually runs: it must resolve the unset config to the
    # default rather than assigning None, which would break the queue worker's wait.
    from dbos._kafka import KAFKA_QUEUE_NAME

    DBOS.destroy(destroy_registry=True)
    suffix = random.randrange(1_000_000_000)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"dbos-kafka-interval-unset-{suffix}",
        },
        [f"dbos-kafka-interval-unset-{suffix}"],
    )
    @DBOS.workflow()
    def unset_consumer(msg: KafkaMessage) -> None:
        pass

    dbos = DBOS(config=config)
    try:
        queues = dbos._registry.queue_info_map
        assert queues[KAFKA_QUEUE_NAME].polling_interval_sec == 1.0
        DBOS.launch()
        assert queues[KAFKA_QUEUE_NAME].polling_interval_sec == 1.0
    finally:
        DBOS.destroy(destroy_registry=True)


def test_kafka_queue_polling_interval_not_stale_across_reinit(
    config: DBOSConfig, cleanup_test_databases: None
) -> None:
    # destroy() without destroy_registry keeps the queue objects, so a second DBOS
    # that does not configure an interval must reset them to the default rather than
    # inherit the previous run's value.
    from dbos._kafka import KAFKA_QUEUE_NAME

    DBOS.destroy(destroy_registry=True)
    suffix = random.randrange(1_000_000_000)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"dbos-kafka-interval-stale-{suffix}",
        },
        [f"dbos-kafka-interval-stale-{suffix}"],
    )
    @DBOS.workflow()
    def stale_consumer(msg: KafkaMessage) -> None:
        pass

    try:
        config["kafka_queue_polling_interval_sec"] = 7.5
        dbos = DBOS(config=config)
        DBOS.launch()
        assert (
            dbos._registry.queue_info_map[KAFKA_QUEUE_NAME].polling_interval_sec == 7.5
        )

        # Keep the registry, so the same Queue object carries 7.5 into the next run.
        DBOS.destroy()
        del config["kafka_queue_polling_interval_sec"]
        dbos = DBOS(config=config)
        DBOS.launch()
        assert (
            dbos._registry.queue_info_map[KAFKA_QUEUE_NAME].polling_interval_sec == 1.0
        )
    finally:
        DBOS.destroy(destroy_registry=True)


def test_init_workflows_idempotent(dbos: DBOS) -> None:
    # Batch insert dedups on workflow ID, making Kafka redelivery a no-op.
    from dbos._core import prepare_enqueued_workflow

    @DBOS.workflow()
    def batch_workflow(value: str) -> None:
        pass

    # Use an undeclared queue so the rows sit ENQUEUED and aren't executed
    queue_name = f"unpolled-queue-{random.randrange(1_000_000_000)}"
    prefix = f"batch-test-{random.randrange(1_000_000_000)}"

    def build_statuses() -> list[Any]:
        return [
            prepare_enqueued_workflow(
                dbos,
                batch_workflow,
                (f"value-{i}",),
                {},
                queue_name=queue_name,
                workflow_id=f"{prefix}-{i}",
            )
            for i in range(5)
        ]

    inserted = dbos._sys_db.init_workflows(build_statuses())
    assert inserted == {f"{prefix}-{i}" for i in range(5)}
    # Redelivering the same batch inserts nothing
    assert dbos._sys_db.init_workflows(build_statuses()) == set()
    # A partially-redelivered batch inserts only the new rows
    statuses = build_statuses()
    statuses[0]["workflow_uuid"] = f"{prefix}-new"
    assert dbos._sys_db.init_workflows(statuses) == {f"{prefix}-new"}


def test_kafka_ordering_across_batches(dbos: DBOS) -> None:
    # Per-partition order must hold when a partition's backlog spans many batches: a small
    # batch_size splits each partition across ~20 batches, which reorder unless created_at is monotonic across batches.
    from confluent_kafka.admin import AdminClient, NewTopic

    server = "localhost:9092"
    topic = f"dbos-kafka-batches-{random.randrange(1_000_000_000)}"
    num_partitions = 6
    num_per_partition = 20
    batch_size = 5  # each partition's 20 msgs land in ~20 batches

    admin = AdminClient({"bootstrap.servers": server})
    try:
        admin.create_topics([NewTopic(topic, num_partitions=num_partitions)])[
            topic
        ].result()
    except Exception:
        pytest.skip("Kafka not available")

    producer = Producer({"bootstrap.servers": server})
    for _ in range(30):
        metadata = producer.list_topics(topic, timeout=5)
        if len(metadata.topics[topic].partitions) == num_partitions:
            break
        time.sleep(0.5)
    for p in range(num_partitions):
        for i in range(num_per_partition):
            producer.produce(topic, partition=p, value=f"{i}")
    if producer.flush(10) > 0:
        pytest.skip("Kafka not available")

    lock = threading.Lock()
    seen: dict[int, list[int]] = {p: [] for p in range(num_partitions)}
    done = threading.Event()

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": f"dbos-test-batch-ordering-{random.randrange(1_000_000_000)}",
            "auto.offset.reset": "earliest",
        },
        [topic],
        ordering="partition",
        batch_size=batch_size,
    )
    @DBOS.workflow()
    def ordered_workflow(msg: KafkaMessage) -> None:
        with lock:
            assert msg.partition is not None
            seen[msg.partition].append(int(msg.value))  # type: ignore
            if sum(len(v) for v in seen.values()) == num_partitions * num_per_partition:
                done.set()

    assert done.wait(timeout=90)
    # Exact Kafka per-partition delivery order, across every batch boundary.
    for p in range(num_partitions):
        assert seen[p] == list(range(num_per_partition))


def test_kafka_topic_ordering_multi_partition(dbos: DBOS) -> None:
    # ordering="topic" across many partitions: the whole topic runs serially (concurrency=1) while each partition stays in offset order.
    from confluent_kafka.admin import AdminClient, NewTopic

    server = "localhost:9092"
    topic = f"dbos-kafka-topicorder-{random.randrange(1_000_000_000)}"
    num_partitions = 4
    num_per_partition = 3

    admin = AdminClient({"bootstrap.servers": server})
    try:
        admin.create_topics([NewTopic(topic, num_partitions=num_partitions)])[
            topic
        ].result()
    except Exception:
        pytest.skip("Kafka not available")

    producer = Producer({"bootstrap.servers": server})
    for _ in range(30):
        metadata = producer.list_topics(topic, timeout=5)
        if len(metadata.topics[topic].partitions) == num_partitions:
            break
        time.sleep(0.5)
    for p in range(num_partitions):
        for i in range(num_per_partition):
            producer.produce(topic, partition=p, value=f"{i}")
    if producer.flush(10) > 0:
        pytest.skip("Kafka not available")

    lock = threading.Lock()
    seen: dict[int, list[int]] = {p: [] for p in range(num_partitions)}
    active = 0
    max_active = 0
    done = threading.Event()

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-topic-ordering",
            "auto.offset.reset": "earliest",
        },
        [topic],
        ordering="topic",
    )
    @DBOS.workflow()
    def topic_workflow(msg: KafkaMessage) -> None:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.1)  # widen the window so any concurrent run is observed
        with lock:
            active -= 1
            assert msg.partition is not None
            seen[msg.partition].append(int(msg.value))  # type: ignore
            if sum(len(v) for v in seen.values()) == num_partitions * num_per_partition:
                done.set()

    assert done.wait(timeout=60)
    # Each partition's messages stayed in offset order.
    for p in range(num_partitions):
        assert seen[p] == list(range(num_per_partition))
    # The whole topic ran serially: never two workflows at once.
    assert max_active == 1


def test_init_workflows_large_batch_per_key_ordering(dbos: DBOS) -> None:
    # A large batch (past the 500-row insert chunk) over many partition keys: created_at must be monotonic within each key, and per-key cursors keep keys decoupled.
    import sqlalchemy as sa

    from dbos._core import prepare_enqueued_workflow
    from dbos._schemas.system_database import SystemSchema

    @DBOS.workflow()
    def big_batch_workflow(value: str) -> None:
        pass

    queue_name = f"unpolled-{random.randrange(1_000_000_000)}"
    prefix = f"bigbatch-{random.randrange(1_000_000_000)}"
    num_keys = 3
    per_key = 400  # num_keys * per_key = 1200 rows, past the 500-row chunk boundary

    statuses = [
        prepare_enqueued_workflow(
            dbos,
            big_batch_workflow,
            (f"{k}-{i}",),
            {},
            queue_name=queue_name,
            workflow_id=f"{prefix}-{k}-{i}",
            queue_partition_key=f"{prefix}-key-{k}",
        )
        for i in range(per_key)
        for k in range(num_keys)  # interleave keys so each key's rows span the batch
    ]
    assert len(dbos._sys_db.init_workflows(statuses)) == num_keys * per_key

    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.workflow_status.c.created_at,
                SystemSchema.workflow_status.c.queue_partition_key,
            ).where(SystemSchema.workflow_status.c.workflow_uuid.like(f"{prefix}-%"))
        ).fetchall()

    assert len(rows) == num_keys * per_key
    by_key: dict[str, list[tuple[int, int]]] = {
        f"{prefix}-key-{k}": [] for k in range(num_keys)
    }
    for wfid, created_at, part_key in rows:
        by_key[part_key].append((created_at, int(wfid.rsplit("-", 1)[1])))

    for k in range(num_keys):
        pairs = sorted(by_key[f"{prefix}-key-{k}"])  # by created_at
        # created_at order equals enqueue (offset) order, strictly increasing, no ties
        assert [offset for _, offset in pairs] == list(range(per_key))
        created = [c for c, _ in pairs]
        assert created == sorted(set(created))

    all_created = [created_at for _, created_at, _ in rows]
    # Per-key cursors keep keys decoupled: all rows fit one per_key-wide window (a process-wide cursor would span num_keys * per_key).
    assert max(all_created) - min(all_created) == per_key - 1


def test_init_workflows_cursor_survives_restart(dbos: DBOS) -> None:
    # Regression: a fresh owner (restart/rebalance) must re-seed the in-memory created_at cursor from the DB, else newer messages sort below a future-dated ENQUEUED backlog and invert per-key order.
    import sqlalchemy as sa

    from dbos._core import prepare_enqueued_workflow
    from dbos._schemas.system_database import SystemSchema

    @DBOS.workflow()
    def restart_workflow(value: str) -> None:
        pass

    # Unpolled queue: the backlog sits ENQUEUED and is never dequeued.
    queue_name = f"unpolled-{random.randrange(1_000_000_000)}"
    prefix = f"restart-{random.randrange(1_000_000_000)}"
    key = f"{prefix}-key"

    def statuses(offsets: range) -> list[Any]:
        return [
            prepare_enqueued_workflow(
                dbos,
                restart_workflow,
                (f"value-{i}",),
                {},
                queue_name=queue_name,
                workflow_id=f"{prefix}-{i}",
                queue_partition_key=key,
            )
            for i in offsets
        ]

    # Simulate drift: force the cursor an hour ahead so the backlog (offsets 0-4) is future-dated and stays ENQUEUED.
    future = int(time.time() * 1000) + 3_600_000
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors[(queue_name, key)] = future
    assert len(dbos._sys_db.init_workflows(statuses(range(0, 5)))) == 5

    # Process A restarts / the partition rebalances: a fresh owner has no in-memory cursor.
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors.clear()

    # The new owner enqueues the next messages (offsets 5-9).
    assert len(dbos._sys_db.init_workflows(statuses(range(5, 10)))) == 5

    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.workflow_status.c.created_at,
            ).where(SystemSchema.workflow_status.c.workflow_uuid.like(f"{prefix}-%"))
        ).fetchall()

    created_by_offset = {int(wfid.rsplit("-", 1)[1]): ca for wfid, ca in rows}
    assert len(created_by_offset) == 10
    ordered = [created_by_offset[i] for i in range(10)]
    # created_at strictly increases in offset order: no inversion across the restart.
    assert ordered == sorted(ordered)
    assert len(set(ordered)) == 10
    # Every post-restart message sorts after the entire pre-restart backlog.
    assert min(ordered[5:]) > max(ordered[:5])


def test_init_workflows_cursor_scoped_per_queue(dbos: DBOS) -> None:
    # Cursors are per (queue, partition key), matching the dequeue scope: the same key on another queue must not inherit this queue's high-water mark.
    import sqlalchemy as sa

    from dbos._core import prepare_enqueued_workflow
    from dbos._schemas.system_database import SystemSchema

    @DBOS.workflow()
    def per_queue_workflow(value: str) -> None:
        pass

    prefix = f"perqueue-{random.randrange(1_000_000_000)}"
    queue_a = f"unpolled-a-{random.randrange(1_000_000_000)}"
    queue_b = f"unpolled-b-{random.randrange(1_000_000_000)}"
    key = (
        f"{prefix}-shared-key"  # one key string, deliberately reused across both queues
    )

    def statuses(queue_name: str, offsets: range) -> list[Any]:
        return [
            prepare_enqueued_workflow(
                dbos,
                per_queue_workflow,
                (f"value-{i}",),
                {},
                queue_name=queue_name,
                workflow_id=f"{prefix}-{queue_name}-{i}",
                queue_partition_key=key,
            )
            for i in offsets
        ]

    # Drift queue_a's cursor an hour ahead so its rows are future-dated and stay ENQUEUED.
    future = int(time.time() * 1000) + 3_600_000
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors[(queue_a, key)] = future
    assert len(dbos._sys_db.init_workflows(statuses(queue_a, range(5)))) == 5

    # Restart: queue_b's first sight of the key must seed from queue_b's own rows (there are none), not from queue_a's.
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors.clear()
    before_ms = int(time.time() * 1000)
    assert len(dbos._sys_db.init_workflows(statuses(queue_b, range(5)))) == 5

    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.queue_name,
                SystemSchema.workflow_status.c.created_at,
            ).where(SystemSchema.workflow_status.c.workflow_uuid.like(f"{prefix}-%"))
        ).fetchall()

    by_queue: dict[str, list[int]] = {queue_a: [], queue_b: []}
    for queue_name, created_at in rows:
        by_queue[queue_name].append(created_at)
    assert len(by_queue[queue_a]) == 5
    assert len(by_queue[queue_b]) == 5
    # queue_b starts at wall clock: a key-only cursor would have seeded it from queue_a's future-dated backlog.
    assert min(by_queue[queue_b]) >= before_ms
    assert max(by_queue[queue_b]) < min(by_queue[queue_a])
    # Only the pair actually enqueued after the restart is tracked.
    assert (queue_b, key) in dbos._sys_db._batch_created_at_cursors
    assert (queue_a, key) not in dbos._sys_db._batch_created_at_cursors


def test_init_workflows_seeds_from_pending_rows(dbos: DBOS) -> None:
    # Seeding must span PENDING rows, not just ENQUEUED: recovery re-enqueues a PENDING row preserving its created_at, so a cursor seeded only from ENQUEUED rows would stamp newer messages below it.
    import sqlalchemy as sa

    from dbos._core import prepare_enqueued_workflow
    from dbos._schemas.system_database import SystemSchema

    @DBOS.workflow()
    def pending_seed_workflow(value: str) -> None:
        pass

    prefix = f"pendseed-{random.randrange(1_000_000_000)}"
    queue_name = f"unpolled-{random.randrange(1_000_000_000)}"
    key = f"{prefix}-key"

    def statuses(offsets: range) -> list[Any]:
        return [
            prepare_enqueued_workflow(
                dbos,
                pending_seed_workflow,
                (f"value-{i}",),
                {},
                queue_name=queue_name,
                workflow_id=f"{prefix}-{i}",
                queue_partition_key=key,
            )
            for i in offsets
        ]

    # Drift the cursor an hour ahead so the backlog (offsets 0-4) is future-dated.
    future = int(time.time() * 1000) + 3_600_000
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors[(queue_name, key)] = future
    assert len(dbos._sys_db.init_workflows(statuses(range(0, 5)))) == 5

    # The entire backlog goes in flight; a foreign executor_id keeps local recovery from claiming these rows.
    with dbos._sys_db.engine.begin() as conn:
        conn.execute(
            sa.update(SystemSchema.workflow_status)
            .where(SystemSchema.workflow_status.c.workflow_uuid.like(f"{prefix}-%"))
            .values(
                status="PENDING",
                executor_id=f"other-{random.randrange(1_000_000_000)}",
            )
        )

    # Fresh owner after a restart: PENDING rows are now the only high-water mark available.
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors.clear()
    assert len(dbos._sys_db.init_workflows(statuses(range(5, 10)))) == 5

    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.workflow_status.c.created_at,
            ).where(SystemSchema.workflow_status.c.workflow_uuid.like(f"{prefix}-%"))
        ).fetchall()

    created_by_offset = {int(wfid.rsplit("-", 1)[1]): ca for wfid, ca in rows}
    assert len(created_by_offset) == 10
    ordered = [created_by_offset[i] for i in range(10)]
    assert ordered == sorted(ordered)
    assert len(set(ordered)) == 10
    # Post-restart messages sort after the in-flight backlog, which recovery may re-enqueue at its original created_at.
    assert min(ordered[5:]) > max(ordered[:5])


def test_init_workflows_multi_queue_batch_seeds_only_requested_pairs(
    dbos: DBOS,
) -> None:
    # The seed query's two IN lists cross-product, so it returns groups for (queue, key) pairs the batch never asked about; those must be discarded rather than cached as cursors.
    import sqlalchemy as sa

    from dbos._core import prepare_enqueued_workflow
    from dbos._schemas.system_database import SystemSchema

    @DBOS.workflow()
    def cross_product_workflow(value: str) -> None:
        pass

    prefix = f"crossp-{random.randrange(1_000_000_000)}"
    queue_a = f"unpolled-a-{random.randrange(1_000_000_000)}"
    queue_b = f"unpolled-b-{random.randrange(1_000_000_000)}"
    key_a = f"{prefix}-key-a"
    key_b = f"{prefix}-key-b"

    def status(queue_name: str, key: str, tag: str) -> Any:
        return prepare_enqueued_workflow(
            dbos,
            cross_product_workflow,
            (tag,),
            {},
            queue_name=queue_name,
            workflow_id=f"{prefix}-{tag}",
            queue_partition_key=key,
        )

    # Give the off-diagonal pair (queue_a, key_b) real rows: the batch below never requests it, but the cross product will match it.
    future = int(time.time() * 1000) + 3_600_000
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors[(queue_a, key_b)] = future
    assert len(dbos._sys_db.init_workflows([status(queue_a, key_b, "offdiag")])) == 1

    # A single batch spanning two queues, requesting only the diagonal pairs.
    with dbos._sys_db._batch_created_at_lock:
        dbos._sys_db._batch_created_at_cursors.clear()
    before_ms = int(time.time() * 1000)
    inserted = dbos._sys_db.init_workflows(
        [status(queue_a, key_a, "diag-a"), status(queue_b, key_b, "diag-b")]
    )
    assert inserted == {f"{prefix}-diag-a", f"{prefix}-diag-b"}

    cursors = dbos._sys_db._batch_created_at_cursors
    assert (queue_a, key_a) in cursors
    assert (queue_b, key_b) in cursors
    # The cross-product match must not leak into the cursor map.
    assert (queue_a, key_b) not in cursors

    with dbos._sys_db.engine.begin() as conn:
        rows = conn.execute(
            sa.select(
                SystemSchema.workflow_status.c.workflow_uuid,
                SystemSchema.workflow_status.c.created_at,
            ).where(
                SystemSchema.workflow_status.c.workflow_uuid.like(f"{prefix}-diag-%")
            )
        ).fetchall()

    # Both requested pairs start at wall clock, untouched by the off-diagonal row's future-dated created_at.
    assert len(rows) == 2
    for _, created_at in rows:
        assert before_ms <= created_at < future


# ---------------------------------------------------------------------------
# Decorator validation, config coercion, and helpers (no broker required)
# ---------------------------------------------------------------------------


def test_kafka_validation_errors(dbos: DBOS) -> None:
    # Every decorator misconfiguration must raise DBOSInitializationError before
    # any consumer is registered. None of these need a broker.
    from dbos._error import DBOSInitializationError

    suffix = random.randrange(1_000_000_000)

    def consumer(**kwargs: Any) -> Any:
        gid = kwargs.pop("gid")
        return DBOS.kafka_consumer(
            {"bootstrap.servers": "localhost:9092", "group.id": gid}, ["t"], **kwargs
        )

    with pytest.raises(DBOSInitializationError, match="either in_order or ordering"):
        consumer(gid=f"v1-{suffix}", in_order=True, ordering="topic")
    with pytest.raises(DBOSInitializationError, match="invalid Kafka ordering"):
        consumer(gid=f"v2-{suffix}", ordering="bogus")
    with pytest.raises(DBOSInitializationError, match="batch_size must be positive"):
        consumer(gid=f"v3-{suffix}", batch_size=0)
    with pytest.raises(DBOSInitializationError, match="only supported with ordering"):
        consumer(gid=f"v4-{suffix}", ordering="partition", queue_name=f"vq-a-{suffix}")


def test_kafka_unknown_queue_name_is_allowed(dbos: DBOS, config: DBOSConfig) -> None:
    # A consumer may name a queue that does not exist yet: it can be registered after launch, so neither decorating nor launching may reject it. No broker needed — nothing here waits on a message.
    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"unknown-q-{random.randrange(1_000_000_000)}",
        },
        ["t"],
        queue_name=f"never-registered-{random.randrange(1_000_000_000)}",
    )
    @DBOS.workflow()
    def unknown_queue_wf(msg: KafkaMessage) -> None:
        pass

    DBOS.launch()


def test_kafka_partitioned_queue_name_rejected_at_launch(
    dbos: DBOS, config: DBOSConfig
) -> None:
    # ordering="none" enqueues no partition key, which a partitioned queue never dequeues, so a partitioned custom queue must be rejected rather than leave its workflows ENQUEUED forever. The queue is named, so the check lands at launch, before the consumer thread starts — no broker needed.
    from dbos._error import DBOSInitializationError

    # Register on the launched fixture instance; the row survives destroy.
    queue_name = f"dbos-test-kafka-partq-{random.randrange(1_000_000_000)}"
    DBOS.register_queue(queue_name, partition_queue=True)

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"partq-{random.randrange(1_000_000_000)}",
        },
        ["t"],
        queue_name=queue_name,
    )
    @DBOS.workflow()
    def partitioned_queue_wf(msg: KafkaMessage) -> None:
        pass

    with pytest.raises(DBOSInitializationError, match="is a partitioned queue"):
        DBOS.launch()


def test_kafka_partitioned_in_memory_queue_rejected_at_launch(
    dbos: DBOS, config: DBOSConfig
) -> None:
    # Same rejection, but for an in-memory queue, which lives only in the registry and has no database row: this is the sole cover for resolving a named queue from queue_info_map rather than from the DB.
    from dbos._error import DBOSInitializationError

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    queue_name = f"dbos-test-kafka-partq-inmem-{random.randrange(1_000_000_000)}"
    Queue(queue_name, partition_queue=True)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": f"partq-inmem-{random.randrange(1_000_000_000)}",
        },
        ["t"],
        queue_name=queue_name,
    )
    @DBOS.workflow()
    def in_memory_partitioned_queue_wf(msg: KafkaMessage) -> None:
        pass

    with pytest.raises(DBOSInitializationError, match="is a partitioned queue"):
        DBOS.launch()


def test_kafka_partitioned_queue_name_rejected_when_launched(dbos: DBOS) -> None:
    # Same check as above, but a consumer declared after launch can resolve its queue immediately, so the decorator itself must reject it.
    from dbos._error import DBOSInitializationError

    queue_name = f"dbos-test-kafka-partq-live-{random.randrange(1_000_000_000)}"
    DBOS.register_queue(queue_name, partition_queue=True)

    @DBOS.workflow()
    def live_partitioned_queue_wf(msg: KafkaMessage) -> None:
        pass

    with pytest.raises(DBOSInitializationError, match="is a partitioned queue"):
        DBOS.kafka_consumer(
            {
                "bootstrap.servers": "localhost:9092",
                "group.id": f"partq-live-{random.randrange(1_000_000_000)}",
            },
            ["t"],
            queue_name=queue_name,
        )(live_partitioned_queue_wf)

    # The rejected consumer left no trace: no registration, no forced poll.
    assert all(
        reg.queue_name != queue_name for reg in dbos._registry.kafka_registrations
    )
    assert queue_name not in dbos._registry.poller_queue_names


def test_kafka_config_coercion(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
    # Inspect the cfg DBOS hands to the consumer loop. Post-launch register_poller
    # starts a real thread, so capture its args instead of running anything.
    captured: dict[str, Any] = {}

    def fake_register_poller(evt: Any, func: Any, *args: Any, **kwargs: Any) -> None:
        captured["cfg"] = args[1]  # (func, cfg, topics, stop, ordering, batch, qname)

    monkeypatch.setattr(dbos._registry, "register_poller", fake_register_poller)

    @DBOS.workflow()
    def cc_wf(msg: KafkaMessage) -> None:
        pass

    def resolved(overrides: dict[str, Any]) -> Any:
        gid = f"cc-{random.randrange(1_000_000_000)}"
        DBOS.kafka_consumer(
            {"bootstrap.servers": "localhost:9092", "group.id": gid, **overrides}, ["t"]
        )(cc_wf)
        return captured["cfg"]

    # Every value librdkafka accepts for a bool must end up truthy so stored
    # offsets flush. (librdkafka rejects "no"/"off" outright, so they're moot.)
    for val in [False, "false", "False", 0, "0", True, "true", 1]:
        got = resolved({"enable.auto.commit": val})["enable.auto.commit"]
        assert got is True or str(got).strip().lower() in (
            "true",
            "1",
        ), f"{val!r}->{got!r}"

    # DBOS manages offset storage itself, so it always disables auto-store.
    assert (
        resolved({"enable.auto.offset.store": True})["enable.auto.offset.store"]
        is False
    )
    assert resolved({})["auto.offset.reset"] == "earliest"
    assert callable(resolved({})["error_cb"])

    # The caller's dict is copied, never mutated.
    caller = {
        "bootstrap.servers": "localhost:9092",
        "group.id": f"cc-mut-{random.randrange(1_000_000_000)}",
    }
    snapshot = dict(caller)
    DBOS.kafka_consumer(caller, ["t"])(cc_wf)
    assert caller == snapshot


def test_kafka_same_group_different_topics_warns(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # Two consumers sharing a group.id but no topics: allowed, but warned about
    # (rebalance churn). No broker needed; suppress the poller so nothing runs.
    from dbos._logger import dbos_logger

    monkeypatch.setattr(dbos._registry, "register_poller", lambda *a, **k: None)
    warnings_logged: list[str] = []
    monkeypatch.setattr(
        dbos_logger, "warning", lambda msg, *a, **k: warnings_logged.append(msg)
    )

    group = f"shared-grp-{random.randrange(1_000_000_000)}"
    base = {"bootstrap.servers": "localhost:9092", "auto.offset.reset": "earliest"}

    @DBOS.kafka_consumer({**base, "group.id": group}, ["topic-a"])
    @DBOS.workflow()
    def consumer_a(msg: KafkaMessage) -> None:
        pass

    @DBOS.kafka_consumer({**base, "group.id": group}, ["topic-b"])
    @DBOS.workflow()
    def consumer_b(msg: KafkaMessage) -> None:
        pass

    assert any(
        "share group.id" in m and "different topics" in m for m in warnings_logged
    ), warnings_logged


def test_prepare_enqueued_workflow_edges(dbos: DBOS) -> None:
    from dbos._core import prepare_enqueued_workflow
    from dbos._error import DBOSWorkflowFunctionNotFoundError

    def unregistered(x: str) -> None:
        pass

    with pytest.raises(DBOSWorkflowFunctionNotFoundError):
        prepare_enqueued_workflow(
            dbos, unregistered, ("a",), {}, queue_name="q", workflow_id="w-unreg"
        )

    @DBOS.workflow()
    def registered(msg: str) -> None:
        pass

    st = prepare_enqueued_workflow(
        dbos,
        registered,
        ("hi",),
        {},
        queue_name="q",
        workflow_id=f"w-{random.randrange(1_000_000_000)}",
        queue_partition_key="pk",
    )
    assert st["status"] == "ENQUEUED"
    assert st["queue_partition_key"] == "pk"
    # Fresh context: no parent/auth leaked from any ambient context.
    assert st["parent_workflow_id"] is None


def test_init_workflows_edge_cases(dbos: DBOS) -> None:
    from dbos._core import prepare_enqueued_workflow

    assert dbos._sys_db.init_workflows([]) == set()

    @DBOS.workflow()
    def edge_wf(v: str) -> None:
        pass

    # Unordered (key=None) rows get wall-clock time and never touch per-key cursors.
    dbos._sys_db._batch_created_at_cursors.clear()
    prefix = f"none-{random.randrange(1_000_000_000)}"
    rows = [
        prepare_enqueued_workflow(
            dbos,
            edge_wf,
            (str(i),),
            {},
            queue_name=f"unp-{prefix}",
            workflow_id=f"{prefix}-{i}",
        )
        for i in range(3)
    ]
    assert dbos._sys_db.init_workflows(rows) == {f"{prefix}-{i}" for i in range(3)}
    assert dbos._sys_db._batch_created_at_cursors == {}


def test_kafka_safe_group_name() -> None:
    from dbos._kafka import safe_group_name

    n = safe_group_name("my_func", ["topic-a", "topic-b"])
    assert n.startswith("dbos-kafka-group-")
    assert n == safe_group_name("my_func", ["topic-a", "topic-b"])  # deterministic
    assert len(safe_group_name("f" * 400, ["t" * 400])) <= 255  # truncated to 255

    # Everything but [a-zA-Z0-9-] is stripped (e.g. regex/underscore chars).
    body = safe_group_name("fn!@#", ["^foo.*bar"])[len("dbos-kafka-group-") :]
    assert all(c.isalnum() or c == "-" for c in body)


def test_kafka_last_message_per_partition() -> None:
    from dbos._kafka import _last_message_per_partition

    class M:
        def __init__(self, t: str, p: int, o: int) -> None:
            self._t, self._p, self._o = t, p, o

        def topic(self) -> str:
            return self._t

        def partition(self) -> int:
            return self._p

        def offset(self) -> int:
            return self._o

    msgs = [M("a", 0, 0), M("a", 1, 0), M("a", 0, 1), M("b", 0, 5), M("a", 1, 2)]
    last = {
        (m.topic(), m.partition()): m.offset()
        for m in _last_message_per_partition(msgs)
    }
    assert last == {("a", 0): 1, ("a", 1): 2, ("b", 0): 5}
    assert _last_message_per_partition([]) == []


def test_kafka_retry_until_success(monkeypatch: pytest.MonkeyPatch) -> None:
    import dbos._kafka as kmod

    monkeypatch.setattr(kmod, "_MIN_RETRY_WAIT_SEC", 0.01)
    monkeypatch.setattr(kmod, "_MAX_RETRY_WAIT_SEC", 0.02)
    stop = threading.Event()

    assert kmod._retry_until_success(stop, lambda: 42, "op") == 42  # succeeds first try

    calls = {"n": 0}

    def flaky() -> str:
        calls["n"] += 1
        if calls["n"] < 3:
            raise ValueError("boom")
        return "ok"

    assert kmod._retry_until_success(stop, flaky, "op") == "ok"  # retries then succeeds
    assert calls["n"] == 3

    # Once stop is set, it abandons without ever calling the operation.
    stop.set()
    called = {"n": 0}

    def never() -> None:
        called["n"] += 1

    assert kmod._retry_until_success(stop, never, "op") is None
    assert called["n"] == 0


# ---------------------------------------------------------------------------
# Consumer-loop failure handling, driven by a fake Consumer (no broker required)
# ---------------------------------------------------------------------------


class _FakeKafkaError:
    def __init__(self, fatal: bool = False) -> None:
        self._fatal = fatal

    def code(self) -> int:
        return -150

    def name(self) -> str:
        return "FAKE"

    def str(self) -> str:
        return "fake kafka error"

    def fatal(self) -> bool:
        return self._fatal


class _FakeKafkaMsg:
    def __init__(
        self,
        topic: str = "t",
        partition: int = 0,
        offset: int = 0,
        value: bytes = b"v",
        err: Any = None,
    ) -> None:
        self._t, self._p, self._o, self._v, self._e = (
            topic,
            partition,
            offset,
            value,
            err,
        )

    def error(self) -> Any:
        return self._e

    def value(self) -> Any:
        return self._v

    def key(self) -> Any:
        return None

    def topic(self) -> str:
        return self._t

    def partition(self) -> int:
        return self._p

    def offset(self) -> int:
        return self._o

    def headers(self) -> Any:
        return None

    def latency(self) -> Any:
        return None

    def leader_epoch(self) -> Any:
        return None

    def timestamp(self) -> Any:
        return (0, 0)


def _start_loop(
    func: Any, group_id: str, queue_name: str
) -> tuple[threading.Thread, threading.Event]:
    import dbos._kafka as kmod

    stop = threading.Event()
    t = threading.Thread(
        target=kmod._kafka_consumer_loop,
        args=(func, {"group.id": group_id}, ["t"], stop, "none", 10, queue_name),
        daemon=True,
    )
    t.start()
    return t, stop


def test_kafka_consumer_loop_fatal_error_recreates_consumer(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A per-message fatal error must trigger a fresh consumer instance.
    import dbos._kafka as kmod

    monkeypatch.setattr(kmod, "_MIN_RETRY_WAIT_SEC", 0.01)
    monkeypatch.setattr(kmod, "_MAX_RETRY_WAIT_SEC", 0.02)

    @DBOS.workflow()
    def wf(msg: KafkaMessage) -> None:
        pass

    instances: list[Any] = []
    recreated = threading.Event()
    ilock = threading.Lock()

    class FatalConsumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            with ilock:
                instances.append(self)
                if len(instances) >= 2:
                    recreated.set()
            self._sent = False

        def subscribe(self, topics: Any) -> None:
            pass

        def consume(self, num_messages: int, timeout: float = 1.0) -> Any:
            if self is instances[0] and not self._sent:
                self._sent = True
                return [_FakeKafkaMsg(err=_FakeKafkaError(fatal=True))]
            time.sleep(0.02)
            return []

        def store_offsets(self, message: Any = None) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(kmod, "Consumer", FatalConsumer)
    t, stop = _start_loop(wf, "mock-fatal", f"mock-q-{random.randrange(1_000_000_000)}")
    try:
        assert recreated.wait(
            timeout=10
        ), "consumer was not recreated after fatal error"
    finally:
        stop.set()
        t.join(timeout=10)


def test_kafka_consumer_loop_recreates_after_unexpected_error(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # An unexpected exception from consume() must back off and rewind onto a fresh consumer.
    import dbos._kafka as kmod

    monkeypatch.setattr(kmod, "_MIN_RETRY_WAIT_SEC", 0.01)
    monkeypatch.setattr(kmod, "_MAX_RETRY_WAIT_SEC", 0.02)

    @DBOS.workflow()
    def wf(msg: KafkaMessage) -> None:
        pass

    instances: list[Any] = []
    recreated = threading.Event()
    ilock = threading.Lock()

    class ErroringConsumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            with ilock:
                instances.append(self)
                if len(instances) >= 2:
                    recreated.set()
            self._raised = False

        def subscribe(self, topics: Any) -> None:
            pass

        def consume(self, num_messages: int, timeout: float = 1.0) -> Any:
            if self is instances[0] and not self._raised:
                self._raised = True
                raise RuntimeError("unexpected consume error")
            time.sleep(0.02)
            return []

        def store_offsets(self, message: Any = None) -> None:
            pass

        def close(self) -> None:
            pass

    monkeypatch.setattr(kmod, "Consumer", ErroringConsumer)
    t, stop = _start_loop(wf, "mock-err", f"mock-q-{random.randrange(1_000_000_000)}")
    try:
        assert recreated.wait(
            timeout=10
        ), "consumer not recreated after unexpected error"
    finally:
        stop.set()
        t.join(timeout=10)


def test_kafka_consumer_loop_store_offsets_exception_continues(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A store_offsets failure (e.g. partition revoked) must be swallowed: the
    # message is already durably enqueued and the loop keeps running.
    import dbos._kafka as kmod

    monkeypatch.setattr(kmod, "_MIN_RETRY_WAIT_SEC", 0.01)

    @DBOS.workflow()
    def wf(msg: KafkaMessage) -> None:
        pass

    enqueued: list[str] = []
    orig_iw = dbos._sys_db.init_workflows

    def spy_iw(statuses: Any) -> Any:
        r = orig_iw(statuses)
        enqueued.extend(r)
        return r

    monkeypatch.setattr(dbos._sys_db, "init_workflows", spy_iw)
    store_attempted = threading.Event()
    suffix = random.randrange(1_000_000_000)

    class StoreFailConsumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            self._sent = False

        def subscribe(self, topics: Any) -> None:
            pass

        def consume(self, num_messages: int, timeout: float = 1.0) -> Any:
            if not self._sent:
                self._sent = True
                return [
                    _FakeKafkaMsg(
                        topic=f"mt-{suffix}", partition=0, offset=0, value=b"hello"
                    )
                ]
            time.sleep(0.02)
            return []

        def store_offsets(self, message: Any = None) -> None:
            store_attempted.set()
            raise KafkaException(KafkaError(KafkaError._STATE))

        def close(self) -> None:
            pass

    monkeypatch.setattr(kmod, "Consumer", StoreFailConsumer)
    t, stop = _start_loop(wf, "mock-store", f"mock-q-{suffix}")
    try:
        assert store_attempted.wait(timeout=10), "store_offsets was never attempted"
        deadline = time.time() + 5
        while len(enqueued) < 1 and time.time() < deadline:
            time.sleep(0.05)
        # Durably enqueued despite the store failure, and the loop is still alive.
        assert len(enqueued) == 1, f"expected 1 enqueued, got {enqueued}"
        assert t.is_alive(), "consumer loop crashed on store_offsets exception"
    finally:
        stop.set()
        t.join(timeout=10)


def test_kafka_poison_message_dropped(
    dbos: DBOS, monkeypatch: pytest.MonkeyPatch
) -> None:
    # A poison message (one whose _build_status raises) is dropped: the rest of the
    # batch is still durably enqueued and the offset advances past the whole batch so
    # the poison message isn't redelivered forever. Driven by a fake consumer.
    import dbos._kafka as kmod

    monkeypatch.setattr(kmod, "_MIN_RETRY_WAIT_SEC", 0.01)

    @DBOS.workflow()
    def poison_wf(msg: KafkaMessage) -> None:
        pass

    group = "mock-poison"
    batch = [
        _FakeKafkaMsg(topic="mt", partition=0, offset=i, value=f"poison-{i}".encode())
        for i in range(5)
    ]

    orig_build = kmod._build_status

    def patched_build(
        d: Any, func: Any, cmsg: Any, gid: str, ordering: Any, qn: str
    ) -> Any:
        if cmsg.value() == b"poison-2":
            raise ValueError("simulated unprocessable message")
        return orig_build(d, func, cmsg, gid, ordering, qn)

    monkeypatch.setattr(kmod, "_build_status", patched_build)

    enqueued: list[str] = []
    iw_done = threading.Event()
    orig_iw = dbos._sys_db.init_workflows

    def spy_iw(statuses: Any) -> Any:
        r = orig_iw(statuses)
        enqueued.extend(r)
        iw_done.set()
        return r

    monkeypatch.setattr(dbos._sys_db, "init_workflows", spy_iw)
    stored_offsets: list[int] = []

    class PoisonConsumer:
        def __init__(self, conf: dict[str, Any]) -> None:
            self._sent = False

        def subscribe(self, topics: Any) -> None:
            pass

        def consume(self, num_messages: int, timeout: float = 1.0) -> Any:
            if not self._sent:
                self._sent = True
                return batch
            time.sleep(0.02)
            return []

        def store_offsets(self, message: Any = None) -> None:
            stored_offsets.append(message.offset())

        def close(self) -> None:
            pass

    monkeypatch.setattr(kmod, "Consumer", PoisonConsumer)
    t, stop = _start_loop(poison_wf, group, f"mock-q-{random.randrange(1_000_000_000)}")
    try:
        assert iw_done.wait(timeout=10), "init_workflows was never called"
        time.sleep(0.5)
        # Only the 4 non-poison messages were enqueued; the poison one (offset 2) is dropped.
        assert len(enqueued) == 4, f"expected 4 enqueued, got {enqueued}"
        assert f"kafka-unique-id-mt-0-{group}-2" not in enqueued
        # The offset advanced past the whole batch (highest offset 4), so the poison
        # message won't be redelivered forever.
        assert stored_offsets and max(stored_offsets) == 4
    finally:
        stop.set()
        t.join(timeout=10)


# ---------------------------------------------------------------------------
# Custom queue, end-to-end (requires a broker)
# ---------------------------------------------------------------------------


def test_kafka_custom_queue(dbos: DBOS) -> None:
    # ordering="none" with a custom queue: messages run on that queue and its concurrency limit is honored.
    from confluent_kafka.admin import AdminClient, NewTopic

    server = "localhost:9092"
    topic = f"dbos-kafka-customq-{random.randrange(1_000_000_000)}"
    admin = AdminClient({"bootstrap.servers": server})
    try:
        admin.create_topics([NewTopic(topic, num_partitions=1)])[topic].result()
    except Exception:
        pytest.skip("Kafka not available")

    num_messages = 6
    producer = Producer({"bootstrap.servers": server})
    for i in range(num_messages):
        producer.produce(topic, partition=0, value=f"{i}")
    if producer.flush(10) > 0:
        pytest.skip("Kafka not available")

    queue_name = f"kafka-custom-q-{random.randrange(1_000_000_000)}"
    DBOS.register_queue(queue_name, concurrency=1)
    lock = threading.Lock()
    processed: set[int] = set()
    active = 0
    max_active = 0
    event = threading.Event()

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": f"customq-grp-{random.randrange(1_000_000_000)}",
            "auto.offset.reset": "earliest",
        },
        [topic],
        queue_name=queue_name,
    )
    @DBOS.workflow()
    def customq_wf(msg: KafkaMessage) -> None:
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        time.sleep(0.2)
        with lock:
            active -= 1
            assert msg.value is not None
            processed.add(int(msg.value.decode()))  # type: ignore[union-attr]
            if len(processed) == num_messages:
                event.set()

    # The consumer's custom queue is force-polled even under a listen_queues filter.
    assert queue_name in dbos._registry.poller_queue_names
    assert event.wait(timeout=60)
    assert processed == set(range(num_messages))
    assert max_active == 1  # concurrency=1 honored
