import random
import threading
import time
from typing import Any, NoReturn, Optional

import pytest
from confluent_kafka import Consumer, KafkaError, Producer

from dbos import DBOS, DBOSConfig, KafkaMessage, Queue

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

    with pytest.warns(DeprecationWarning):

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

    # Reference the database-backed queue by name; this reference is NOT added to the
    # in-memory registry, so the poller can only reach it by resolving it from the DB.
    consumer_queue = Queue(queue_name, database_backed_queue=True)

    @DBOS.kafka_consumer(
        {
            "bootstrap.servers": server,
            "group.id": "dbos-test-dbq-listen",
            "auto.offset.reset": "earliest",
        },
        [topic],
        queue=consumer_queue,
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


def test_kafka_throughput(dbos: DBOS, monkeypatch: pytest.MonkeyPatch) -> None:
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
