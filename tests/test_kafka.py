from confluent_kafka import Message

from dbos import DBOS, SetWorkflowID


@DBOS.kafka_consumer(
    {
        "bootstrap.servers": "localhost:9092",
        "group.id": "dbos-test",
    },
    ["dbos-test-topic"],
)
@DBOS.workflow()
def foo(msg: Message):
    DBOS.logger.info(f"Received message: {msg.key()} {msg.value()}")
