import sqlalchemy as sa

from dbos import DBOS


def test_workflow(dbos: DBOS) -> None:

    @DBOS.step()
    def step_one(x: int) -> int:
        return x + 1

    @DBOS.step()
    def step_two(x: int) -> int:
        return x + 2

    @DBOS.transaction()
    def txn_one(x: int) -> int:
        DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return x + 3

    @DBOS.workflow()
    def workflow(x: int) -> int:
        x = step_one(x)
        x = step_two(x)
        x = txn_one(x)
        return x

    num_workflows = 5000

    for i in range(num_workflows):
        assert workflow(i) == i + 6
