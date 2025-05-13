from dbos import DBOS

def test_workflow(dbos: DBOS) -> None:

    def step_one(x: int) -> int:
        return x + 1

    def step_two(x: int) -> int:
        return x + 2

    @DBOS.workflow()
    def workflow(x: int) -> int:
        DBOS.sleep(1)
        x = step_one(x)
        x = step_two(x)
        return x

    num_workflows = 5

    for i in range(num_workflows):
        assert workflow(i) == i + 3
