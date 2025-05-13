from dbos import DBOS


def test_workflow(dbos: DBOS):

    def step_one(x: int) -> int:
        return x + 1

    def step_two(x: int) -> int:
        return x + 2

    @DBOS.workflow()
    def workflow(x: int) -> int:
        x = step_one(x)
        x = step_two(x)
        return x

    input = 5
    output = input + 3

    assert workflow(input) == output
