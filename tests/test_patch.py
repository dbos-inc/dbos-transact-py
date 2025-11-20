# mypy: disable-error-code="no-redef"
from dbos import DBOS, DBOSConfig


def test_patch(dbos: DBOS, config: DBOSConfig) -> None:

    @DBOS.step()
    def step_one() -> int:
        return 1

    @DBOS.step()
    def step_two() -> int:
        return 2

    @DBOS.step()
    def step_three() -> int:
        return 3

    @DBOS.workflow()
    def workflow() -> int:
        a = step_one()
        b = step_two()
        return a + b

    handle = DBOS.start_workflow(workflow)
    v1_id = handle.workflow_id
    assert handle.get_result() == 3

    DBOS.destroy(destroy_registry=True)
    DBOS(config=config)

    step_one = DBOS.step()(step_one)
    step_two = DBOS.step()(step_two)
    step_three = DBOS.step()(step_three)

    @DBOS.workflow()
    def workflow() -> int:
        if DBOS.patch():
            a = step_three()
        else:
            a = step_one()
        b = step_two()
        return a + b

    DBOS.launch()

    handle = DBOS.start_workflow(workflow)
    assert handle.get_result() == 5

    handle = DBOS.fork_workflow(v1_id, 2)
    assert handle.get_result() == 3
