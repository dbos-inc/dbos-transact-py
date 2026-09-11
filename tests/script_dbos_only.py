import os

from dbos import DBOS, DBOSConfig

config: DBOSConfig = {
    "name": "dbos-starter",
    "system_database_url": os.environ.get("DBOS_SYSTEM_DATABASE_URL"),
}
DBOS(config=config)


@DBOS.step()
def step_one() -> None:
    print("Step one completed!")


@DBOS.step()
def step_two() -> None:
    print("Step two completed!")


@DBOS.workflow()
def dbos_workflow() -> None:
    step_one()
    step_two()


if __name__ == "__main__":
    DBOS.launch()
    dbos_workflow()
