from dbos import DBOS
from dbos._dbos_config import DBOSConfig


@DBOS.workflow()
def my_function(foo: str) -> str:
    return foo


def default_config() -> DBOSConfig:
    return {
        "name": "forgot-launch",
        "database_url": f"postgresql://postgres:doesntmatter@localhost:5432/notneeded",
    }


DBOS(config=default_config())
