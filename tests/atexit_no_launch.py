from dbos import DBOS
from dbos._dbos_config import ConfigFile


@DBOS.workflow()
def my_function(foo: str) -> str:
    return foo


def default_config() -> ConfigFile:
    return {
        "name": "forgot-launch",
        "language": "python",
        "database": {
            "hostname": "localhost",
            "port": 5432,
            "username": "postgres",
            "password": "doesntmatter",
            "app_db_name": "doesntmatter",
        },
        "runtimeConfig": {
            "start": ["doesntmatter"],
        },
        "telemetry": {},
        "env": {},
    }


DBOS(config=default_config())
