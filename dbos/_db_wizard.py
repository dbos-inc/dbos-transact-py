from typing import TYPE_CHECKING

from sqlalchemy import URL, create_engine, text

if TYPE_CHECKING:
    from ._dbos_config import ConfigFile

from ._error import DBOSInitializationError
from ._logger import dbos_logger


def db_connect(config: "ConfigFile", config_file_path: str) -> "ConfigFile":
    # 1. Check the connectivity to the database. Return if successful. If cannot connect, continue to the following steps.
    db_connected = _check_db_connectivity(config)
    if db_connected:
        dbos_logger.info("Database is connected")
        return config

    # 2. Check if the database config is the default one. If not, surface a connection error and exit the process.
    db_config = config["database"]
    if (
        db_config["hostname"] != "localhost"
        or db_config["port"] != 5432
        or db_config["username"] != "postgres"
    ):
        raise DBOSInitializationError(
            "Could not connect to the database. Please check the database configuration."
        )

    # 3. If the database config is the default one, check if the user has Docker properly installed.

    # 4. If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.

    # 5. If no Docker, then prompt the user to log in to DBOS Cloud and provision a DB there. Wait for the remote DB to be ready, and then create a copy of the original config file, and then load the remote connection string to the local config file.

    # 6. Save the config to the config file and return the updated config.
    return config


def _check_db_connectivity(config: "ConfigFile") -> bool:
    postgres_db_url = URL.create(
        "postgresql+psycopg",
        username=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["hostname"],
        port=config["database"]["port"],
        database="postgres",
    )
    postgres_db_engine = create_engine(postgres_db_url)
    try:
        with postgres_db_engine.connect() as conn:
            val = conn.execute(text("SELECT 1")).scalar()
            if val != 1:
                dbos_logger.error(
                    f"Unexpected value returned from database: expected 1, received {val}"
                )
                return False
    except Exception as e:
        dbos_logger.error(f"Could not connect to the database: {e}")
        return False
    finally:
        postgres_db_engine.dispose()

    return True
