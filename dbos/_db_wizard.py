import time
from typing import TYPE_CHECKING

import docker  # type: ignore
import yaml
from sqlalchemy import URL, create_engine, text

if TYPE_CHECKING:
    from ._dbos_config import ConfigFile

from ._cloudutils.cloudutils import get_cloud_credentials
from ._cloudutils.databases import choose_database
from ._error import DBOSInitializationError
from ._logger import dbos_logger


def db_connect(config: "ConfigFile", config_file_path: str) -> "ConfigFile":
    # 1. Check the connectivity to the database. Return if successful. If cannot connect, continue to the following steps.
    db_connected = _check_db_connectivity(config)
    if db_connected:
        dbos_logger.debug("Database is connected")
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
    has_docker = _check_docker_installed()

    # 4. If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.
    docker_started = False
    if has_docker:
        dbos_logger.debug("Docker is installed")
        docker_started = _start_docker_postgres(config)

    # 5. If no Docker, then prompt the user to log in to DBOS Cloud and provision a DB there. Wait for the remote DB to be ready, and then create a copy of the original config file, and then load the remote connection string to the local config file.
    if not docker_started:
        dbos_logger.info("Connecting to DBOS Cloud")
        cred = get_cloud_credentials()
        db = choose_database(cred)
        if db is None:
            raise DBOSInitializationError("Error connecting to cloud database")
        config["database"]["hostname"] = db.HostName
        config["database"]["port"] = db.Port
        config["database"]["username"] = db.DatabaseUsername
        password = input("Enter password: ")
        config["database"]["password"] = password

    # 6. Save the config to the config file and return the updated config.
    # TODO: make the config file prettier
    with open(config_file_path, "w") as file:
        file.write(yaml.dump(config))

    return config


def _start_docker_postgres(config: "ConfigFile") -> bool:
    dbos_logger.info("Starting a Postgres Docker container")
    config["database"]["password"] = "dbos"
    client = docker.from_env()
    pg_data = "/var/lib/postgresql/data"
    container_name = "dbos-db"
    client.containers.run(
        image="pgvector/pgvector:pg16",
        detach=True,
        environment={
            "POSTGRES_PASSWORD": config["database"]["password"],
            "PGDATA": pg_data,
        },
        volumes={pg_data: {"bind": pg_data, "mode": "rw"}},
        ports={"5432/tcp": config["database"]["port"]},
        name=container_name,
        remove=True,
    )

    container = client.containers.get(container_name)
    attempts = 30
    while attempts > 0:
        if attempts % 5 == 0:
            dbos_logger.info("Waiting for Postgres to start...")
        try:
            res = container.exec_run("psql -U postgres -c 'SELECT 1;'")
            if res.exit_code != 0:
                attempts -= 1
                time.sleep(1)
                continue
            dbos_logger.info("Postgres Docker started successfully!")
            break
        except Exception as e:
            attempts -= 1
            time.sleep(1)

    if attempts == 0:
        dbos_logger.warning("Failed to start Postgres Docker container.")
        return False

    return True


def _check_docker_installed() -> bool:
    # Check if Docker is installed
    try:
        client = docker.from_env()
        client.ping()
    except Exception as e:
        dbos_logger.error(f"Could not connect to Docker: {e}")
        return False
    return True


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
        # TODO: check if the error is pg password related. If so, prompt the user to set the PGPASSWORD env variable.
        dbos_logger.error(f"Could not connect to the database: {e}")
        return False
    finally:
        postgres_db_engine.dispose()

    return True
