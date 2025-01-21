import json
import os
import time
from typing import TYPE_CHECKING, Optional, TypedDict

import docker  # type: ignore
import typer
import yaml
from rich import print
from sqlalchemy import URL, create_engine, text

if TYPE_CHECKING:
    from ._dbos_config import ConfigFile

from ._cloudutils.cloudutils import get_cloud_credentials
from ._cloudutils.databases import choose_database, get_user_db_credentials
from ._error import DBOSInitializationError
from ._logger import dbos_logger

DB_CONNECTION_PATH = os.path.join(".dbos", "db_connection")


class DatabaseConnection(TypedDict):
    hostname: Optional[str]
    port: Optional[int]
    username: Optional[str]
    password: Optional[str]
    local_suffix: Optional[bool]


def db_wizard(config: "ConfigFile", config_file_path: str) -> "ConfigFile":
    # 1. Check the connectivity to the database. Return if successful. If cannot connect, continue to the following steps.
    db_connection_error = _check_db_connectivity(config)
    if db_connection_error is None:
        return config

    # 2. If the error is due to password authentication or the configuration is non-default, surface the error and exit.
    error_str = str(db_connection_error)
    if (
        "password authentication failed" in error_str
        or "28P01" in error_str
        or "no password supplied" in error_str
    ):
        raise DBOSInitializationError(
            f"Could not connect to Postgres: password authentication failed: {db_connection_error}"
        )
    db_config = config["database"]
    if (
        db_config["hostname"] != "localhost"
        or db_config["port"] != 5432
        or db_config["username"] != "postgres"
    ):
        raise DBOSInitializationError(
            f"Could not connect to the database. Exception: {db_connection_error}"
        )
    print("[yellow]Postgres not detected locally[/yellow]")

    # 3. If the database config is the default one, check if the user has Docker properly installed.
    print("Attempting to start Postgres via Docker")
    has_docker = _check_docker_installed()

    # 4. If Docker is installed, prompt the user to start a local Docker based Postgres, and then set the PGPASSWORD to 'dbos' and try to connect to the database.
    docker_started = False
    if has_docker:
        docker_started = _start_docker_postgres(config)
    else:
        print("[yellow]Docker not detected locally[/yellow]")

    # 5. If no Docker, then prompt the user to log in to DBOS Cloud and provision a DB there. Wait for the remote DB to be ready, and then create a copy of the original config file, and then load the remote connection string to the local config file.
    if not docker_started:
        print("Attempting to connect to Postgres via DBOS Cloud")
        cred = get_cloud_credentials()
        db = choose_database(cred)
        if db is None:
            raise DBOSInitializationError("Error connecting to cloud database")
        config["database"]["hostname"] = db.HostName
        config["database"]["port"] = db.Port
        if db.SupabaseReference is not None:
            config["database"]["username"] = f"postgres.{db.SupabaseReference}"
            supabase_password = typer.prompt(
                "Enter your Supabase database password", hide_input=True
            )
            config["database"]["password"] = supabase_password
        else:
            config["database"]["username"] = db.DatabaseUsername
            db_credentials = get_user_db_credentials(cred, db.PostgresInstanceName)
            config["database"]["password"] = db_credentials.Password
        config["database"]["local_suffix"] = True

        # Verify these new credentials work
        db_connection_error = _check_db_connectivity(config)
        if db_connection_error is not None:
            raise DBOSInitializationError(
                f"Could not connect to the database. Exception: {db_connection_error}"
            )

    # 6. Save the config to the database connection file
    updated_connection = DatabaseConnection(
        hostname=config["database"]["hostname"],
        port=config["database"]["port"],
        username=config["database"]["username"],
        password=config["database"]["password"],
        local_suffix=config["database"]["local_suffix"],
    )
    save_db_connection(updated_connection)
    return config


def _start_docker_postgres(config: "ConfigFile") -> bool:
    print("Starting a Postgres Docker container...")
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
            print("Waiting for Postgres Docker container to start...")
        try:
            res = container.exec_run("psql -U postgres -c 'SELECT 1;'")
            if res.exit_code != 0:
                attempts -= 1
                time.sleep(1)
                continue
            print("[green]Postgres Docker container started successfully![/green]")
            break
        except:
            attempts -= 1
            time.sleep(1)

    if attempts == 0:
        print("[yellow]Failed to start Postgres Docker container.[/yellow]")
        return False

    return True


def _check_docker_installed() -> bool:
    # Check if Docker is installed
    try:
        client = docker.from_env()
        client.ping()
    except Exception:
        return False
    return True


def _check_db_connectivity(config: "ConfigFile") -> Optional[Exception]:
    postgres_db_url = URL.create(
        "postgresql+psycopg",
        username=config["database"]["username"],
        password=config["database"]["password"],
        host=config["database"]["hostname"],
        port=config["database"]["port"],
        database="postgres",
        query={"connect_timeout": "1"},
    )
    postgres_db_engine = create_engine(postgres_db_url)
    try:
        with postgres_db_engine.connect() as conn:
            val = conn.execute(text("SELECT 1")).scalar()
            if val != 1:
                dbos_logger.error(
                    f"Unexpected value returned from database: expected 1, received {val}"
                )
                return Exception()
    except Exception as e:
        return e
    finally:
        postgres_db_engine.dispose()

    return None


def load_db_connection() -> DatabaseConnection:
    try:
        with open(DB_CONNECTION_PATH, "r") as f:
            data = json.load(f)
            return DatabaseConnection(
                hostname=data.get("hostname", None),
                port=data.get("port", None),
                username=data.get("username", None),
                password=data.get("password", None),
                local_suffix=data.get("local_suffix", None),
            )
    except:
        return DatabaseConnection(
            hostname=None, port=None, username=None, password=None, local_suffix=None
        )


def save_db_connection(connection: DatabaseConnection) -> None:
    os.makedirs(".dbos", exist_ok=True)
    with open(DB_CONNECTION_PATH, "w") as f:
        json.dump(connection, f)
