import logging
import os
import subprocess
import time

import psycopg

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
from typing import Any, Dict, Optional, Tuple


def start_docker_pg() -> None:
    """
    Starts a PostgreSQL database in a Docker container.

    This function checks if Docker is installed, and if so, starts a local PostgreSQL
    database in a Docker container. It configures the database with default settings
    and provides connection information upon successful startup.

    The function uses environment variable PGPASSWORD if available, otherwise
    defaults to 'dbos' as the database password.

    Returns:
        None

    Raises:
        Exception: If there is an error starting the Docker container or if the
                   PostgreSQL service does not become available within the timeout period.
    """

    logging.info("Attempting to create a Docker Postgres container...")
    has_docker = check_docker_installed()

    pool_config = {
        "host": "localhost",
        "port": 5432,
        "password": os.environ.get("PGPASSWORD", "dbos"),
        "user": "postgres",
        "database": "postgres",
        "connect_timeout": 2,
    }

    # If Docker is installed, start a local Docker based Postgres
    if has_docker:
        start_docker_postgres(pool_config)
        logging.info(
            f"Postgres available at postgres://postgres:{pool_config['password']}@{pool_config['host']}:{pool_config['port']}"
        )
    else:
        logging.warning("Docker not detected locally")


def check_db_connectivity(config: Dict[str, Any]) -> Optional[Exception]:
    conn = None
    try:
        conn = psycopg.connect(
            host=config["host"],
            port=config["port"],
            user=config["user"],
            password=config["password"],
            dbname=config["database"],
            connect_timeout=config.get("connect_timeout", 30),
        )
        cursor = conn.cursor()
        cursor.execute("SELECT 1;")
        cursor.close()
        return None
    except Exception as error:
        return error
    finally:
        if conn is not None:
            conn.close()


def exec_sync(cmd: str) -> Tuple[str, str]:
    result = subprocess.run(cmd, shell=True, text=True, capture_output=True, check=True)
    return result.stdout, result.stderr


def start_docker_postgres(pool_config: Dict[str, Any]) -> bool:
    logging.info("Starting a Postgres Docker container...")
    container_name = "dbos-db"
    pg_data = "/var/lib/postgresql/data"

    # Create and start the container
    docker_cmd = f"""docker run -d \
            --name {container_name} \
            -e POSTGRES_PASSWORD={pool_config['password']} \
            -e PGDATA={pg_data} \
            -p {pool_config['port']}:5432 \
            -v {pg_data}:{pg_data}:rw \
            --rm \
            pgvector/pgvector:pg16"""

    try:
        exec_sync(docker_cmd)
    except subprocess.CalledProcessError as e:
        # Check if this is a container name conflict error
        if e.returncode == 125 and "Conflict. The container name" in e.stderr:
            # Container already exists
            logging.info(f"Container '{container_name}' is already running.")
            return True
        else:
            # Other docker error - re-raise with clearer message
            raise Exception(f"Docker error: {e.stderr.strip()}") from e

    # Wait for PostgreSQL to be ready
    attempts = 30
    while attempts > 0:
        if attempts % 5 == 0:
            logging.info("Waiting for Postgres Docker container to start...")

        if check_db_connectivity(pool_config) is None:
            return True

        attempts -= 1
        time.sleep(1)

    raise Exception(
        f"Failed to start Docker container: Container {container_name} did not start in time."
    )


def check_docker_installed() -> bool:
    try:
        exec_sync("docker --version")
        return True
    except Exception:
        return False


def stop_docker_pg() -> None:
    """
    Stops the Docker Postgres container.

    Returns:
        bool: True if the container was successfully stopped, False if it wasn't running

    Raises:
        Exception: If there was an error stopping the container
    """
    logger = logging.getLogger()
    container_name = "dbos-db"

    try:
        logger.info(f"Stopping Docker Postgres container {container_name}...")

        # Check if container exists and is running
        try:
            stdout, _ = exec_sync(
                f'docker ps -a -f name={container_name} --format "{{{{.Status}}}}"'
            )
            container_status = stdout.strip()
        except subprocess.CalledProcessError:
            # If the command fails, assume container doesn't exist
            container_status = ""

        if not container_status:
            logger.info(f"Container {container_name} does not exist.")
            return

        is_running = "up" in container_status.lower()
        if not is_running:
            logger.info(f"Container {container_name} exists but is not running.")
            return

        # Stop the container
        exec_sync(f"docker stop {container_name}")
        logger.info(f"Successfully stopped Docker Postgres container {container_name}.")
        return

    except Exception as error:
        error_message = str(error)
        logger.error(f"Failed to stop Docker Postgres container: {error_message}")
        raise
