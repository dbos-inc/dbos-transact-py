import json
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
            f"Postgres available at postgresql://postgres:{pool_config['password']}@{pool_config['host']}:{pool_config['port']}"
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
    image_name = "pgvector/pgvector:pg16"

    try:
        # Check if the container already exists
        try:
            result = subprocess.run(
                f"docker inspect {container_name}",
                shell=True,
                text=True,
                capture_output=True,
            )

            if result.returncode == 0:
                # Container exists, check its status
                container_info = json.loads(result.stdout)
                status = container_info[0]["State"]["Status"]

                if status == "running":
                    logging.info(f"Container '{container_name}' is already running.")
                    return True
                elif status == "exited":
                    subprocess.run(
                        f"docker start {container_name}", shell=True, check=True
                    )
                    logging.info(
                        f"Container '{container_name}' was stopped and has been restarted."
                    )
                    return True
        except (
            subprocess.CalledProcessError,
            json.JSONDecodeError,
            KeyError,
            IndexError,
        ):
            # Container doesn't exist or error parsing, proceed with creation
            pass

        # Check if the image exists locally
        result = subprocess.run(
            f"docker images -q {image_name}", shell=True, text=True, capture_output=True
        )

        if not result.stdout.strip():
            logging.info(f"Pulling Docker image {image_name}...")
            subprocess.run(f"docker pull {image_name}", shell=True, check=True)

        # Create and start the container
        cmd = [
            "docker run",
            "-d",
            f"--name {container_name}",
            f"-e POSTGRES_PASSWORD={pool_config['password']}",
            f"-e PGDATA={pg_data}",
            f"-p {pool_config['port']}:5432",
            f"-v {pg_data}:{pg_data}",
            "--rm",
            image_name,
        ]

        result = subprocess.run(
            " ".join(cmd), shell=True, text=True, capture_output=True, check=True
        )

        container_id = result.stdout.strip()
        logging.info(f"Created container: {container_id}")

    except subprocess.CalledProcessError as e:
        raise Exception(f"Docker command error: {e.stderr if e.stderr else str(e)}")

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
    """
    Check if Docker is installed and running using the Docker CLI.

    Returns:
        bool: True if Docker is installed and running, False otherwise.
    """
    try:
        result = subprocess.run(
            "docker version --format json", shell=True, capture_output=True, text=True
        )
        return result.returncode == 0
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

        # Check if container exists
        result = subprocess.run(
            f"docker inspect {container_name}",
            shell=True,
            text=True,
            capture_output=True,
        )

        if result.returncode == 0:
            # Container exists, check its status
            try:
                container_info = json.loads(result.stdout)
                status = container_info[0]["State"]["Status"]

                if status == "running":
                    subprocess.run(
                        f"docker stop {container_name}", shell=True, check=True
                    )
                    logger.info(
                        f"Successfully stopped Docker Postgres container {container_name}."
                    )
                else:
                    logger.info(
                        f"Container {container_name} exists but is not running."
                    )
            except (json.JSONDecodeError, KeyError, IndexError) as e:
                logger.error(f"Error parsing container info: {e}")
                raise
        else:
            logger.info(f"Container {container_name} does not exist.")

    except subprocess.CalledProcessError as error:
        error_message = error.stderr if error.stderr else str(error)
        logger.error(f"Failed to stop Docker Postgres container: {error_message}")
        raise
    except Exception as error:
        error_message = str(error)
        logger.error(f"Failed to stop Docker Postgres container: {error_message}")
        raise
