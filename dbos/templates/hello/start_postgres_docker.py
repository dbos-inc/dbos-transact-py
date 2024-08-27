import os
import subprocess
import sys
import time

# Default PostgreSQL port
port = "5432"

# Set the host PostgreSQL port with the -p/--port flag
for i, arg in enumerate(sys.argv):
    if arg in ["-p", "--port"]:
        if i + 1 < len(sys.argv):
            port = sys.argv[i + 1]

if "PGPASSWORD" not in os.environ:
    print("Error: PGPASSWORD is not set.")
    sys.exit(1)

try:
    subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--name=dbos-db",
            f'--env=POSTGRES_PASSWORD={os.environ["PGPASSWORD"]}',
            "--env=PGDATA=/var/lib/postgresql/data",
            "--volume=/var/lib/postgresql/data",
            "-p",
            f"{port}:5432",
            "-d",
            "pgvector/pgvector:pg16",
        ],
        check=True,
    )

    print("Waiting for PostgreSQL to start...")
    attempts = 30

    while attempts > 0:
        try:
            subprocess.run(
                [
                    "docker",
                    "exec",
                    "dbos-db",
                    "psql",
                    "-U",
                    "postgres",
                    "-c",
                    "SELECT 1;",
                ],
                check=True,
                capture_output=True,
            )
            print("PostgreSQL started!")
            print("Database started successfully!")
            break
        except subprocess.CalledProcessError:
            attempts -= 1
            time.sleep(1)

    if attempts == 0:
        print("Failed to start PostgreSQL.")

except subprocess.CalledProcessError as error:
    print(f"Error starting PostgreSQL in Docker: {error}")
