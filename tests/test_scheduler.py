import time
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Public API
from dbos import DBOS
from dbos._error import DBOSWorkflowFunctionNotFoundError


def simulate_db_restart(engine: Engine, downtime: float) -> None:
    # Get DB name
    with engine.connect() as connection:
        current_db = connection.execute(text("SELECT current_database()")).scalar()

    # Goal here is to disable connections to the DB for a while.
    #   Need a temp DB to do that and recover connectivity...
    temp_db_name = "temp_database_for_maintenance"

    # Retrieve the URL of the current engine
    main_db_url = engine.url

    # Modify the URL to point to the temporary database
    temp_db_url = main_db_url.set(database=temp_db_name)

    try:
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            # Create a temporary database
            connection.execute(text(f"CREATE DATABASE {temp_db_name};"))
            print(f"Temporary database '{temp_db_name}' created.")
    except Exception as e:
        print("Could not create temp db: ", e)

    temp_engine = create_engine(temp_db_url)
    with temp_engine.connect().execution_options(
        isolation_level="AUTOCOMMIT"
    ) as temp_connection:
        try:
            # Disable new connections to the database
            temp_connection.execute(
                text(f"ALTER DATABASE {current_db} WITH ALLOW_CONNECTIONS false;")
            )

            # Terminate all connections except the current one
            temp_connection.execute(
                text(
                    f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                AND datname = '{current_db}';
            """
                )
            )
        except Exception as e:
            print(f"Could not disable db {current_db}: ", e)

        time.sleep(downtime)

        # Re-enable new connections
        try:
            temp_connection.execute(
                text(f"ALTER DATABASE {current_db} WITH ALLOW_CONNECTIONS true;")
            )
        except Exception as e:
            print(f"Could not reenable db {current_db}: ", e)
    temp_engine.dispose()

    try:
        with engine.connect().execution_options(
            isolation_level="AUTOCOMMIT"
        ) as connection:
            # Clean up the temp DB
            connection.execute(
                text(
                    f"""
                SELECT pg_terminate_backend(pid)
                FROM pg_stat_activity
                WHERE pid <> pg_backend_pid()
                AND datname = '{temp_db_name}';
            """
                )
            )

            # Drop temporary database
            connection.execute(text(f"DROP DATABASE {temp_db_name};"))
    except Exception as e:
        print(f"Could not clean up temp db {temp_db_name}: ", e)


def test_scheduled_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    time.sleep(5)
    assert wf_counter > 2 and wf_counter <= 5


def test_appdb_downtime(dbos: DBOS, skip_with_sqlite: None) -> None:
    wf_counter: int = 0

    @DBOS.transaction()
    def test_transaction(var2: str) -> str:
        rows = DBOS.sql_session.execute(text("SELECT 1")).fetchall()
        return "ran"

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        test_transaction("x")
        wf_counter += 1

    time.sleep(2)
    simulate_db_restart(dbos._app_db.engine, 2)
    time.sleep(2)
    assert wf_counter > 2


def test_sysdb_downtime(dbos: DBOS, skip_with_sqlite: None) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    time.sleep(2)
    simulate_db_restart(dbos._sys_db.engine, 2)
    time.sleep(2)
    # We know there should be at least 2 occurrences from the 4 seconds when the DB was up.
    #  There could be more than 4, depending on the pace the machine...
    assert wf_counter >= 2


def test_scheduled_transaction(dbos: DBOS) -> None:
    txn_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.transaction()
    def test_transaction(scheduled: datetime, actual: datetime) -> None:
        nonlocal txn_counter
        txn_counter += 1

    time.sleep(5)
    assert txn_counter > 2 and txn_counter <= 5


def test_scheduled_step(dbos: DBOS) -> None:
    step_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.step()
    def test_step(scheduled: datetime, actual: datetime) -> None:
        nonlocal step_counter
        step_counter += 1

    time.sleep(5)
    assert step_counter > 2 and step_counter <= 5


def test_scheduled_workflow_exception(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_failing_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1
        raise Exception("error")

    time.sleep(4)
    assert wf_counter >= 1 and wf_counter <= 4


def test_scheduler_oaoo(dbos: DBOS) -> None:
    wf_counter: int = 0
    txn_counter: int = 0
    workflow_id: str = ""

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        test_transaction()
        nonlocal wf_counter
        wf_counter += 1
        nonlocal workflow_id
        wf_id = DBOS.workflow_id
        assert wf_id is not None
        workflow_id = wf_id

    @DBOS.transaction()
    def test_transaction() -> None:
        nonlocal txn_counter
        txn_counter += 1

    time.sleep(3)
    assert wf_counter >= 1 and wf_counter <= 3
    max_tries = 10
    for i in range(max_tries):
        try:
            assert txn_counter == wf_counter
            break
        except Exception as e:
            if i == max_tries - 1:
                raise e
            else:
                time.sleep(1)

    # Stop the scheduled workflow
    for evt in dbos.poller_stop_events:
        evt.set()

    # Wait for workflows to finish
    time.sleep(2)

    dbos._sys_db.update_workflow_outcome(workflow_id, "PENDING")

    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == None
    max_tries = 10
    for i in range(max_tries):
        try:
            assert txn_counter + 1 == wf_counter
            break
        except Exception as e:
            if i == max_tries - 1:
                raise e
            else:
                time.sleep(1)


def test_long_workflow(dbos: DBOS) -> None:
    """
    This runs every hour and does nothing. Goal is to verify that it shuts down properly.
    """

    @DBOS.scheduled("0 * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        pass


def test_bad_schedule(dbos: DBOS) -> None:
    code = """
@DBOS.scheduled("*/invalid * * * * *")  # this is not a valid schedule
def my_function():
    pass
"""
    # Use exec to run the code and catch the expected exception
    with pytest.raises(DBOSWorkflowFunctionNotFoundError) as excinfo:
        exec(code)
