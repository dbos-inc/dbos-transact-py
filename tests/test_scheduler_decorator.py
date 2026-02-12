import time
from datetime import datetime

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

# Public API
from dbos import DBOS
from dbos._error import DBOSWorkflowFunctionNotFoundError

from .conftest import retry_until_success


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

    def check_fired() -> None:
        assert wf_counter >= 2

    retry_until_success(check_fired)


def test_async_scheduled_workflow(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    async def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    def check_fired() -> None:
        assert wf_counter >= 2

    retry_until_success(check_fired)


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

    def check_fired_before() -> None:
        assert wf_counter >= 1

    retry_until_success(check_fired_before)

    assert dbos._app_db
    simulate_db_restart(dbos._app_db.engine, 2)

    count_before_restart = wf_counter

    def check_fired_after_restart() -> None:
        assert wf_counter > count_before_restart

    retry_until_success(check_fired_after_restart)


def test_sysdb_downtime(dbos: DBOS, skip_with_sqlite: None) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1

    def check_fired_before() -> None:
        assert wf_counter >= 1

    retry_until_success(check_fired_before)

    simulate_db_restart(dbos._sys_db.engine, 2)

    count_before_restart = wf_counter

    def check_fired_after_restart() -> None:
        assert wf_counter > count_before_restart

    retry_until_success(check_fired_after_restart)


def test_scheduled_transaction(dbos: DBOS) -> None:
    txn_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.transaction()
    def test_transaction(scheduled: datetime, actual: datetime) -> None:
        nonlocal txn_counter
        txn_counter += 1

    def check_fired() -> None:
        assert txn_counter >= 2

    retry_until_success(check_fired)


def test_scheduled_step(dbos: DBOS) -> None:
    step_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.step()
    def test_step(scheduled: datetime, actual: datetime) -> None:
        nonlocal step_counter
        step_counter += 1

    def check_fired() -> None:
        assert step_counter >= 2

    retry_until_success(check_fired)


def test_scheduled_workflow_exception(dbos: DBOS) -> None:
    wf_counter: int = 0

    @DBOS.scheduled("* * * * * *")
    @DBOS.workflow()
    def test_failing_workflow(scheduled: datetime, actual: datetime) -> None:
        nonlocal wf_counter
        wf_counter += 1
        raise Exception("error")

    def check_fired() -> None:
        assert wf_counter >= 2

    retry_until_success(check_fired)


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

    def check_fired() -> None:
        assert wf_counter >= 1

    retry_until_success(check_fired)

    def check_txn_matches_wf() -> None:
        assert txn_counter == wf_counter

    retry_until_success(check_txn_matches_wf)

    # Stop the scheduled workflow
    for evt in dbos.poller_stop_events:
        evt.set()

    # Wait for workflows to finish
    time.sleep(3)

    dbos._sys_db.update_workflow_outcome(workflow_id, "PENDING")

    workflow_handles = DBOS._recover_pending_workflows()
    assert len(workflow_handles) == 1
    assert workflow_handles[0].get_result() == None

    def check_recovery_oaoo() -> None:
        assert wf_counter == txn_counter + 1

    retry_until_success(check_recovery_oaoo)


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
