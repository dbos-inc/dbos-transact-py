import os
import shutil
import subprocess
import tempfile
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytest
import sqlalchemy as sa

# Public API
from dbos import DBOS, DBOSConfig, run_dbos_database_migrations
from dbos._migration import get_dbos_migrations, should_migrate, sqlite_migrations

# Private API because this is a unit test
from dbos._schemas.system_database import SystemSchema
from dbos._serialization import DefaultSerializer
from dbos._sys_db import SystemDatabase
from dbos.cli.migration import print_dbos_database_migrations


def test_systemdb_migration(dbos: DBOS, skip_with_sqlite: None) -> None:
    # Make sure all tables exist
    with dbos._sys_db.engine.connect() as connection:
        sql = SystemSchema.workflow_status.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.operation_outputs.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.workflow_events.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        sql = SystemSchema.notifications.select()
        result = connection.execute(sql)
        assert result.fetchall() == []

        # Check dbos_migrations table exists, has one row, and has the right version
        migrations_result = connection.execute(
            sa.text("SELECT version FROM dbos.dbos_migrations")
        )
        migrations_rows = migrations_result.fetchall()
        assert len(migrations_rows) == 1
        assert migrations_rows[0][0] == len(get_dbos_migrations("dbos", True))


def test_systemdb_migration_custom_schema(
    config: DBOSConfig,
    skip_with_sqlite: None,
    cleanup_test_databases: None,
) -> None:

    config["application_database_url"] = None
    schema = "F8nny_sCHem@-n@m3"
    config["dbos_system_schema"] = schema
    DBOS.destroy(destroy_registry=True)
    dbos = DBOS(config=config)
    DBOS.launch()
    # Make sure all tables exist
    with dbos._sys_db.engine.connect() as connection:
        result = connection.execute(
            sa.text(f'SELECT * FROM "{schema}".workflow_status')
        )
        rows = result.fetchall()
        assert len(rows) == 0
        # Check dbos_migrations table exists, has one row, and has the right version
        result = connection.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
        )
        rows = result.fetchall()
        assert len(rows) == 1
        assert rows[0][0] == len(get_dbos_migrations(schema, True))

        # Check that the 'dbos' schema does not exist
        result = connection.execute(
            sa.text(
                "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'dbos'"
            )
        )
        rows = result.fetchall()
        assert len(rows) == 0

    DBOS.destroy()


def test_two_schemas_isolated_in_one_process(
    config: DBOSConfig,
    skip_with_sqlite: None,
    cleanup_test_databases: None,
) -> None:
    """Two system databases with different schemas must stay isolated within one process."""
    sys_db_url = config["system_database_url"]
    assert sys_db_url is not None

    db_a = SystemDatabase.create(
        system_database_url=sys_db_url,
        engine_kwargs={},
        engine=None,
        schema="schema_alpha",
        executor_id=None,
        serializer=DefaultSerializer(),
        use_listen_notify=False,
    )
    db_b = SystemDatabase.create(
        system_database_url=sys_db_url,
        engine_kwargs={},
        engine=None,
        schema="schema_beta",
        executor_id=None,
        serializer=DefaultSerializer(),
        use_listen_notify=False,
    )
    try:
        db_a.run_migrations()
        db_b.run_migrations()

        # Write a row through each instance; it should land in that instance's schema.
        ins = SystemSchema.application_versions.insert()
        with db_a.engine.begin() as conn:
            conn.execute(
                ins.values(
                    version_id="alpha-row",
                    version_name="alpha-row",
                    version_timestamp=1,
                    created_at=1,
                )
            )
        with db_b.engine.begin() as conn:
            conn.execute(
                ins.values(
                    version_id="beta-row",
                    version_name="beta-row",
                    version_timestamp=1,
                    created_at=1,
                )
            )

        # Each instance's Core queries see only its own schema's row.
        sel = sa.select(SystemSchema.application_versions.c.version_id)
        with db_a.engine.begin() as conn:
            assert [r[0] for r in conn.execute(sel)] == ["alpha-row"]
        with db_b.engine.begin() as conn:
            assert [r[0] for r in conn.execute(sel)] == ["beta-row"]

        # And the rows physically live in the correct schemas.
        with db_a.engine.begin() as conn:
            assert (
                conn.execute(
                    sa.text("SELECT version_id FROM schema_alpha.application_versions")
                ).scalar_one()
                == "alpha-row"
            )
            assert (
                conn.execute(
                    sa.text("SELECT version_id FROM schema_beta.application_versions")
                ).scalar_one()
                == "beta-row"
            )
    finally:
        db_a.destroy()
        db_b.destroy()


def test_reset(
    config: DBOSConfig, db_engine: sa.Engine, skip_with_sqlite: None
) -> None:
    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.launch()

    # Make sure the system database exists
    with dbos._sys_db.engine.connect() as c:
        sql = SystemSchema.workflow_status.select()
        result = c.execute(sql)
        assert result.fetchall() == []
    sysdb_name = dbos._sys_db.engine.url.database

    DBOS.destroy()
    dbos = DBOS(config=config)
    DBOS.reset_system_database()

    with db_engine.connect() as c:
        c.execution_options(isolation_level="AUTOCOMMIT")
        count: int = c.execute(
            sa.text(f"SELECT COUNT(*) FROM pg_database WHERE datname = '{sysdb_name}'")
        ).scalar_one()
        assert count == 0

    DBOS.launch()

    # Make sure the system database is recreated
    with dbos._sys_db.engine.connect() as c:
        sql = SystemSchema.workflow_status.select()
        result = c.execute(sql)
        assert result.fetchall() == []

    # Verify that resetting after launch throws
    with pytest.raises(AssertionError):
        DBOS.reset_system_database()


def test_sqlite_systemdb_migration() -> None:
    """Test SQLite system database migration."""
    # Create a temporary SQLite database file
    with tempfile.NamedTemporaryFile(suffix=".sqlite", delete=False) as temp_db:
        temp_db_path = temp_db.name

        # Create SQLite system database URL
        sqlite_url = f"sqlite:///{temp_db_path}"

        # Create and run migrations
        sys_db = SystemDatabase.create(
            system_database_url=sqlite_url,
            engine_kwargs={},
            engine=None,
            schema=None,
            executor_id=None,
            serializer=DefaultSerializer(),
        )

        # Run migrations
        sys_db.run_migrations()

        # Verify all tables exist and are empty
        with sys_db.engine.connect() as connection:
            # Note: SQLite schema doesn't use "dbos." prefix

            # Test workflow_status table
            sql = sa.text("SELECT * FROM workflow_status")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test operation_outputs table
            sql = sa.text("SELECT * FROM operation_outputs")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test workflow_events table
            sql = sa.text("SELECT * FROM workflow_events")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test notifications table
            sql = sa.text("SELECT * FROM notifications")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Test streams table
            sql = sa.text("SELECT * FROM streams")
            result = connection.execute(sql)
            assert result.fetchall() == []

            # Check dbos_migrations table exists, has one row, and has the right version
            migrations_result = connection.execute(
                sa.text("SELECT version FROM dbos_migrations")
            )
            migrations_rows = migrations_result.fetchall()
            assert len(migrations_rows) == 1
            assert migrations_rows[0][0] == len(sqlite_migrations)

            # Verify foreign keys are enabled
            fk_result = connection.execute(sa.text("PRAGMA foreign_keys"))
            fk_enabled = fk_result.fetchone()
            assert fk_enabled and fk_enabled[0] == 1  # 1 means enabled

        # Clean up
        sys_db.destroy()

    # Test resetting the system database
    assert os.path.exists(temp_db_path)
    DBOS.destroy()
    DBOS(config={"name": "sqlite_test", "database_url": sqlite_url})
    DBOS.reset_system_database()
    assert not os.path.exists(temp_db_path)
    DBOS.destroy()


def test_migrate(db_engine: sa.Engine, skip_with_sqlite: None) -> None:
    """Test that you can migrate with a privileged role and run DBOS with a less-privileged role"""
    database_name = "migrate_test"
    role_name = "migrate-test-role"
    role_password = "migrate_test_password"

    # Verify migration is agnostic to driver name (under the hood it uses postgresql+psycopg)
    db_url = db_engine.url.set(database=database_name).set(drivername="postgresql")
    db_url_string = db_url.render_as_string(hide_password=False)

    # Test with different system schema names
    for schema in ["dbos", "public", "F8nny_sCHem@-n@m3"]:
        for use_app_db in [True, False]:
            # Drop the DBOS database if it exists. Create a test role with no permissions.
            with db_engine.connect() as connection:
                connection.execution_options(isolation_level="AUTOCOMMIT")
                connection.execute(
                    sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
                )
                connection.execute(sa.text(f'DROP ROLE IF EXISTS "{role_name}"'))
                connection.execute(
                    sa.text(
                        f"CREATE ROLE \"{role_name}\" WITH LOGIN PASSWORD '{role_password}'"
                    )
                )

            # Using the admin role, create the DBOS database and verify it exists.
            # Set permissions for the test role.
            if use_app_db:
                subprocess.check_call(
                    [
                        "dbos",
                        "migrate",
                        "-D",
                        db_url_string,
                        "-s",
                        db_url_string,
                        "-r",
                        role_name,
                        "--schema",
                        schema,
                    ]
                )
            else:
                subprocess.check_call(
                    [
                        "dbos",
                        "migrate",
                        "-s",
                        db_url_string,
                        "-r",
                        role_name,
                        "--schema",
                        schema,
                    ]
                )
            with db_engine.connect() as c:
                c.execution_options(isolation_level="AUTOCOMMIT")
                result = c.execute(
                    sa.text(
                        f"SELECT COUNT(*) FROM pg_database WHERE datname = '{database_name}'"
                    )
                ).scalar()
                assert result == 1

            # Initialize DBOS with the test role. Verify various operations work.
            test_db_url = (
                db_url.set(username=role_name).set(password=role_password)
            ).render_as_string(hide_password=False)
            DBOS.destroy(destroy_registry=True)
            config: DBOSConfig = {
                "name": "test_migrate",
                "database_url": test_db_url if use_app_db else None,
                "system_database_url": test_db_url,
                "dbos_system_schema": schema,
            }
            dbos = DBOS(config=config)
            if not use_app_db:
                assert dbos._app_db is None

            @DBOS.transaction()
            def test_transaction() -> str:
                rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
                return str(rows[0][0])

            @DBOS.step()
            def test_step() -> str:
                return "1"

            @DBOS.workflow()
            def test_workflow() -> str:
                if use_app_db:
                    assert test_transaction() == "1"
                else:
                    assert test_step() == "1"
                id = DBOS.workflow_id
                assert id
                DBOS.set_event(id, id)
                return id

            DBOS.launch()

            workflow_id = test_workflow()
            assert workflow_id
            assert DBOS.get_event(workflow_id, workflow_id) == workflow_id

            steps = DBOS.list_workflow_steps(workflow_id)
            assert len(steps) == 2
            assert (
                steps[0]["function_name"] == test_transaction.__qualname__
                if use_app_db
                else test_step.__qualname__
            )
            assert steps[1]["function_name"] == "DBOS.setEvent"
            DBOS.destroy()


def test_programmatic_migration(db_engine: sa.Engine, skip_with_sqlite: None) -> None:
    database_name = "migrate_test"
    migrate_role = "migrate-test-role"
    app_role = "app-test-role"
    role_password = "migrate_test_password"

    # Verify migration is agnostic to driver name (under the hood it uses postgresql+psycopg)
    db_url = db_engine.url.set(database=database_name).set(drivername="postgresql")

    # Drop the DBOS database if it exists. Create a test role with no permissions.
    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )
        for role in [migrate_role, app_role]:
            connection.execute(sa.text(f'DROP ROLE IF EXISTS "{role}"'))
            connection.execute(
                sa.text(f"CREATE ROLE \"{role}\" WITH LOGIN PASSWORD '{role_password}'")
            )
            connection.execute(
                sa.text(f'REVOKE CONNECT ON DATABASE postgres FROM "{role}"')
            )
        connection.execute(
            sa.text(f'CREATE DATABASE {database_name} OWNER "{migrate_role}"')
        )

    # Using the admin role, create the DBOS database and verify it exists.
    # Set permissions for the test role.
    schema = "F8nny_sCHem@-n@m3"
    migrate_url = (
        db_url.set(username=migrate_role)
        .set(password=role_password)
        .render_as_string(hide_password=False)
    )
    run_dbos_database_migrations(
        migrate_url,
        app_database_url=migrate_url,
        schema=schema,
        application_role=app_role,
    )
    with db_engine.connect() as c:
        c.execution_options(isolation_level="AUTOCOMMIT")
        result = c.execute(
            sa.text(
                f"SELECT COUNT(*) FROM pg_database WHERE datname = '{database_name}'"
            )
        ).scalar()
        assert result == 1

    # Initialize DBOS with the test role. Verify various operations work.
    test_db_url = (
        db_url.set(username=app_role).set(password=role_password)
    ).render_as_string(hide_password=False)
    DBOS.destroy(destroy_registry=True)
    config: DBOSConfig = {
        "name": "test_migrate",
        "database_url": test_db_url,
        "system_database_url": test_db_url,
        "dbos_system_schema": schema,
    }
    DBOS(config=config)

    @DBOS.transaction()
    def test_transaction() -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return str(rows[0][0])

    @DBOS.step()
    def test_step() -> str:
        return "1"

    @DBOS.workflow()
    def test_workflow() -> str:
        assert test_transaction() == "1"
        assert test_step() == "1"
        id = DBOS.workflow_id
        assert id
        DBOS.set_event(id, id)
        return id

    DBOS.launch()

    workflow_id = test_workflow()
    assert workflow_id
    assert DBOS.get_event(workflow_id, workflow_id) == workflow_id
    DBOS.destroy()


def test_concurrent_migrations(db_engine: sa.Engine, skip_with_sqlite: None) -> None:
    """Test that 10 system databases can be created and migrated concurrently on the same database."""
    num_instances = 10
    db_name = "concurrent_migration_test"

    # Clean up any leftover database from previous runs
    with db_engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(sa.text(f"DROP DATABASE IF EXISTS {db_name} WITH (FORCE)"))

    sys_url = db_engine.url.set(
        drivername="postgresql+psycopg", database=db_name
    ).render_as_string(hide_password=False)

    def create_and_migrate(index: int) -> None:
        sys_db = SystemDatabase.create(
            system_database_url=sys_url,
            engine_kwargs={},
            engine=None,
            schema="dbos",
            serializer=DefaultSerializer(),
            executor_id=None,
        )
        try:
            sys_db.run_migrations()
            # Verify migration succeeded
            with sys_db.engine.connect() as conn:
                rows = conn.execute(
                    sa.text("SELECT version FROM dbos.dbos_migrations")
                ).fetchall()
                assert len(rows) == 1
                assert rows[0][0] == len(get_dbos_migrations("dbos", True))
        finally:
            sys_db.destroy()

    # Run all migrations concurrently on the same database
    errors = []
    with ThreadPoolExecutor(max_workers=num_instances) as executor:
        futures = {
            executor.submit(create_and_migrate, i): i for i in range(num_instances)
        }
        for future in as_completed(futures):
            index = futures[future]
            try:
                future.result()
            except Exception as e:
                errors.append((index, e))

    # Assert no errors
    if errors:
        error_details = "\n".join(
            f"  Instance {idx}: {type(e).__name__}: {e}" for idx, e in errors
        )
        pytest.fail(
            f"{len(errors)}/{num_instances} concurrent migrations failed:\n{error_details}"
        )


def test_online_migrations_are_idempotent(dbos: DBOS, skip_with_sqlite: None) -> None:
    """Re-running every migration from the first online one onward against an
    already-migrated schema must succeed without error. Guards against
    missing IF [NOT] EXISTS clauses in any drop/create migration."""
    from dbos._migration import _ONLINE_MIGRATIONS, run_dbos_migrations

    engine = dbos._sys_db.engine
    schema = "dbos"
    rewind_to = min(_ONLINE_MIGRATIONS) - 1
    final_version = len(get_dbos_migrations(schema, True))

    with engine.begin() as conn:
        conn.execute(
            sa.text(f'UPDATE "{schema}".dbos_migrations SET version = :v'),
            {"v": rewind_to},
        )

    run_dbos_migrations(engine, schema, use_listen_notify=True)

    with engine.connect() as conn:
        version = conn.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
        ).scalar()
        assert version == final_version


def test_version_not_bumped_on_migration_failure(
    dbos: DBOS, skip_with_sqlite: None
) -> None:
    """If a migration raises mid-flight, the version counter must stay at the
    prior value so the runner re-attempts it on the next start."""
    from unittest.mock import patch

    from dbos._migration import run_dbos_migrations

    engine = dbos._sys_db.engine
    schema = "dbos"
    rewind_to_version = 31  # one before migration 32
    final_version = len(get_dbos_migrations(schema, True))

    # Rewind so migration 32 is pending again
    with engine.begin() as conn:
        conn.execute(
            sa.text(f'UPDATE "{schema}".dbos_migrations SET version = :v'),
            {"v": rewind_to_version},
        )

    # Replace migration 32 with invalid SQL. Its execution must raise, and
    # the runner must not advance the version past 31.
    with patch(
        "dbos._migration.get_dbos_migration_thirtytwo",
        return_value="THIS IS NOT VALID SQL",
    ):
        with pytest.raises(Exception):
            run_dbos_migrations(engine, schema, use_listen_notify=True)

    with engine.connect() as conn:
        version = conn.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
        ).scalar()
        assert version == rewind_to_version

    # Re-run with the real migrations: IF NOT EXISTS guards make 32+ idempotent
    # given the index still exists from the original fixture migration.
    run_dbos_migrations(engine, schema, use_listen_notify=True)

    with engine.connect() as conn:
        version = conn.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
        ).scalar()
        assert version == final_version


def test_should_migrate(dbos: DBOS, skip_with_sqlite: None) -> None:
    """should_migrate must return True when the schema is missing, the
    dbos_migrations table is missing, or the recorded version is behind the
    latest; and False once the schema is fully migrated."""
    from dbos._migration import should_migrate

    engine = dbos._sys_db.engine
    schema = "dbos"
    latest_version = len(get_dbos_migrations(schema, True))

    # A freshly-migrated dbos fixture should be up to date
    assert should_migrate(engine, schema, use_listen_notify=True) is False

    # Rewinding the version makes migrations pending again
    with engine.begin() as conn:
        conn.execute(
            sa.text(f'UPDATE "{schema}".dbos_migrations SET version = :v'),
            {"v": latest_version - 1},
        )
    assert should_migrate(engine, schema, use_listen_notify=True) is True

    # Restore version, then drop the migrations table to simulate a partially
    # initialized schema. should_migrate must report True.
    with engine.begin() as conn:
        conn.execute(
            sa.text(f'UPDATE "{schema}".dbos_migrations SET version = :v'),
            {"v": latest_version},
        )
    assert should_migrate(engine, schema, use_listen_notify=True) is False

    with engine.begin() as conn:
        conn.execute(sa.text(f'DROP TABLE "{schema}".dbos_migrations'))
    assert should_migrate(engine, schema, use_listen_notify=True) is True

    # A schema name that does not exist at all must also report True
    assert (
        should_migrate(engine, "nonexistent_schema_xyz", use_listen_notify=True) is True
    )


def test_runner_resumes_after_invalid_index(dbos: DBOS, skip_with_sqlite: None) -> None:
    """Simulate a CREATE INDEX CONCURRENTLY that crashed mid-build (leaving an
    INVALID index) and verify the runner cleans it up and re-runs the
    migration on the next start."""
    from dbos._migration import run_dbos_migrations

    engine = dbos._sys_db.engine
    schema = "dbos"
    target_index = "idx_workflow_status_in_flight"
    rewind_to_version = 31  # one before migration 32 which builds target_index
    final_version = len(get_dbos_migrations(schema, True))

    # Drop the existing valid index, then plant an INVALID index of the same
    # name. Flipping pg_index.indisvalid mimics what Postgres leaves behind
    # when CREATE INDEX CONCURRENTLY aborts mid-build.
    with engine.connect() as raw_conn:
        conn = raw_conn.execution_options(isolation_level="AUTOCOMMIT")
        conn.execute(sa.text(f'DROP INDEX IF EXISTS "{schema}"."{target_index}"'))
        conn.execute(
            sa.text(
                f'CREATE INDEX "{target_index}" ON "{schema}"."workflow_status" '
                "(queue_name, status, priority, created_at) "
                "WHERE status IN ('ENQUEUED', 'PENDING')"
            )
        )
        conn.execute(
            sa.text(
                "UPDATE pg_index SET indisvalid = false "
                f"WHERE indexrelid = '{schema}.{target_index}'::regclass"
            )
        )

    # Confirm the planted index is INVALID
    with engine.connect() as conn:
        valid = conn.execute(
            sa.text(
                "SELECT indisvalid FROM pg_index "
                f"WHERE indexrelid = '{schema}.{target_index}'::regclass"
            )
        ).scalar()
        assert valid is False

    # Rewind the version counter so the runner re-executes migration 32
    with engine.begin() as conn:
        conn.execute(
            sa.text(f'UPDATE "{schema}".dbos_migrations SET version = :v'),
            {"v": rewind_to_version},
        )

    # Re-run migrations: cleanup should drop the invalid index, then 32+ rebuild
    run_dbos_migrations(engine, schema, use_listen_notify=True)

    # The index now exists and is valid, and the version is back at the latest
    with engine.connect() as conn:
        valid = conn.execute(
            sa.text(
                "SELECT indisvalid FROM pg_index "
                f"WHERE indexrelid = '{schema}.{target_index}'::regclass"
            )
        ).scalar()
        assert valid is True

        version = conn.execute(
            sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
        ).scalar()
        assert version == final_version


def test_migrate_print_only(
    db_engine: sa.Engine,
    skip_with_sqlite: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Test that --print-only emits complete SQL that psql can apply to a fresh database."""
    database_name = "print_only_test"
    db_url = db_engine.url.set(database=database_name).set(drivername="postgresql")
    db_url_string = db_url.render_as_string(hide_password=False)
    latest_version = len(get_dbos_migrations("dbos", True))

    # Printing requires no database connection and includes grants for the role
    print_dbos_database_migrations(
        db_url_string,
        app_database_url=db_url_string,
        schema="dbos",
        application_role="print-only-role",
    )
    out = capsys.readouterr().out
    assert 'CREATE SCHEMA IF NOT EXISTS "dbos";' in out
    assert (
        f'INSERT INTO "dbos".dbos_migrations (version) VALUES ({latest_version});'
        in out
    )
    assert 'GRANT USAGE ON SCHEMA "dbos" TO "print-only-role";' in out
    assert "transaction_outputs" in out
    for line in out.splitlines():
        assert line.startswith("--") or not line.startswith(
            ("Starting", "Granting", "System database")
        )

    if shutil.which("psql") is None:
        pytest.skip("psql not available")

    # Print without a role (the role does not exist) and apply the SQL to a fresh database
    print_dbos_database_migrations(db_url_string, app_database_url=None, schema="dbos")
    sql = capsys.readouterr().out
    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )
        connection.execute(sa.text(f"CREATE DATABASE {database_name}"))

    with tempfile.NamedTemporaryFile("w", suffix=".sql") as f:
        f.write(sql)
        f.flush()
        subprocess.check_call(
            ["psql", db_url_string, "-v", "ON_ERROR_STOP=1", "-q", "-f", f.name]
        )

    # A real migration now considers the database up to date
    engine = sa.create_engine(db_url.set(drivername="postgresql+psycopg"))
    try:
        assert should_migrate(engine, "dbos", True) is False
        with engine.connect() as conn:
            version = conn.execute(
                sa.text('SELECT version FROM "dbos".dbos_migrations')
            ).scalar()
            assert version == latest_version
    finally:
        engine.dispose()

    # Re-applying the script to a non-fresh database must fail fast
    with tempfile.NamedTemporaryFile("w", suffix=".sql") as f:
        f.write(sql)
        f.flush()
        with pytest.raises(subprocess.CalledProcessError):
            subprocess.check_call(
                ["psql", db_url_string, "-v", "ON_ERROR_STOP=1", "-q", "-f", f.name],
                stderr=subprocess.DEVNULL,
            )

    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )


def test_migrate_print_only_custom_schema(
    db_engine: sa.Engine,
    skip_with_sqlite: None,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """--print-only must quote identifiers correctly for unusual schema names."""
    database_name = "print_only_schema_test"
    schema = "F8nny_sCHem@-n@m3"
    db_url = db_engine.url.set(database=database_name).set(drivername="postgresql")
    db_url_string = db_url.render_as_string(hide_password=False)
    latest_version = len(get_dbos_migrations(schema, True))

    print_dbos_database_migrations(db_url_string, app_database_url=None, schema=schema)
    sql = capsys.readouterr().out
    assert f'CREATE SCHEMA IF NOT EXISTS "{schema}";' in sql
    assert (
        f'CREATE TABLE IF NOT EXISTS "{schema}".dbos_migrations (version BIGINT NOT NULL PRIMARY KEY);'
        in sql
    )
    # Migration 10's guard embeds the schema in a single-quoted literal
    assert f"WHERE table_schema = '{schema}'" in sql
    assert (
        f'ALTER TABLE "{schema}".notifications ADD PRIMARY KEY (message_uuid);' in sql
    )
    assert (
        f'INSERT INTO "{schema}".dbos_migrations (version) VALUES ({latest_version});'
        in sql
    )
    # The unquoted schema name must never appear outside quotes or literals
    assert f"CREATE TABLE {schema}." not in sql

    # Schema names containing quotes are rejected
    with pytest.raises(Exception):
        print_dbos_database_migrations(
            db_url_string, app_database_url=None, schema='bad"schema'
        )
    capsys.readouterr()

    # The CLI flag path accepts the funny schema name
    cli_out = subprocess.check_output(
        ["dbos", "migrate", "-s", db_url_string, "--schema", schema, "--print-only"],
        text=True,
    )
    assert f'CREATE SCHEMA IF NOT EXISTS "{schema}";' in cli_out

    if shutil.which("psql") is None:
        pytest.skip("psql not available")

    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )
        connection.execute(sa.text(f"CREATE DATABASE {database_name}"))

    with tempfile.NamedTemporaryFile("w", suffix=".sql") as f:
        f.write(sql)
        f.flush()
        subprocess.check_call(
            ["psql", db_url_string, "-v", "ON_ERROR_STOP=1", "-q", "-f", f.name]
        )

    engine = sa.create_engine(db_url.set(drivername="postgresql+psycopg"))
    try:
        assert should_migrate(engine, schema, True) is False
        with engine.connect() as conn:
            assert (
                conn.execute(
                    sa.text(
                        "SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"
                    ),
                    {"s": schema},
                ).scalar()
                == 1
            )
            for table in ["workflow_status", "notifications", "dbos_migrations"]:
                assert conn.execute(
                    sa.text(f'SELECT COUNT(*) FROM "{schema}".{table}')
                ).scalar() in (0, 1)
            version = conn.execute(
                sa.text(f'SELECT version FROM "{schema}".dbos_migrations')
            ).scalar()
            assert version == latest_version
            # The notifications primary key exists (created by migration 1;
            # migration 10's guarded backfill was a no-op)
            assert (
                conn.execute(
                    sa.text(
                        "SELECT COUNT(*) FROM information_schema.table_constraints "
                        "WHERE table_schema = :s AND table_name = 'notifications' "
                        "AND constraint_type = 'PRIMARY KEY'"
                    ),
                    {"s": schema},
                ).scalar()
                == 1
            )
    finally:
        engine.dispose()

    with db_engine.connect() as connection:
        connection.execution_options(isolation_level="AUTOCOMMIT")
        connection.execute(
            sa.text(f"DROP DATABASE IF EXISTS {database_name} WITH (FORCE)")
        )
