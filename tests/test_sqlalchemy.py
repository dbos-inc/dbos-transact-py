import uuid

import pytest
import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

# Public API
from dbos import DBOS, SetWorkflowID


# Declare a SQLAlchemy ORM base class
class Base(DeclarativeBase):
    pass


# Declare a SQLAlchemy ORM class for accessing the database table.
class Hello(Base):
    __tablename__ = "dbos_hello"
    greet_count: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(nullable=False)

    def __repr__(self) -> str:
        return f"Hello(greet_count={self.greet_count!r}, name={self.name!r})"


def test_simple_transaction(
    dbos: DBOS, db_engine: sa.Engine, skip_with_sqlite: None
) -> None:
    txn_counter: int = 0
    assert dbos._app_db_field is not None
    Base.metadata.drop_all(dbos._app_db_field.engine)
    Base.metadata.create_all(dbos._app_db_field.engine)

    @DBOS.transaction()
    def test_transaction(name: str) -> str:
        new_greeting = Hello(name=name)
        DBOS.sql_session.add(new_greeting)
        stmt = (
            sa.select(Hello)
            .where(Hello.name == name)
            .order_by(Hello.greet_count.desc())
            .limit(1)
        )
        row = DBOS.sql_session.scalar(stmt)
        assert row is not None
        greet_count = row.greet_count
        nonlocal txn_counter
        txn_counter += 1
        return name + str(greet_count)

    assert test_transaction("alice") == "alice1"
    assert test_transaction("alice") == "alice2"
    assert txn_counter == 2

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        assert test_transaction("alice") == "alice3"
    with SetWorkflowID(wfuuid):
        assert test_transaction("alice") == "alice3"
    assert txn_counter == 3  # Only increment once

    Base.metadata.drop_all(dbos._app_db_field.engine)

    # Make sure no transactions are left open
    with db_engine.begin() as conn:
        result = conn.execute(
            sa.text(
                "select * from pg_stat_activity where state = 'idle in transaction'"
            )
        ).fetchall()
        assert len(result) == 0


def test_error_transaction(
    dbos: DBOS, db_engine: sa.Engine, skip_with_sqlite: None
) -> None:
    txn_counter: int = 0
    assert dbos._app_db_field is not None
    # Drop the database but don't re-create. Should fail.
    Base.metadata.drop_all(dbos._app_db_field.engine)

    @DBOS.transaction()
    def test_transaction(name: str) -> str:
        nonlocal txn_counter
        txn_counter += 1
        new_greeting = Hello(name=name)
        DBOS.sql_session.add(new_greeting)
        return name

    with pytest.raises(Exception) as exc_info:
        test_transaction("alice")
    assert 'relation "dbos_hello" does not exist' in str(exc_info.value)
    assert txn_counter == 1

    # Test OAOO
    wfuuid = str(uuid.uuid4())
    with SetWorkflowID(wfuuid):
        with pytest.raises(Exception) as exc_info:
            test_transaction("alice")
    assert 'relation "dbos_hello" does not exist' in str(exc_info.value)
    assert txn_counter == 2

    with SetWorkflowID(wfuuid):
        with pytest.raises(Exception) as exc_info:
            test_transaction("alice")
    assert 'relation "dbos_hello" does not exist' in str(exc_info.value)
    assert txn_counter == 2

    # Make sure no transactions are left open
    with db_engine.begin() as conn:
        result = conn.execute(
            sa.text(
                "select * from pg_stat_activity where state = 'idle in transaction'"
            )
        ).fetchall()
        assert len(result) == 0
