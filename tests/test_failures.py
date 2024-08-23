import pytest
import sqlalchemy as sa
from sqlalchemy.exc import DBAPIError

# Public API
from dbos import DBOS


def test_transaction_errors(dbos: DBOS) -> None:
    retry_counter: int = 0

    @dbos.transaction()
    def test_retry_transaction(max_retry: int) -> int:
        nonlocal retry_counter
        if retry_counter < max_retry:
            retry_counter += 1
            base_err = BaseException()
            base_err.pgcode = "40001"  # type: ignore
            err = DBAPIError("Serialization test error", {}, base_err)
            raise err
        return max_retry

    @dbos.transaction()
    def test_noretry_transaction() -> None:
        nonlocal retry_counter
        retry_counter += 1
        DBOS.sql_session.execute(sa.text("selct abc from c;")).fetchall()

    res = test_retry_transaction(10)
    assert res == 10
    assert retry_counter == 10

    with pytest.raises(Exception) as exc_info:
        test_noretry_transaction()
    assert exc_info.value.orig.pgcode == "42601"  # type: ignore
    assert retry_counter == 11
