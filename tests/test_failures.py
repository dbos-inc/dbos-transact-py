from sqlalchemy.exc import DBAPIError

from dbos_transact import DBOS


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

    res = test_retry_transaction(10)
    assert res == 10
    assert retry_counter == 10
