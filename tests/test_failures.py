from sqlalchemy.exc import DBAPIError

from dbos_transact import DBOS


def test_transaction_errors(dbos: DBOS) -> None:
    txn_counter: int = 0

    @dbos.workflow()
    def test_workflow(max_retry: int) -> int:
        res = test_transaction(max_retry)
        return res

    @dbos.transaction()
    def test_transaction(max_retry: int) -> int:
        nonlocal txn_counter
        if txn_counter < max_retry:
            txn_counter += 1
            base_err = BaseException()
            base_err.pgcode = "40001"  # type: ignore
            err = DBAPIError("Serialization test error", {}, base_err)
            raise err
        return max_retry

    res = test_workflow(10)
    assert res == 10
    assert txn_counter == 10
