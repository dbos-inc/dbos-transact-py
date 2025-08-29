from dbos import DBOS
from dbos._mutex import DBOSMutex


def test_mutex(dbos: DBOS) -> None:

    @DBOSMutex("")
    def test_lock() -> bool:
        return True

    test_lock()


def test_mutex_in_transaction(dbos: DBOS) -> None:

    @DBOS.transaction()
    def test_lock() -> bool:

        with DBOSMutex("test") as m:
            return True

    test_lock()
