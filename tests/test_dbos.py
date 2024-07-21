from dbos_transact import DBOS

from . import conftest


def test_dbos():
    dbos = DBOS(conftest.defaultConfig)
    assert dbos.example() == "postgres"
