from dbos_transact import DBOS


def test_dbos(dbos):
    assert dbos.example() == "postgres"
