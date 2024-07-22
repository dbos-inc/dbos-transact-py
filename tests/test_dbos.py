from dbos_transact import DBOS


def test_dbos(reset_test_database):
    config, _ = reset_test_database
    dbos = DBOS(config)
    assert dbos.example() == "postgres"
