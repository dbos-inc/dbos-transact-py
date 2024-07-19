from dbos_transact import DBOS

def test_dbos():
    dbos = DBOS()
    assert dbos.example() == 0