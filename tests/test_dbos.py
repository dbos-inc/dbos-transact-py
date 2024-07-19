from dbos_transact.dbos import DBOS

def test_dbos():
    dbos = DBOS()
    assert dbos.example() == 0