from typing import Tuple

import sqlalchemy as sa
from fastapi import FastAPI
from fastapi.testclient import TestClient

from dbos_transact.dbos import DBOS


def test_simple_endpoint(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi
    client = TestClient(app)

    @app.get("/{var}/{var2}")
    def endpoint(var: str, var2: str) -> str:
        return test_workflow(var, var2)

    @dbos.workflow()
    def test_workflow(var1: str, var2: str) -> str:
        res1 = test_transaction(var1)
        res2 = test_communicator(var2)
        return res1 + res2

    @dbos.transaction()
    def test_transaction(var: str) -> str:
        rows = DBOS.sql_session.execute(sa.text("SELECT 1")).fetchall()
        return var + str(rows[0][0])

    @dbos.communicator()
    def test_communicator(var: str) -> str:
        return var

    response = client.get("/bob/bob")
    assert response.status_code == 200
    assert response.text == '"bob1bob"'
