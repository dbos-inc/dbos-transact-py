import os
import sys
from pathlib import Path

from dbos import DBOS, SetWorkflowID
from tests import client_collateral as cc
from tests.conftest import default_config

app_vers = os.environ.get("DBOS__APPVERSION")
if app_vers is None:
    DBOS.logger.error("DBOS__APPVERSION not set")
    os._exit(1)
else:
    DBOS.logger.info(f"DBOS__APPVERSION: {app_vers}")

if len(sys.argv) < 3:
    DBOS.logger.error("Usage: client_worker sqlite_path wfid <topic>")
    os._exit(1)

sqlite_path = Path(sys.argv[1])
wfid = sys.argv[2]
topic = sys.argv[3] if len(sys.argv) > 3 else None

config = default_config(sqlite_path)
DBOS(config=config)
DBOS.launch()

DBOS.logger.info(f"Starting send_test with WF ID: {wfid}")
with SetWorkflowID(wfid):
    DBOS.start_workflow(cc.send_test, topic)

os._exit(0)
