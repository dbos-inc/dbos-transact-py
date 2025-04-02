import os
import sys

from dbos import DBOS, SetWorkflowID
from tests import client_collateral as cc
from tests.conftest import default_config

app_vers = os.environ.get("DBOS__APPVERSION")
if app_vers is None:
    DBOS.logger.error("DBOS__APPVERSION not set")
    os._exit(1)
else:
    DBOS.logger.info(f"DBOS__APPVERSION: {app_vers}")

if len(sys.argv) < 2:
    DBOS.logger.error("Usage: client_worker wfid <topic>")
    os._exit(1)

wfid = sys.argv[1]
topic = sys.argv[2] if len(sys.argv) > 2 else None

config = default_config()
DBOS(config=config)
DBOS.launch()

DBOS.logger.info(f"Starting send_test with WF ID: {wfid}")
with SetWorkflowID(wfid):
    DBOS.start_workflow(cc.send_test, topic)

os._exit(0)
