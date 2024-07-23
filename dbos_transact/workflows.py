from dbos_transact.system_database import SystemDatabase


class WorkflowContext:

    def __init__(self, workflow_uuid: str, sys_db: SystemDatabase):
        self.workflow_uuid = workflow_uuid
        self.sys_db = sys_db
