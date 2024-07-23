from dbos_transact.system_database import SystemDatabase


class WorkflowContext:

    def __init__(self, sys_db: SystemDatabase):
        self.sys_db = sys_db
