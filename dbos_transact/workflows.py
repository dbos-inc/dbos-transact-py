from typing import cast

from dbos_transact.transaction import TransactionContext


class WorkflowContext:

    def __init__(self, workflow_uuid: str):
        self.workflow_uuid = workflow_uuid
        self.function_id = 0

    def txn_ctx(self) -> TransactionContext:
        return cast(TransactionContext, self)
