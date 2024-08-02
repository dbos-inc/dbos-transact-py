from sqlalchemy.orm import Session

from dbos_transact.context import DBOSContext


class TransactionContext(DBOSContext):

    def __init__(self, session: Session, function_id: int):
        self.session = session
        self.function_id = function_id
        super().__init__()
