from sqlalchemy.orm import Session


class TransactionContext:

    def __init__(self, session: Session, function_id: int):
        self.session = session
        self.function_id = function_id
