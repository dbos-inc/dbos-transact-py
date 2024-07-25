from sqlalchemy import Connection


class TransactionContext:

    def __init__(self, connection: Connection, function_id: int):
        self.connection = connection
        self.function_id = function_id
