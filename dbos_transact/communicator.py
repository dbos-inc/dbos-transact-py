from dbos_transact.context import DBOSContext


class CommunicatorContext(DBOSContext):
    def __init__(self, function_id: int):
        self.function_id = function_id
        super().__init__()
