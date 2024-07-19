import logging

class DBOS:
    def __init__(self) -> None:
        logger = logging.getLogger("dbos")
        logger.info("Initializing DBOS!")

    def example(self) -> int:
        return 0