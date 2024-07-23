import logging

from dbos_transact.dbos_config import ConfigFile

dbos_logger = logging.getLogger("dbos")


def config_logger(config: ConfigFile) -> None:
    # Configure the DBOS logger. Log to the console by default.
    if not dbos_logger.handlers:
        dbos_logger.propagate = False
        log_level = config.get("telemetry", {}).get("logs", {}).get("logLevel")  # type: ignore
        if log_level is not None:
            dbos_logger.setLevel(log_level)
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)8s] (%(name)s:%(filename)s:%(lineno)s) %(message)s",
            datefmt="%H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)
        dbos_logger.addHandler(console_handler)
