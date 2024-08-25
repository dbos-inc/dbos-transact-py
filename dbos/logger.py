import logging
import os
from typing import TYPE_CHECKING, Any

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

if TYPE_CHECKING:
    from dbos.dbos_config import ConfigFile

dbos_logger = logging.getLogger("dbos")


class DBOSLogTransformer(logging.Filter):
    def __init__(self) -> None:
        super().__init__()
        self.app_id = os.environ.get("DBOS__APPID", "")
        self.app_version = os.environ.get("DBOS__APPVERSION", "")
        self.executor_id = os.environ.get("DBOS__VMID", "local")

    def filter(self, record: Any) -> bool:
        record.applicationID = self.app_id
        record.applicationVersion = self.app_version
        record.executorID = self.executor_id
        return True


# Mitigation for https://github.com/open-telemetry/opentelemetry-python/issues/3193
# Reduce the force flush timeout and auto-retry.
class PatchedOTLPLoggerProvider(LoggerProvider):
    def force_flush(self, timeout_millis: int = 5000) -> bool:
        max_tries = 5
        for _ in range(max_tries):
            ret = super().force_flush(timeout_millis)
            if ret:
                return True
        return False


def init_logger() -> None:
    # By default, log to the console
    if not dbos_logger.handlers:
        dbos_logger.propagate = False
        console_handler = logging.StreamHandler()
        console_formatter = logging.Formatter(
            "%(asctime)s [%(levelname)8s] (%(name)s:%(filename)s:%(lineno)s) %(message)s",
            datefmt="%H:%M:%S",
        )
        console_handler.setFormatter(console_formatter)
        dbos_logger.addHandler(console_handler)


def config_logger(config: "ConfigFile") -> None:
    # Configure the log level
    log_level = config.get("telemetry", {}).get("logs", {}).get("logLevel")  # type: ignore
    if log_level is not None:
        dbos_logger.setLevel(log_level)

    # Log to the OTLP endpoint if provided
    otlp_logs_endpoint = (
        config.get("telemetry", {}).get("OTLPExporter", {}).get("logsEndpoint")  # type: ignore
    )
    if otlp_logs_endpoint:
        log_provider = PatchedOTLPLoggerProvider(
            Resource.create(
                attributes={
                    "service.name": "dbos-application",
                }
            )
        )
        set_logger_provider(log_provider)
        log_provider.add_log_record_processor(
            BatchLogRecordProcessor(
                OTLPLogExporter(endpoint=otlp_logs_endpoint),
                export_timeout_millis=5000,
            )
        )
        otlp_handler = LoggingHandler(logger_provider=log_provider)

        # Attach DBOS-specific attributes to all log entries.
        otlp_transformer = DBOSLogTransformer()

        # Direct all logs to OTLP
        add_otlp_to_all_loggers(otlp_handler, otlp_transformer)


def add_otlp_to_all_loggers(
    otlp_handler: LoggingHandler, otlp_transformer: DBOSLogTransformer
) -> None:
    root = logging.root

    root.addHandler(otlp_handler)
    root.addFilter(otlp_transformer)

    for logger_name in root.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        if not logger.propagate:
            logger.addHandler(otlp_handler)
        logger.addFilter(otlp_transformer)
