import logging
import os
from typing import Any

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from dbos_transact.dbos_config import ConfigFile

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
# Reduce the force flush timeout
class PatchedOTLPLoggerProvider(LoggerProvider):
    def force_flush(self, timeout_millis: int = 5000) -> bool:
        return super().force_flush(timeout_millis)


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

        otlp_logs_endpoint = (
            config.get("telemetry", {}).get("OTLPExporter", {}).get("logsEndpoint")  # type: ignore
        )
        if otlp_logs_endpoint:
            # Also log to the OTLP endpoint if provided
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
            dbos_logger.addHandler(otlp_handler)

            # Attach DBOS-specific attributes to all log entries.
            log_transformer = DBOSLogTransformer()
            dbos_logger.addFilter(log_transformer)

            # Attach the OTLP logger and transformer to the root logger
            root_logger = logging.getLogger()
            root_logger.addHandler(otlp_handler)
            root_logger.addFilter(log_transformer)
