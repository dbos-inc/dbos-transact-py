import logging
import os

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

from dbos_transact.dbos_config import ConfigFile

dbos_logger = logging.getLogger("dbos")


class AttributeFilter(logging.Filter):
    def __init__(self, app_id, app_version, executor_id):
        super().__init__()
        self.app_id = app_id
        self.app_version = app_version
        self.executor_id = executor_id

    def filter(self, record):
        record.applicationID = self.app_id
        record.applicationVersion = self.app_version
        record.executorID = self.executor_id
        return True


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
        config.get("telemetry", {}).get("OTLPExporter", {}).get("logsEndpoint")
    )
    if otlp_logs_endpoint:
        resource = Resource.create(
            attributes={
                "service.name": "dbos-application",
            }
        )
        log_provider = LoggerProvider(resource=resource)
        set_logger_provider(log_provider)
        otlp_exporter = OTLPLogExporter(endpoint=otlp_logs_endpoint)
        log_processor = BatchLogRecordProcessor(otlp_exporter)
        log_provider.add_log_record_processor(log_processor)
        otlp_handler = LoggingHandler(
            level=logging.NOTSET, logger_provider=log_provider
        )

        root_logger = logging.getLogger()
        dbos_logger.addHandler(otlp_handler)
        root_logger.addHandler(otlp_handler)

        application_id = os.environ.get("DBOS__APPID", "")
        application_version = os.environ.get("DBOS__APPVERSION", "")
        executor_id = os.environ.get("DBOS__VMID", "")
        attribute_filter = AttributeFilter(
            application_id, application_version, executor_id
        )
        dbos_logger.addFilter(attribute_filter)
        root_logger.addFilter(attribute_filter)
