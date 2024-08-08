import logging

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource

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

    otlp_logs_endpoint = (
        config.get("telemetry", {}).get("OTLPExporter", {}).get("logsEndpoint")
    )
    if otlp_logs_endpoint:
        resource = Resource(attributes={"service.name": "dbos-application"})

        log_provider = LoggerProvider(resource=resource)
        set_logger_provider(log_provider)

        otlp_exporter = OTLPLogExporter(endpoint=otlp_logs_endpoint)
        log_processor = BatchLogRecordProcessor(otlp_exporter)
        log_provider.add_log_record_processor(log_processor)

        # Add OpenTelemetry handler to the root logger
        otlp_handler = LoggingHandler(
            level=logging.NOTSET, logger_provider=log_provider
        )
        root_logger = logging.getLogger()
        root_logger.addHandler(otlp_handler)
        dbos_logger.addHandler(otlp_handler)
        dbos_logger.info("bob")
