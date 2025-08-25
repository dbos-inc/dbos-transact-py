import logging
import os
from typing import TYPE_CHECKING, Any

from opentelemetry._logs import set_logger_provider
from opentelemetry.exporter.otlp.proto.http._log_exporter import OTLPLogExporter
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor
from opentelemetry.sdk.resources import Resource
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace.span import format_trace_id

from dbos._utils import GlobalParams

if TYPE_CHECKING:
    from ._dbos_config import ConfigFile

dbos_logger = logging.getLogger("dbos")
_otlp_handler, _dbos_log_transformer = None, None


class DBOSLogTransformer(logging.Filter):
    def __init__(self, config: "ConfigFile") -> None:
        super().__init__()
        self.app_id = os.environ.get("DBOS__APPID", "")
        self.otlp_attributes: dict[str, str] = config.get("telemetry", {}).get("otlp_attributes", {})  # type: ignore

    def filter(self, record: Any) -> bool:
        record.applicationID = self.app_id
        record.applicationVersion = GlobalParams.app_version
        record.executorID = GlobalParams.executor_id
        for k, v in self.otlp_attributes.items():
            setattr(record, k, v)

        # If available, decorate the log entry with Workflow ID and Trace ID
        from dbos._context import get_local_dbos_context

        ctx = get_local_dbos_context()
        if ctx:
            if ctx.is_within_workflow():
                record.operationUUID = ctx.workflow_id
            span = ctx.get_current_span()
            if span:
                trace_id = format_trace_id(span.get_span_context().trace_id)
                record.traceId = trace_id

        return True


# Mitigation for https://github.com/open-telemetry/opentelemetry-python/issues/3193
# Reduce the force flush timeout
class PatchedOTLPLoggerProvider(LoggerProvider):
    def force_flush(self, timeout_millis: int = 5000) -> bool:
        return super().force_flush(timeout_millis)


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
    otlp_logs_endpoints = (
        config.get("telemetry", {}).get("OTLPExporter", {}).get("logsEndpoint")  # type: ignore
    )
    disable_otlp = config.get("telemetry", {}).get("disable_otlp", False)  # type: ignore

    if not disable_otlp and otlp_logs_endpoints:
        log_provider = PatchedOTLPLoggerProvider(
            Resource.create(
                attributes={
                    ResourceAttributes.SERVICE_NAME: config["name"],
                }
            )
        )
        set_logger_provider(log_provider)
        for e in otlp_logs_endpoints:
            log_provider.add_log_record_processor(
                BatchLogRecordProcessor(
                    OTLPLogExporter(endpoint=e),
                    export_timeout_millis=5000,
                )
            )
        global _otlp_handler
        _otlp_handler = LoggingHandler(logger_provider=log_provider)

        # Direct DBOS logs to OTLP
        dbos_logger.addHandler(_otlp_handler)

    # Attach DBOS-specific attributes to all log entries.
    global _dbos_log_transformer
    _dbos_log_transformer = DBOSLogTransformer(config)
    dbos_logger.addFilter(_dbos_log_transformer)


def add_otlp_to_all_loggers() -> None:
    if _otlp_handler is not None:
        root = logging.root
        root.addHandler(_otlp_handler)
        for logger_name in root.manager.loggerDict:
            if logger_name != dbos_logger.name:
                logger = logging.getLogger(logger_name)
                if not logger.propagate:
                    logger.addHandler(_otlp_handler)


def add_transformer_to_all_loggers() -> None:
    if _dbos_log_transformer is not None:
        root = logging.root
        root.addFilter(_dbos_log_transformer)
        for logger_name in root.manager.loggerDict:
            if logger_name != dbos_logger.name:
                logger = logging.getLogger(logger_name)
                logger.addFilter(_dbos_log_transformer)
