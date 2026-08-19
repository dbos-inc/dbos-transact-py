import logging

import pytest

import dbos._logger
from dbos._logger import add_otlp_to_all_loggers, dbos_logger

# OTel's own non-propagating loggers. LoggingHandler.emit() reports re-entrancy
# through the first one, and the SDK's export path guards itself with the second.
OTEL_GUARD_LOGGERS = [
    "opentelemetry.instrumentation.logging.handler.internal",
    "opentelemetry.sdk._logs._internal.export.propagate.false",
]


def _detach_everywhere(handler: logging.Handler) -> None:
    logging.root.removeHandler(handler)
    for name in list(logging.root.manager.loggerDict):
        logging.getLogger(name).removeHandler(handler)


def test_add_otlp_to_all_loggers_skips_otel_guard_loggers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # add_otlp_to_all_loggers only runs under DBOS Cloud, so drive it directly.
    from opentelemetry.instrumentation.logging.handler import LoggingHandler
    from opentelemetry.sdk._logs import LoggerProvider
    from opentelemetry.sdk._logs import export as _sdk_log_export  # noqa: F401

    handler = LoggingHandler(logger_provider=LoggerProvider(), log_code_attributes=True)

    # A non-OTel logger opting out of propagation: what the fan-out is meant to catch.
    app_logger = logging.getLogger("tests.test_logger.nonpropagating")
    app_logger.propagate = False

    for name in OTEL_GUARD_LOGGERS:
        assert not logging.getLogger(name).propagate, (
            f"{name} is expected to be non-propagating; without the skip in "
            "add_otlp_to_all_loggers it would be handed the OTLP handler"
        )

    monkeypatch.setattr(dbos._logger, "_otlp_handler", handler)
    try:
        add_otlp_to_all_loggers()

        assert handler in logging.root.handlers
        assert handler in app_logger.handlers
        for name in OTEL_GUARD_LOGGERS:
            assert handler not in logging.getLogger(name).handlers

        # The real regression: with the guard armed, emit() reports re-entrancy
        # through the internal logger. If the handler were attached to it, that
        # report would re-enter emit() and recurse until the stack blew.
        record = dbos_logger.makeRecord(
            dbos_logger.name, logging.INFO, __file__, 0, "probe", (), None
        )
        token = LoggingHandler._is_emitting.set(True)
        try:
            handler.emit(record)
        finally:
            LoggingHandler._is_emitting.reset(token)
    finally:
        _detach_everywhere(handler)
        app_logger.propagate = True

    assert handler not in logging.root.handlers
