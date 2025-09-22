from dataclasses import dataclass, field
from typing import Optional, Tuple

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from inline_snapshot import snapshot
from opentelemetry._logs import set_logger_provider
from opentelemetry.sdk import trace as tracesdk
from opentelemetry.sdk._logs import LoggerProvider, LoggingHandler
from opentelemetry.sdk._logs.export import BatchLogRecordProcessor, InMemoryLogExporter
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.trace.span import format_trace_id

from dbos import DBOS, DBOSConfig
from dbos._logger import dbos_logger
from dbos._tracer import dbos_tracer
from dbos._utils import GlobalParams


@dataclass
class BasicSpan:
    content: str
    children: list["BasicSpan"] = field(default_factory=list)
    parent_id: Optional[int] = field(repr=False, compare=False, default=None)


def test_spans(config: DBOSConfig) -> None:
    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    dbos_tracer.set_provider(provider)

    DBOS.destroy(destroy_registry=True)
    config["otlp_attributes"] = {"foo": "bar"}
    DBOS(config=config)
    DBOS.launch()

    my_tracer = provider.get_tracer("dbos")

    @DBOS.workflow()
    def test_workflow() -> None:
        with my_tracer.start_as_current_span(  # pyright: ignore[reportAttributeAccessIssue]
            "manual_span"
        ):
            test_step()
            current_span = DBOS.span
            subspan = DBOS.tracer.start_span(
                {"name": "a new span"}, parent=current_span
            )
            # Note: DBOS.tracer.start_span() does not set the new span as the current span. So this log is still attached to the workflow span.
            DBOS.logger.info("This is a test_workflow")
            subspan.add_event("greeting_event", {"name": "a new event"})
            DBOS.tracer.end_span(subspan)

    @DBOS.step()
    def test_step() -> None:
        DBOS.logger.info("This is a test_step")
        return

    # Set up in-memory log exporter
    log_exporter = InMemoryLogExporter()  # type: ignore
    log_processor = BatchLogRecordProcessor(log_exporter)
    log_provider = LoggerProvider()
    log_provider.add_log_record_processor(log_processor)
    set_logger_provider(log_provider)
    dbos_logger.addHandler(LoggingHandler(logger_provider=log_provider))

    test_workflow()

    log_processor.force_flush(timeout_millis=5000)
    logs = log_exporter.get_finished_logs()
    assert len(logs) == 2
    for log in logs:
        assert log.log_record.attributes is not None
        assert (
            log.log_record.attributes["applicationVersion"] == DBOS.application_version
        )
        assert log.log_record.attributes["executorID"] == GlobalParams.executor_id
        assert log.log_record.attributes["foo"] == "bar"
        # Make sure the log record has a span_id and trace_id
        assert log.log_record.span_id is not None and log.log_record.span_id > 0
        assert log.log_record.trace_id is not None and log.log_record.trace_id > 0
        assert (
            log.log_record.body == "This is a test_step"
            or log.log_record.body == "This is a test_workflow"
        )
        assert log.log_record.attributes["traceId"] == format_trace_id(
            log.log_record.trace_id
        )

    spans = exporter.get_finished_spans()

    for span in spans:
        if span.name == "manual_span":
            # Skip the manual span because it was not created by DBOS.tracer
            continue
        assert span.attributes is not None
        assert span.attributes["applicationVersion"] == DBOS.application_version
        assert span.attributes["executorID"] == GlobalParams.executor_id
        assert span.context is not None
        assert span.attributes["foo"] == "bar"
        assert span.context.span_id > 0
        assert span.context.trace_id > 0

    assert spans[0].name == test_step.__qualname__
    assert spans[1].name == "a new span"
    assert spans[3].name == test_workflow.__qualname__

    assert spans[0].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[1].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[2].parent.span_id == spans[3].context.span_id  # type: ignore
    assert spans[3].parent == None

    # Span ID and trace ID should match the log record
    # For pyright
    assert spans[0].context is not None
    assert spans[2].context is not None
    assert logs[0].log_record.span_id == spans[0].context.span_id
    assert logs[0].log_record.trace_id == spans[0].context.trace_id
    assert logs[1].log_record.span_id == spans[2].context.span_id
    assert logs[1].log_record.trace_id == spans[2].context.trace_id

    # Test the span tree structure
    basic_spans = {
        span.context.span_id: BasicSpan(  # pyright: ignore[reportOptionalMemberAccess]
            content=span.name, parent_id=span.parent.span_id if span.parent else None
        )
        for span in spans
    }
    root_span = None
    for basic_span in basic_spans.values():
        if basic_span.parent_id is None:
            root_span = basic_span
        else:
            parent_id = basic_span.parent_id
            parent_span = basic_spans[parent_id]
            parent_span.children.append(basic_span)

    assert len(spans) == 4
    # Make sure the span tree structure is correct
    assert root_span == snapshot(
        BasicSpan(
            content="test_spans.<locals>.test_workflow",
            children=[
                BasicSpan(
                    content="manual_span",
                    children=[
                        BasicSpan(content="test_spans.<locals>.test_step"),
                        BasicSpan(content="a new span"),
                    ],
                )
            ],
        )
    )


@pytest.mark.asyncio
async def test_spans_async(dbos: DBOS) -> None:
    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    dbos_tracer.set_provider(provider)

    my_tracer = provider.get_tracer("dbos")

    @DBOS.workflow()
    async def test_workflow() -> None:
        with my_tracer.start_as_current_span(  # pyright: ignore[reportAttributeAccessIssue]
            "manual_span"
        ):
            await test_step()
            current_span = DBOS.span
            subspan = DBOS.tracer.start_span(
                {"name": "a new span"}, parent=current_span
            )
            # Note: DBOS.tracer.start_span() does not set the new span as the current span. So this log is still attached to the workflow span.
            DBOS.logger.info("This is a test_workflow")
            subspan.add_event("greeting_event", {"name": "a new event"})
            DBOS.tracer.end_span(subspan)

    @DBOS.step()
    async def test_step() -> None:
        DBOS.logger.info("This is a test_step")
        return

    # Set up in-memory log exporter
    log_exporter = InMemoryLogExporter()  # type: ignore
    log_processor = BatchLogRecordProcessor(log_exporter)
    log_provider = LoggerProvider()
    log_provider.add_log_record_processor(log_processor)
    set_logger_provider(log_provider)
    dbos_logger.addHandler(LoggingHandler(logger_provider=log_provider))

    await test_workflow()

    log_processor.force_flush(timeout_millis=5000)
    logs = log_exporter.get_finished_logs()
    assert len(logs) == 2
    for log in logs:
        assert log.log_record.attributes is not None
        assert (
            log.log_record.attributes["applicationVersion"] == DBOS.application_version
        )
        assert log.log_record.attributes["executorID"] == GlobalParams.executor_id
        # Make sure the log record has a span_id and trace_id
        assert log.log_record.span_id is not None and log.log_record.span_id > 0
        assert log.log_record.trace_id is not None and log.log_record.trace_id > 0
        assert (
            log.log_record.body == "This is a test_step"
            or log.log_record.body == "This is a test_workflow"
        )
        assert log.log_record.attributes["traceId"] == format_trace_id(
            log.log_record.trace_id
        )

    spans = exporter.get_finished_spans()

    assert len(spans) == 4

    for span in spans:
        if span.name == "manual_span":
            # Skip the manual span because it was not created by DBOS.tracer
            continue
        assert span.attributes is not None
        assert span.attributes["applicationVersion"] == DBOS.application_version
        assert span.attributes["executorID"] == GlobalParams.executor_id
        assert span.context is not None
        assert span.context.span_id > 0
        assert span.context.trace_id > 0

    assert spans[0].name == test_step.__qualname__
    assert spans[1].name == "a new span"
    assert spans[3].name == test_workflow.__qualname__

    assert spans[0].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[1].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[2].parent.span_id == spans[3].context.span_id  # type: ignore
    assert spans[3].parent == None

    # Span ID and trace ID should match the log record
    assert spans[0].context is not None
    assert spans[2].context is not None
    assert logs[0].log_record.span_id == spans[0].context.span_id
    assert logs[0].log_record.trace_id == spans[0].context.trace_id
    assert logs[1].log_record.span_id == spans[2].context.span_id
    assert logs[1].log_record.trace_id == spans[2].context.trace_id

    # Test the span tree structure
    basic_spans = {
        span.context.span_id: BasicSpan(  # pyright: ignore[reportOptionalMemberAccess]
            content=span.name, parent_id=span.parent.span_id if span.parent else None
        )
        for span in spans
    }
    root_span = None
    for basic_span in basic_spans.values():
        if basic_span.parent_id is None:
            root_span = basic_span
        else:
            parent_id = basic_span.parent_id
            parent_span = basic_spans[parent_id]
            parent_span.children.append(basic_span)

    assert len(spans) == 4
    # Make sure the span tree structure is correct
    assert root_span == snapshot(
        BasicSpan(
            content="test_spans_async.<locals>.test_workflow",
            children=[
                BasicSpan(
                    content="manual_span",
                    children=[
                        BasicSpan(content="test_spans_async.<locals>.test_step"),
                        BasicSpan(content="a new span"),
                    ],
                )
            ],
        )
    )


def test_wf_fastapi(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi

    @app.get("/wf")
    @DBOS.workflow()
    def test_workflow_endpoint() -> str:
        dbos.logger.info("This is a test_workflow_endpoint")
        return "test"

    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    dbos_tracer.set_provider(provider)

    # Set up in-memory log exporter
    log_exporter = InMemoryLogExporter()  # type: ignore
    log_processor = BatchLogRecordProcessor(log_exporter)
    log_provider = LoggerProvider()
    log_provider.add_log_record_processor(log_processor)
    set_logger_provider(log_provider)
    dbos_logger.addHandler(LoggingHandler(logger_provider=log_provider))

    client = TestClient(app)
    response = client.get("/wf")
    assert response.status_code == 200
    assert response.text == '"test"'

    log_processor.force_flush(timeout_millis=5000)
    logs = log_exporter.get_finished_logs()
    assert len(logs) == 1
    assert logs[0].log_record.attributes is not None
    assert (
        logs[0].log_record.attributes["applicationVersion"] == DBOS.application_version
    )
    assert logs[0].log_record.span_id is not None and logs[0].log_record.span_id > 0
    assert logs[0].log_record.trace_id is not None and logs[0].log_record.trace_id > 0
    assert logs[0].log_record.body == "This is a test_workflow_endpoint"
    assert logs[0].log_record.attributes["traceId"] == format_trace_id(
        logs[0].log_record.trace_id
    )

    spans = exporter.get_finished_spans()

    assert len(spans) == 2

    for span in spans:
        assert span.attributes is not None
        assert span.attributes["applicationVersion"] == DBOS.application_version
        assert span.context is not None
        assert span.context.span_id > 0
        assert span.context.trace_id > 0

    assert spans[0].name == test_workflow_endpoint.__qualname__
    assert spans[1].name == "/wf"
    assert spans[1].attributes is not None
    assert spans[1].attributes["responseCode"] == 200

    assert spans[0].parent.span_id == spans[1].context.span_id  # type: ignore
    assert spans[1].parent == None

    # Span ID and trace ID should match the log record
    assert spans[0].context is not None
    assert logs[0].log_record.span_id == spans[0].context.span_id
    assert logs[0].log_record.trace_id == spans[0].context.trace_id


def test_disable_otlp_no_spans(config: DBOSConfig) -> None:
    DBOS.destroy(destroy_registry=True)
    config["otlp_attributes"] = {"foo": "bar"}
    config["enable_otlp"] = False
    DBOS(config=config)
    DBOS.launch()

    @DBOS.workflow()
    def test_workflow() -> None:
        test_step()
        DBOS.logger.info("This is a test_workflow")

    @DBOS.step()
    def test_step() -> None:
        DBOS.logger.info("This is a test_step")
        return

    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    dbos_tracer.set_provider(provider)

    # Set up in-memory log exporter
    log_exporter = InMemoryLogExporter()  # type: ignore
    log_processor = BatchLogRecordProcessor(log_exporter)
    log_provider = LoggerProvider()
    log_provider.add_log_record_processor(log_processor)
    set_logger_provider(log_provider)
    dbos_logger.addHandler(LoggingHandler(logger_provider=log_provider))

    test_workflow()

    log_processor.force_flush(timeout_millis=5000)
    logs = log_exporter.get_finished_logs()
    assert len(logs) == 2
    for log in logs:
        assert log.log_record.attributes is not None
        assert (
            log.log_record.attributes["applicationVersion"] == DBOS.application_version
        )
        assert log.log_record.attributes["executorID"] == GlobalParams.executor_id
        assert log.log_record.attributes["foo"] == "bar"
        # We disable OTLP, so no span_id or trace_id should be present
        assert log.log_record.span_id is not None and log.log_record.span_id == 0
        assert log.log_record.trace_id is not None and log.log_record.trace_id == 0
        assert (
            log.log_record.body == "This is a test_step"
            or log.log_record.body == "This is a test_workflow"
        )
        assert log.log_record.attributes.get("traceId") is None

    spans = exporter.get_finished_spans()

    # No spans should be created since OTLP is disabled
    assert len(spans) == 0
