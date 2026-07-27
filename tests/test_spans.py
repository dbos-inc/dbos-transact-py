from dataclasses import dataclass, field
from typing import Optional

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from inline_snapshot import snapshot
from opentelemetry import context as otel_context
from opentelemetry import trace
from opentelemetry.trace.span import format_trace_id

from dbos import (
    DBOS,
    DBOSClient,
    DBOSConfig,
    PropagateOtelContext,
    SetWorkflowAttributes,
)
from dbos._dbos import WorkflowHandle
from dbos._utils import GlobalParams
from tests.conftest import TestOtelType, set_workflow_status


@dataclass
class BasicSpan:
    content: str
    children: list["BasicSpan"] = field(default_factory=list)
    parent_id: Optional[int] = field(repr=False, compare=False, default=None)


def test_spans(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    exporter, log_processor, log_exporter = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["otlp_attributes"] = {"foo": "bar"}
    config["enable_otlp"] = True
    DBOS(config=config)
    DBOS.launch()

    provider = trace.get_tracer_provider()
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

    log_processor.force_flush(timeout_millis=5000)
    log_exporter.clear()  # Clear any logs generated during setup
    exporter.clear()

    test_workflow()

    expected_log_bodies = {"This is a test_step", "This is a test_workflow"}

    log_processor.force_flush(timeout_millis=5000)
    logs = [
        l
        for l in log_exporter.get_finished_logs()
        if l.log_record.body in expected_log_bodies
    ]
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
        assert "queueName" not in span.attributes
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
async def test_spans_async(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    exporter, log_processor, log_exporter = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["otlp_attributes"] = {"foo": "bar"}
    config["enable_otlp"] = True
    DBOS(config=config)
    DBOS.launch()

    provider = trace.get_tracer_provider()
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

    log_processor.force_flush(timeout_millis=5000)
    log_exporter.clear()  # Clear any logs generated during setup
    exporter.clear()

    expected_log_bodies = {"This is a test_step", "This is a test_workflow"}

    await test_workflow()

    log_processor.force_flush(timeout_millis=5000)
    logs = [
        l
        for l in log_exporter.get_finished_logs()
        if l.log_record.body in expected_log_bodies
    ]
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


def test_wf_fastapi(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    exporter, log_processor, log_exporter = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    app = FastAPI()
    dbos = DBOS(fastapi=app, config=config)
    DBOS.launch()

    @app.get("/wf")
    @DBOS.workflow()
    def test_workflow_endpoint() -> str:
        dbos.logger.info("This is a test_workflow_endpoint")
        return "test"

    log_processor.force_flush(timeout_millis=5000)
    log_exporter.clear()  # Clear any logs generated during setup
    exporter.clear()

    client = TestClient(app)
    response = client.get("/wf")
    assert response.status_code == 200
    assert response.text == '"test"'

    expected_log_bodies = {"This is a test_workflow_endpoint"}

    log_processor.force_flush(timeout_millis=5000)
    logs = [
        l
        for l in log_exporter.get_finished_logs()
        if l.log_record.body in expected_log_bodies
    ]

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


def test_disable_otlp_no_spans(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    exporter, log_processor, log_exporter = setup_in_memory_otlp_collector

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

    log_processor.force_flush(timeout_millis=5000)
    log_exporter.clear()  # Clear any logs generated during setup
    exporter.clear()

    expected_log_bodies = {"This is a test_step", "This is a test_workflow"}

    test_workflow()

    log_processor.force_flush(timeout_millis=5000)
    logs = [
        l
        for l in log_exporter.get_finished_logs()
        if l.log_record.body in expected_log_bodies
    ]
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
        assert log.log_record.attributes.get("traceId") is None

    spans = exporter.get_finished_spans()

    # No spans should be created since OTLP is disabled
    assert len(spans) == 0


def test_queue_span_has_queue_name(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    exporter, log_processor, log_exporter = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    DBOS(config=config)

    @DBOS.workflow()
    def queued_workflow() -> str:
        return "queued_result"

    DBOS.launch()
    DBOS.register_queue("test_queue")

    log_processor.force_flush(timeout_millis=5000)
    log_exporter.clear()
    exporter.clear()

    handle = DBOS.enqueue_workflow("test_queue", queued_workflow)
    result = handle.get_result()
    assert result == "queued_result"

    spans = exporter.get_finished_spans()
    workflow_spans = [s for s in spans if s.name == queued_workflow.__qualname__]
    assert len(workflow_spans) == 1
    span = workflow_spans[0]
    assert span.attributes is not None
    assert span.attributes["queueName"] == "test_queue"
    assert span.attributes["operationType"] == "workflow"
    assert span.attributes["applicationVersion"] == DBOS.application_version
    assert span.attributes["executorID"] == GlobalParams.executor_id
    assert span.attributes["operationUUID"] == handle.workflow_id


def test_start_workflow_inherits_caller_trace(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """DBOS.start_workflow runs the workflow on an executor thread, which does not
    inherit contextvars. Its span must still parent to the span active at the call
    site, matching a direct call and DBOS.start_workflow_async."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    DBOS(config=config)

    @DBOS.step()
    def a_step() -> None:
        pass

    @DBOS.workflow()
    def a_workflow() -> str:
        a_step()
        return "done"

    DBOS.launch()
    exporter.clear()

    my_tracer = trace.get_tracer_provider().get_tracer("dbos")
    with my_tracer.start_as_current_span(
        "caller"
    ) as caller:  # pyright: ignore[reportAttributeAccessIssue]
        caller_ctx = caller.get_span_context()
        handle = DBOS.start_workflow(a_workflow)
        assert handle.get_result() == "done"

    spans = exporter.get_finished_spans()
    workflow_spans = [s for s in spans if s.name == a_workflow.__qualname__]
    assert len(workflow_spans) == 1
    workflow_span = workflow_spans[0]
    assert workflow_span.context is not None
    assert workflow_span.context.trace_id == caller_ctx.trace_id
    assert workflow_span.parent is not None
    assert workflow_span.parent.span_id == caller_ctx.span_id

    # Steps still parent to the workflow span, so the whole chain is on one trace.
    step_spans = [s for s in spans if s.name == a_step.__qualname__]
    assert len(step_spans) == 1
    assert step_spans[0].parent is not None
    assert step_spans[0].parent.span_id == workflow_span.context.span_id


def test_start_workflow_without_caller_span_is_root(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """With no span active at the call site, the workflow span still roots its own trace."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    DBOS(config=config)

    @DBOS.workflow()
    def a_workflow() -> str:
        return "done"

    DBOS.launch()
    exporter.clear()

    handle = DBOS.start_workflow(a_workflow)
    assert handle.get_result() == "done"

    workflow_spans = [
        s for s in exporter.get_finished_spans() if s.name == a_workflow.__qualname__
    ]
    assert len(workflow_spans) == 1
    assert workflow_spans[0].parent is None


def test_propagate_otel_context_queued_workflow_joins_caller_trace(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """A queued workflow runs after the enqueuing context is gone, so it only joins the
    caller's trace if PropagateOtelContext recorded that trace with the workflow."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    DBOS(config=config)

    @DBOS.step()
    def a_step() -> None:
        pass

    @DBOS.workflow()
    def a_workflow() -> str:
        a_step()
        return "done"

    DBOS.launch()
    DBOS.register_queue("otel_queue")
    exporter.clear()

    my_tracer = trace.get_tracer_provider().get_tracer("dbos")
    with my_tracer.start_as_current_span(
        "caller"
    ) as caller:  # pyright: ignore[reportAttributeAccessIssue]
        caller_ctx = caller.get_span_context()
        with PropagateOtelContext():
            traced = DBOS.enqueue_workflow("otel_queue", a_workflow)
        untraced = DBOS.enqueue_workflow("otel_queue", a_workflow)
    assert traced.get_result() == "done"
    assert untraced.get_result() == "done"

    spans = exporter.get_finished_spans()
    by_workflow_id = {
        s.attributes["operationUUID"]: s
        for s in spans
        if s.name == a_workflow.__qualname__ and s.attributes
    }

    traced_span = by_workflow_id[traced.workflow_id]
    assert traced_span.context is not None
    assert traced_span.context.trace_id == caller_ctx.trace_id
    assert traced_span.parent is not None
    assert traced_span.parent.span_id == caller_ctx.span_id
    # The workflow span keeps its DBOS attributes on the caller's trace.
    assert traced_span.attributes is not None
    assert traced_span.attributes["queueName"] == "otel_queue"

    # Steps still parent to the workflow span rather than to the restored context.
    step_spans = [
        s
        for s in spans
        if s.name == a_step.__qualname__
        and s.parent
        and s.parent.span_id == traced_span.context.span_id
    ]
    assert len(step_spans) == 1

    # Without PropagateOtelContext, a queued workflow still roots its own trace.
    untraced_span = by_workflow_id[untraced.workflow_id]
    assert untraced_span.context is not None
    assert untraced_span.parent is None
    assert untraced_span.context.trace_id != caller_ctx.trace_id


@pytest.mark.asyncio
async def test_propagate_otel_context_queued_async_workflow_joins_caller_trace(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """Async workflows run through _execute_workflow_async, a separate call site from
    the sync executor path, so the restored context needs its own coverage there."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    DBOS(config=config)

    @DBOS.step()
    async def a_step() -> None:
        pass

    @DBOS.workflow()
    async def a_workflow() -> str:
        await a_step()
        return "done"

    DBOS.launch()
    await DBOS.register_queue_async("async_otel_queue")
    exporter.clear()

    my_tracer = trace.get_tracer_provider().get_tracer("dbos")
    with my_tracer.start_as_current_span(
        "caller"
    ) as caller:  # pyright: ignore[reportAttributeAccessIssue]
        caller_ctx = caller.get_span_context()
        with PropagateOtelContext():
            traced = await DBOS.enqueue_workflow_async("async_otel_queue", a_workflow)
        untraced = await DBOS.enqueue_workflow_async("async_otel_queue", a_workflow)
    assert (await traced.get_result()) == "done"
    assert (await untraced.get_result()) == "done"

    spans = exporter.get_finished_spans()
    by_workflow_id = {
        s.attributes["operationUUID"]: s
        for s in spans
        if s.name == a_workflow.__qualname__ and s.attributes
    }

    traced_span = by_workflow_id[traced.workflow_id]
    assert traced_span.context is not None
    assert traced_span.context.trace_id == caller_ctx.trace_id
    assert traced_span.parent is not None
    assert traced_span.parent.span_id == caller_ctx.span_id

    # Async steps still parent to the workflow span.
    step_spans = [
        s
        for s in spans
        if s.name == a_step.__qualname__
        and s.parent
        and s.parent.span_id == traced_span.context.span_id
    ]
    assert len(step_spans) == 1

    untraced_span = by_workflow_id[untraced.workflow_id]
    assert untraced_span.context is not None
    assert untraced_span.parent is None
    assert untraced_span.context.trace_id != caller_ctx.trace_id


def test_propagate_otel_context_survives_recovery(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """A recovered workflow re-executes in a context with no ambient trace, so it must
    rebuild its parent from the carrier persisted with the workflow."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    dbos = DBOS(config=config)

    @DBOS.workflow()
    def a_workflow() -> str:
        return "done"

    DBOS.launch()
    exporter.clear()

    my_tracer = trace.get_tracer_provider().get_tracer("dbos")
    with my_tracer.start_as_current_span(
        "caller"
    ) as caller:  # pyright: ignore[reportAttributeAccessIssue]
        caller_ctx = caller.get_span_context()
        with PropagateOtelContext():
            handle = DBOS.start_workflow(a_workflow)
    assert handle.get_result() == "done"

    exporter.clear()
    set_workflow_status(dbos._sys_db, handle.workflow_id, "PENDING")
    recovery_handles = DBOS._recover_pending_workflows()
    assert len(recovery_handles) == 1
    assert recovery_handles[0].get_result() == "done"

    recovered = [
        s for s in exporter.get_finished_spans() if s.name == a_workflow.__qualname__
    ]
    assert len(recovered) == 1
    assert recovered[0].context is not None
    assert recovered[0].context.trace_id == caller_ctx.trace_id
    assert recovered[0].parent is not None
    assert recovered[0].parent.span_id == caller_ctx.span_id


@pytest.mark.parametrize("otel_outermost", [True, False])
def test_propagate_otel_context_composes_with_workflow_attributes(
    dbos: DBOS,
    setup_in_memory_otlp_collector: TestOtelType,
    otel_outermost: bool,
) -> None:
    """The carrier lives outside workflow_attributes, so the two context managers can be
    nested in either order without one replacing the other's attributes."""

    @DBOS.workflow()
    def a_workflow() -> str:
        return "done"

    my_tracer = trace.get_tracer_provider().get_tracer("dbos")
    with my_tracer.start_as_current_span(
        "caller"
    ):  # pyright: ignore[reportAttributeAccessIssue]
        if otel_outermost:
            with PropagateOtelContext(), SetWorkflowAttributes({"customer": "acme"}):
                handle = DBOS.start_workflow(a_workflow)
        else:
            with SetWorkflowAttributes({"customer": "acme"}), PropagateOtelContext():
                handle = DBOS.start_workflow(a_workflow)
    assert handle.get_result() == "done"

    attributes = DBOS.retrieve_workflow(handle.workflow_id).get_status().attributes
    assert attributes is not None
    assert attributes["customer"] == "acme"
    assert "traceparent" in attributes["dbos.otelContext"]


def test_propagate_otel_context_without_active_span_is_noop(
    dbos: DBOS, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """With no span to propagate, nothing is recorded rather than an empty carrier."""

    @DBOS.workflow()
    def a_workflow() -> str:
        return "done"

    with PropagateOtelContext():
        handle = DBOS.start_workflow(a_workflow)
    assert handle.get_result() == "done"

    assert DBOS.retrieve_workflow(handle.workflow_id).get_status().attributes is None


def test_client_otel_context_joins_caller_trace(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """The EnqueueOptions.otel_context field is the client-side PropagateOtelContext: a
    workflow enqueued from outside the application still joins the caller's trace."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    DBOS(config=config)

    @DBOS.workflow()
    def client_workflow() -> str:
        return "done"

    DBOS.launch()
    DBOS.register_queue("client_otel_queue")
    exporter.clear()

    assert config["system_database_url"] is not None
    client = DBOSClient(system_database_url=config["system_database_url"])
    try:
        my_tracer = trace.get_tracer_provider().get_tracer("dbos")
        with my_tracer.start_as_current_span(
            "caller"
        ) as caller:  # pyright: ignore[reportAttributeAccessIssue]
            caller_ctx = caller.get_span_context()
            handle: WorkflowHandle[str] = client.enqueue(
                {
                    "queue_name": "client_otel_queue",
                    "workflow_name": client_workflow.__qualname__,
                    "otel_context": otel_context.get_current(),
                }
            )
        assert handle.get_result() == "done"
    finally:
        client.destroy()

    workflow_spans = [
        s
        for s in exporter.get_finished_spans()
        if s.name == client_workflow.__qualname__
    ]
    assert len(workflow_spans) == 1
    span = workflow_spans[0]
    assert span.context is not None
    assert span.context.trace_id == caller_ctx.trace_id
    assert span.parent is not None
    assert span.parent.span_id == caller_ctx.span_id
