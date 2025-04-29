from typing import Tuple

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk import trace as tracesdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dbos import DBOS
from dbos._tracer import dbos_tracer
from dbos._utils import GlobalParams


def test_spans(dbos: DBOS) -> None:

    @DBOS.workflow()
    def test_workflow() -> None:
        test_step()
        current_span = DBOS.span
        subspan = DBOS.tracer.start_span({"name": "a new span"}, parent=current_span)
        subspan.add_event("greeting_event", {"name": "a new event"})
        DBOS.tracer.end_span(subspan)

    @DBOS.step()
    def test_step() -> None:
        return

    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    dbos_tracer.set_provider(provider)

    test_workflow()
    test_step()

    spans = exporter.get_finished_spans()

    assert len(spans) == 4

    for span in spans:
        assert span.attributes is not None
        assert span.attributes["applicationVersion"] == GlobalParams.app_version
        assert span.attributes["executorID"] == GlobalParams.executor_id
        assert span.context is not None

    assert spans[0].name == test_step.__name__
    assert spans[1].name == "a new span"
    assert spans[2].name == test_workflow.__name__
    assert spans[3].name == test_step.__name__

    assert spans[0].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[1].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[2].parent == None
    assert spans[3].parent == None


@pytest.mark.asyncio
async def test_spans_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def test_workflow() -> None:
        await test_step()
        current_span = DBOS.span
        subspan = DBOS.tracer.start_span({"name": "a new span"}, parent=current_span)
        subspan.add_event("greeting_event", {"name": "a new event"})
        DBOS.tracer.end_span(subspan)

    @DBOS.step()
    async def test_step() -> None:
        return

    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    dbos_tracer.set_provider(provider)

    await test_workflow()
    await test_step()

    spans = exporter.get_finished_spans()

    assert len(spans) == 4

    for span in spans:
        assert span.attributes is not None
        assert span.attributes["applicationVersion"] == GlobalParams.app_version
        assert span.attributes["executorID"] == GlobalParams.executor_id
        assert span.context is not None

    assert spans[0].name == test_step.__name__
    assert spans[1].name == "a new span"
    assert spans[2].name == test_workflow.__name__
    assert spans[3].name == test_step.__name__

    assert spans[0].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[1].parent.span_id == spans[2].context.span_id  # type: ignore
    assert spans[2].parent == None
    assert spans[3].parent == None


def test_temp_wf_fastapi(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    dbos, app = dbos_fastapi

    @app.get("/step")
    @DBOS.step()
    def test_step_endpoint() -> str:
        return "test"

    exporter = InMemorySpanExporter()
    span_processor = SimpleSpanProcessor(exporter)
    provider = tracesdk.TracerProvider()
    provider.add_span_processor(span_processor)
    dbos_tracer.set_provider(provider)

    client = TestClient(app)
    response = client.get("/step")
    assert response.status_code == 200
    assert response.text == '"test"'

    spans = exporter.get_finished_spans()

    assert len(spans) == 2

    for span in spans:
        assert span.attributes is not None
        assert span.attributes["applicationVersion"] == GlobalParams.app_version
        assert span.context is not None

    assert spans[0].name == test_step_endpoint.__name__
    assert spans[1].name == "/step"

    assert spans[0].parent.span_id == spans[1].context.span_id  # type:ignore
    assert spans[1].parent == None
