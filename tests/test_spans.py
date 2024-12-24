from typing import Tuple

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from opentelemetry.sdk import trace as tracesdk
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from dbos import DBOS
from dbos._tracer import dbos_tracer


def test_spans(dbos: DBOS) -> None:

    @DBOS.workflow()
    def test_workflow() -> None:
        test_step()

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

    assert len(spans) == 3

    for span in spans:
        assert span.attributes is not None
        assert span.context is not None

    assert spans[0].name == test_step.__name__
    assert spans[1].name == test_workflow.__name__
    assert spans[2].name == test_step.__name__

    assert spans[0].parent.span_id == spans[1].context.span_id  # type: ignore
    assert spans[1].parent == None
    assert spans[2].parent == None


@pytest.mark.asyncio
async def test_spans_async(dbos: DBOS) -> None:

    @DBOS.workflow()
    async def test_workflow() -> None:
        await test_step()

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

    assert len(spans) == 3

    for span in spans:
        assert span.attributes is not None
        assert span.context is not None

    assert spans[0].name == test_step.__name__
    assert spans[1].name == test_workflow.__name__
    assert spans[2].name == test_step.__name__

    assert spans[0].parent.span_id == spans[1].context.span_id  # type: ignore
    assert spans[1].parent == None
    assert spans[2].parent == None


def test_temp_wf_fastapi(dbos_fastapi: Tuple[DBOS, FastAPI]) -> None:
    _, app = dbos_fastapi

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
        assert span.context is not None

    assert spans[0].name == test_step_endpoint.__name__
    assert spans[1].name == "/step"

    assert spans[0].parent.span_id == spans[1].context.span_id  # type:ignore
    assert spans[1].parent == None
