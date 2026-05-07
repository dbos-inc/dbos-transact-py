"""Tests for the `otel_attribute_format` configuration flag.

When `otel_attribute_format="legacy"` (the default), DBOS span attributes
use their original camelCase names. When `otel_attribute_format="semconv"`,
they're emitted under the OTel-style `dbos.*` namespace.
"""

from dbos import DBOS, DBOSConfig
from dbos._tracer import _LEGACY_TO_SEMCONV, DBOSTracer
from tests.conftest import TestOtelType


def test_resolve_attribute_name_legacy_passthrough() -> None:
    tracer = DBOSTracer()
    tracer.otel_attribute_format = "legacy"
    for legacy_name in _LEGACY_TO_SEMCONV:
        assert tracer._resolve_attribute_name(legacy_name) == legacy_name


def test_resolve_attribute_name_semconv_remap() -> None:
    tracer = DBOSTracer()
    tracer.otel_attribute_format = "semconv"
    for legacy_name, semconv_name in _LEGACY_TO_SEMCONV.items():
        assert tracer._resolve_attribute_name(legacy_name) == semconv_name


def test_resolve_attribute_name_unknown_passes_through() -> None:
    """Unknown attribute names are unaffected by the format flag."""
    for fmt in ("legacy", "semconv"):
        tracer = DBOSTracer()
        tracer.otel_attribute_format = fmt  # type: ignore[assignment]
        assert tracer._resolve_attribute_name("custom.user.attribute") == (
            "custom.user.attribute"
        )


def test_legacy_attributes_default_emitted_on_span(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """With no override, spans carry the legacy attribute names."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    DBOS(config=config)
    DBOS.launch()

    @DBOS.workflow()
    def w() -> None:
        pass

    exporter.clear()
    w()

    spans = exporter.get_finished_spans()
    assert spans, "expected at least one span"
    attrs = spans[-1].attributes or {}
    assert "applicationVersion" in attrs
    assert "executorID" in attrs
    assert "dbos.application.version" not in attrs
    assert "dbos.executor.id" not in attrs


def test_semconv_attributes_emitted_on_span(
    config: DBOSConfig, setup_in_memory_otlp_collector: TestOtelType
) -> None:
    """With `otel_attribute_format="semconv"`, spans carry the dbos.* names
    in place of the legacy ones."""
    exporter, _, _ = setup_in_memory_otlp_collector

    DBOS.destroy(destroy_registry=True)
    config["enable_otlp"] = True
    config["otel_attribute_format"] = "semconv"
    DBOS(config=config)
    DBOS.launch()

    @DBOS.workflow()
    def w() -> None:
        pass

    exporter.clear()
    w()

    spans = exporter.get_finished_spans()
    assert spans, "expected at least one span"
    attrs = spans[-1].attributes or {}
    assert "dbos.application.version" in attrs
    assert "dbos.executor.id" in attrs
    # Legacy names must not also appear when semconv is selected.
    assert "applicationVersion" not in attrs
    assert "executorID" not in attrs
