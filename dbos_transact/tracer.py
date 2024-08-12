from typing import TYPE_CHECKING, Literal, Optional, Type, TypedDict

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Span

if TYPE_CHECKING:
    from .context import TracedAttributes


class DBOSTracer:

    def __init__(self) -> None:
        provider = TracerProvider()
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

    def start_span(self, attributes: "TracedAttributes") -> Span:
        tracer = trace.get_tracer("dbos-tracer")
        span: Span = tracer.start_span(name=attributes["name"])
        for k, v in attributes.items():
            if v is not None and isinstance(v, (str, bool, int, float)):
                span.set_attribute(k, v)
        return span

    def end_span(self, span: Span) -> None:
        span.end()


dbos_tracer = DBOSTracer()