from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.trace import Span


class DBOSTracer:

    def __init__(self) -> None:
        provider = TracerProvider()
        processor = BatchSpanProcessor(ConsoleSpanExporter())
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

    def start_span(self) -> Span:
        tracer = trace.get_tracer("dbos-tracer")
        span: Span = tracer.start_span(name="steve")
        return span

    def end_span(self, span: Span) -> None:
        span.end()


dbos_tracer = DBOSTracer()
