import os
from typing import TYPE_CHECKING, Optional

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
from opentelemetry.semconv.resource import ResourceAttributes
from opentelemetry.trace import Span

from dbos._utils import GlobalParams

from ._dbos_config import ConfigFile

if TYPE_CHECKING:
    from ._context import TracedAttributes


class DBOSTracer:

    otlp_attributes: dict[str, str] = {}

    def __init__(self) -> None:
        self.app_id = os.environ.get("DBOS__APPID", None)
        self.provider: Optional[TracerProvider] = None
        self.disable_otlp: bool = False

    def config(self, config: ConfigFile) -> None:
        self.otlp_attributes = config.get("telemetry", {}).get("otlp_attributes", {})  # type: ignore
        self.disable_otlp = config.get("telemetry", {}).get("disable_otlp", False)  # type: ignore
        if not self.disable_otlp and not isinstance(
            trace.get_tracer_provider(), TracerProvider
        ):
            resource = Resource(
                attributes={
                    ResourceAttributes.SERVICE_NAME: config["name"],
                }
            )

            provider = TracerProvider(resource=resource)
            if os.environ.get("DBOS__CONSOLE_TRACES", None) is not None:
                processor = BatchSpanProcessor(ConsoleSpanExporter())
                provider.add_span_processor(processor)
            otlp_traces_endpoints = (
                config.get("telemetry", {}).get("OTLPExporter", {}).get("tracesEndpoint")  # type: ignore
            )
            if otlp_traces_endpoints:
                for e in otlp_traces_endpoints:
                    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=e))
                    provider.add_span_processor(processor)
            trace.set_tracer_provider(provider)

    def set_provider(self, provider: Optional[TracerProvider]) -> None:
        self.provider = provider

    def start_span(
        self, attributes: "TracedAttributes", parent: Optional[Span] = None
    ) -> Span:
        tracer = (
            self.provider.get_tracer("dbos-tracer")
            if self.provider is not None
            else trace.get_tracer("dbos-tracer")
        )
        context = trace.set_span_in_context(parent) if parent else None
        span: Span = tracer.start_span(name=attributes["name"], context=context)
        attributes["applicationID"] = self.app_id
        attributes["applicationVersion"] = GlobalParams.app_version
        attributes["executorID"] = GlobalParams.executor_id
        for k, v in attributes.items():
            if k != "name" and v is not None and isinstance(v, (str, bool, int, float)):
                span.set_attribute(k, v)
        for k, v in self.otlp_attributes.items():
            span.set_attribute(k, v)
        return span

    def end_span(self, span: Span) -> None:
        span.end()


dbos_tracer = DBOSTracer()
