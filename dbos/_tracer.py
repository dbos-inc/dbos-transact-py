import os
from typing import TYPE_CHECKING, Literal, Optional

if TYPE_CHECKING:
    from opentelemetry.trace import Span
    from opentelemetry.sdk.trace import TracerProvider

from dbos._utils import GlobalParams

from ._dbos_config import ConfigFile
from ._logger import dbos_logger

if TYPE_CHECKING:
    from ._context import TracedAttributes


# How span attribute names are emitted to OTLP.
#
# - "legacy"  : original DBOS names (e.g. operationUUID, applicationID).
#               Default for backward compatibility with existing dashboards
#               and the TypeScript Transact SDK.
# - "semconv" : OpenTelemetry-style names under the dbos.* namespace
#               (e.g. dbos.operation.uuid, dbos.application.id). Follows
#               https://opentelemetry.io/docs/specs/semconv/general/attribute-naming/
OtelAttributeFormat = Literal["legacy", "semconv"]


# Legacy DBOS attribute name -> OpenTelemetry semconv-style equivalent.
# Keys MUST match the field names in `TracedAttributes` in `_context.py`,
# plus the few attributes set ad-hoc (`responseCode`,
# `authenticatedUser*`).
_LEGACY_TO_SEMCONV: dict[str, str] = {
    "operationUUID": "dbos.operation.uuid",
    "operationType": "dbos.operation.type",
    "applicationID": "dbos.application.id",
    "applicationVersion": "dbos.application.version",
    "executorID": "dbos.executor.id",
    "queueName": "dbos.queue.name",
    "authenticatedUser": "dbos.user.name",
    "authenticatedUserRoles": "dbos.user.roles",
    "authenticatedUserAssumedRole": "dbos.user.assumed_role",
    "requestID": "dbos.request.id",
    "requestIP": "dbos.request.ip",
    "requestURL": "dbos.request.url",
    "requestMethod": "dbos.request.method",
    "responseCode": "dbos.response.status_code",
}


_DEFAULT_OTEL_ATTRIBUTE_FORMAT: OtelAttributeFormat = "legacy"


class DBOSTracer:

    otlp_attributes: dict[str, str] = {}

    def __init__(self) -> None:
        self.app_id = os.environ.get("DBOS__APPID", None)
        self.provider: Optional[TracerProvider] = None
        self.disable_otlp: bool = False
        self.otel_attribute_format: OtelAttributeFormat = (
            _DEFAULT_OTEL_ATTRIBUTE_FORMAT
        )

    def config(self, config: ConfigFile) -> None:
        self.otlp_attributes = config.get("telemetry", {}).get("otlp_attributes", {})  # type: ignore
        self.disable_otlp = config.get("telemetry", {}).get("disable_otlp", False)  # type: ignore
        self.otel_attribute_format = config.get("telemetry", {}).get(  # type: ignore
            "otel_attribute_format", _DEFAULT_OTEL_ATTRIBUTE_FORMAT
        )
        otlp_traces_endpoints = (
            config.get("telemetry", {}).get("OTLPExporter", {}).get("tracesEndpoint")  # type: ignore
        )

        if not self.disable_otlp:
            from opentelemetry import trace
            from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
                OTLPSpanExporter,
            )
            from opentelemetry.sdk.resources import Resource
            from opentelemetry.sdk.trace import TracerProvider
            from opentelemetry.sdk.trace.export import (
                BatchSpanProcessor,
                ConsoleSpanExporter,
            )
            from opentelemetry.semconv.attributes.service_attributes import SERVICE_NAME

            tracer_provider = trace.get_tracer_provider()

            # Only set up OTLP provider and exporter if endpoints are provided
            if otlp_traces_endpoints is not None and len(otlp_traces_endpoints) > 0:
                if isinstance(tracer_provider, trace.ProxyTracerProvider):
                    # Set a real TracerProvider if it was previously a ProxyTracerProvider
                    resource = Resource(
                        attributes={
                            SERVICE_NAME: config["name"],
                        }
                    )

                    tracer_provider = TracerProvider(resource=resource)
                    if os.environ.get("DBOS__CONSOLE_TRACES", None) is not None:
                        processor = BatchSpanProcessor(ConsoleSpanExporter())
                        tracer_provider.add_span_processor(processor)
                    trace.set_tracer_provider(tracer_provider)

                for e in otlp_traces_endpoints:
                    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint=e))
                    tracer_provider.add_span_processor(processor)  # type: ignore

            if isinstance(tracer_provider, trace.ProxyTracerProvider):
                dbos_logger.warning(
                    "OTLP is enabled but tracer provider not set, skipping trace exporter setup."
                )

    def set_provider(self, provider: "Optional[TracerProvider]") -> None:
        self.provider = provider

    def _resolve_attribute_name(self, key: str) -> str:
        """Map a legacy DBOS attribute name to the name that should be
        emitted on the span, per `otel_attribute_format`. Returns the
        original key for unknown attributes."""
        if self.otel_attribute_format == "semconv":
            return _LEGACY_TO_SEMCONV.get(key, key)
        return key

    def start_span(
        self, attributes: "TracedAttributes", parent: "Optional[Span]" = None
    ) -> "Span":
        from opentelemetry import trace

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
                span.set_attribute(self._resolve_attribute_name(k), v)
        for k, v in self.otlp_attributes.items():
            # User-provided custom attributes are passed through verbatim;
            # they don't go through the legacy/semconv mapping.
            span.set_attribute(k, v)
        return span

    def end_span(self, span: "Span") -> None:
        span.end()

    def get_current_span(self) -> "Optional[Span]":
        # Return the current active span if any. It might not be a DBOS span.
        from opentelemetry import trace

        span = trace.get_current_span()
        if span.get_span_context().is_valid:
            return span
        return None


dbos_tracer = DBOSTracer()
