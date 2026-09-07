package telemetry

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

const TracerName = "distributed-scheduler/telemetry"

// Trace attribute keys for correlation IDs (never placed in low-cardinality metrics)
const (
	AttrTaskID     = attribute.Key("task.id")
	AttrTenantID   = attribute.Key("tenant.id")
	AttrDAGID      = attribute.Key("dag.id")
	AttrWorkerID   = attribute.Key("worker.id")
	AttrSessionID  = attribute.Key("session.id")
	AttrLeaseEpoch = attribute.Key("lease.epoch")
	AttrRaftTerm   = attribute.Key("raft.term")
	AttrRaftIndex  = attribute.Key("raft.index")
	AttrNodeID     = attribute.Key("node.id")
	AttrStatus     = attribute.Key("status")
	AttrError      = attribute.Key("error.message")
)

// TracerProvider holds the active OpenTelemetry tracer provider.
type TracerProvider struct {
	tp *sdktrace.TracerProvider
}

// InitTracer initializes an OpenTelemetry TracerProvider.
// If enabled is false, it registers a no-op provider for zero runtime overhead.
func InitTracer(serviceName, nodeID string, enabled bool) (*TracerProvider, error) {
	if !enabled {
		otel.SetTracerProvider(noop.NewTracerProvider())
		return &TracerProvider{tp: nil}, nil
	}

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceNameKey.String(serviceName),
			attribute.String("node.id", nodeID),
		),
	)
	if err != nil {
		return nil, err
	}

	// In-memory / SDK tracer provider (can be extended with OTLP or stdout exporters)
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)

	otel.SetTracerProvider(tp)
	return &TracerProvider{tp: tp}, nil
}

// Shutdown flushes and terminates the tracer provider.
func (p *TracerProvider) Shutdown(ctx context.Context) error {
	if p.tp != nil {
		return p.tp.Shutdown(ctx)
	}
	return nil
}

// GetTracer returns the standard named tracer.
func GetTracer() trace.Tracer {
	return otel.GetTracerProvider().Tracer(TracerName)
}

// StartSpan creates a child span with common scheduling context attributes.
func StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return GetTracer().Start(ctx, spanName, opts...)
}
