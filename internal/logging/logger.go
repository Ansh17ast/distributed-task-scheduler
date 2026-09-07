package logging

import (
	"context"
	"log/slog"
	"os"
)

type contextKey string

const (
	keyTraceID  contextKey = "trace_id"
	keyTenantID contextKey = "tenant_id"
	keyTaskID   contextKey = "task_id"
	keyWorkerID contextKey = "worker_id"
	keyDAGID    contextKey = "dag_id"
)

// WithTraceID injects a trace ID into context.
func WithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, keyTraceID, traceID)
}

// WithTenantID injects a tenant ID into context.
func WithTenantID(ctx context.Context, tenantID string) context.Context {
	return context.WithValue(ctx, keyTenantID, tenantID)
}

// WithTaskID injects a task ID into context.
func WithTaskID(ctx context.Context, taskID string) context.Context {
	return context.WithValue(ctx, keyTaskID, taskID)
}

// WithWorkerID injects a worker ID into context.
func WithWorkerID(ctx context.Context, workerID string) context.Context {
	return context.WithValue(ctx, keyWorkerID, workerID)
}

// WithDAGID injects a DAG ID into context.
func WithDAGID(ctx context.Context, dagID string) context.Context {
	return context.WithValue(ctx, keyDAGID, dagID)
}

// CorrelatingHandler wraps a slog.Handler to automatically attach context correlation IDs.
type CorrelatingHandler struct {
	slog.Handler
}

// Handle appends context values (trace_id, tenant_id, task_id, worker_id) to the log record.
func (h *CorrelatingHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		if v, ok := ctx.Value(keyTraceID).(string); ok && v != "" {
			r.AddAttrs(slog.String("trace_id", v))
		}
		if v, ok := ctx.Value(keyTenantID).(string); ok && v != "" {
			r.AddAttrs(slog.String("tenant_id", v))
		}
		if v, ok := ctx.Value(keyDAGID).(string); ok && v != "" {
			r.AddAttrs(slog.String("dag_id", v))
		}
		if v, ok := ctx.Value(keyTaskID).(string); ok && v != "" {
			r.AddAttrs(slog.String("task_id", v))
		}
		if v, ok := ctx.Value(keyWorkerID).(string); ok && v != "" {
			r.AddAttrs(slog.String("worker_id", v))
		}
	}
	return h.Handler.Handle(ctx, r)
}

// NewLogger creates a structured slog logger with correlation ID support.
func NewLogger(level string) *slog.Logger {
	var logLevel slog.Level
	switch level {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "WARN":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
	}

	baseHandler := slog.NewJSONHandler(os.Stdout, opts)
	handler := &CorrelatingHandler{Handler: baseHandler}
	return slog.New(handler)
}
