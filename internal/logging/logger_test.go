package logging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"testing"
)

func TestCorrelatingLogger(t *testing.T) {
	var buf bytes.Buffer
	handler := &CorrelatingHandler{
		Handler: slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}),
	}
	logger := slog.New(handler)

	ctx := context.Background()
	ctx = WithTraceID(ctx, "trace-xyz")
	ctx = WithTenantID(ctx, "tenant-1")
	ctx = WithTaskID(ctx, "task-99")
	ctx = WithWorkerID(ctx, "worker-alpha")

	logger.InfoContext(ctx, "task dispatched")

	var record map[string]interface{}
	if err := json.Unmarshal(buf.Bytes(), &record); err != nil {
		t.Fatalf("failed to unmarshal log output: %v", err)
	}

	if record["trace_id"] != "trace-xyz" {
		t.Errorf("expected trace-xyz, got %v", record["trace_id"])
	}
	if record["tenant_id"] != "tenant-1" {
		t.Errorf("expected tenant-1, got %v", record["tenant_id"])
	}
	if record["task_id"] != "task-99" {
		t.Errorf("expected task-99, got %v", record["task_id"])
	}
	if record["worker_id"] != "worker-alpha" {
		t.Errorf("expected worker-alpha, got %v", record["worker_id"])
	}
}
