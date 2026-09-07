package telemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

type mockStatusProvider struct {
	ready  bool
	status ClusterStatus
}

func (m *mockStatusProvider) GetClusterStatus() ClusterStatus {
	return m.status
}

func (m *mockStatusProvider) IsReady() bool {
	return m.ready
}

func TestTelemetryHTTPServer(t *testing.T) {
	metrics := NewMetrics()
	provider := &mockStatusProvider{
		ready: true,
		status: ClusterStatus{
			NodeID:        "coord-test-1",
			Role:          "Leader",
			LeaderID:      "coord-test-1",
			Term:          3,
			CommitIndex:   150,
			AppliedIndex:  150,
			ActiveWorkers: 4,
			QueueDepth:    12,
			StartTime:     time.Now().Add(-10 * time.Minute),
			UptimeSeconds: 600.0,
		},
	}

	srv := NewHTTPServer("127.0.0.1:0", metrics, provider)
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start http server: %v", err)
	}
	defer func() {
		_ = srv.Stop(context.Background())
	}()

	client := &http.Client{Timeout: 2 * time.Second}
	baseURL := "http://" + srv.Addr()

	// 1. Test /healthz
	resp, err := client.Get(baseURL + "/healthz")
	if err != nil {
		t.Fatalf("healthz failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var healthBody map[string]interface{}
	_ = json.NewDecoder(resp.Body).Decode(&healthBody)
	_ = resp.Body.Close()
	if healthBody["status"] != "healthy" {
		t.Errorf("expected healthy, got %v", healthBody["status"])
	}

	// 2. Test /readyz (ready = true)
	resp, err = client.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("readyz failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// Test /readyz (ready = false)
	provider.ready = false
	resp, err = client.Get(baseURL + "/readyz")
	if err != nil {
		t.Fatalf("readyz (not ready) failed: %v", err)
	}
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("expected 503, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 3. Test /status
	resp, err = client.Get(baseURL + "/status")
	if err != nil {
		t.Fatalf("status failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	var st ClusterStatus
	_ = json.NewDecoder(resp.Body).Decode(&st)
	_ = resp.Body.Close()
	if st.NodeID != "coord-test-1" || st.Role != "Leader" || st.Term != 3 {
		t.Errorf("unexpected status payload: %+v", st)
	}

	// 4. Test /metrics
	// Record some sample metrics
	metrics.TasksSubmittedTotal.WithLabelValues("tenant-a").Inc()
	metrics.RaftCurrentTerm.WithLabelValues("coord-test-1").Set(3)
	metrics.SchedulerQueueDepth.WithLabelValues("tenant-a").Set(5)

	resp, err = client.Get(baseURL + "/metrics")
	if err != nil {
		t.Fatalf("metrics failed: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200, got %d", resp.StatusCode)
	}
	bodyBytes, err := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if err != nil {
		t.Fatalf("failed to read metrics body: %v", err)
	}
	bodyStr := string(bodyBytes)

	if !strings.Contains(bodyStr, "tasks_submitted_total") {
		t.Errorf("expected tasks_submitted_total in metrics output")
	}
	if !strings.Contains(bodyStr, "raft_current_term") {
		t.Errorf("expected raft_current_term in metrics output")
	}
}

func TestOpenTelemetryTracing(t *testing.T) {
	tp, err := InitTracer("test-scheduler", "node-1", true)
	if err != nil {
		t.Fatalf("InitTracer failed: %v", err)
	}
	defer func() {
		_ = tp.Shutdown(context.Background())
	}()

	ctx := context.Background()
	ctx, span := StartSpan(ctx, "test-task-schedule")
	span.SetAttributes(
		AttrTaskID.String("task-123"),
		AttrTenantID.String("tenant-xyz"),
		AttrRaftTerm.Int64(2),
		AttrRaftIndex.Int64(45),
	)
	span.End()
}
