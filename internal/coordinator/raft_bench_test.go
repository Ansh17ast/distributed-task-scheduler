package coordinator

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/domain"
)

// Helper to compute p50, p95, p99 percentiles from durations
func computePercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sort.Slice(latencies, func(i, j int) bool {
		return latencies[i] < latencies[j]
	})
	p50 = latencies[len(latencies)*50/100]
	p95 = latencies[len(latencies)*95/100]
	p99 = latencies[len(latencies)*99/100]
	return
}

// TestBenchmark_DistributedMetrics runs real distributed cluster benchmarks
// measuring p50, p95, p99 across the 5 required dimensions.
func TestBenchmark_DistributedMetrics(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	t.Log("=================================================================")
	t.Log("PHASE 7 REAL DISTRIBUTED BENCHMARKS (3-Node Cluster, Quorum = 2)")
	t.Log("=================================================================")

	// -----------------------------------------------------------------
	// 1. Raft Command Commit Latency (Direct Raft.Apply)
	// -----------------------------------------------------------------
	const numCommands = 300
	commitLatencies := make([]time.Duration, 0, numCommands)

	for i := 0; i < numCommands; i++ {
		cmd := &consensus.Command{
			Op: consensus.OpSetTenantQuota,
			TenantQuota: &domain.TenantQuota{
				TenantID:       fmt.Sprintf("tenant-bench-%d", i),
				MaxConcurrency: int32(10 + i),
				MaxQueueDepth:  100,
			},
			Timestamp: time.Now(),
		}
		start := time.Now()
		_, err := h.nodes[leaderIdx].Apply(cmd, 3*time.Second)
		if err != nil {
			t.Fatalf("failed applying command %d: %v", i, err)
		}
		commitLatencies = append(commitLatencies, time.Since(start))
	}
	c50, c95, c99 := computePercentiles(commitLatencies)
	t.Logf("[1] Raft Command Commit Latency (%d samples):", numCommands)
	t.Logf("    p50: %v | p95: %v | p99: %v", c50, c95, c99)

	// -----------------------------------------------------------------
	// 2. End-to-End Task Submission Latency (gRPC Client -> Raft -> FSM)
	// -----------------------------------------------------------------
	// Configure high concurrency quota for tenant-bench
	_, _ = h.nodes[leaderIdx].Apply(&consensus.Command{
		Op: consensus.OpSetTenantQuota,
		TenantQuota: &domain.TenantQuota{
			TenantID:       "tenant-bench",
			MaxConcurrency: 1000,
			MaxQueueDepth:  1000,
		},
		Timestamp: time.Now(),
	}, 3*time.Second)

	const numSubmissions = 300
	submitLatencies := make([]time.Duration, 0, numSubmissions)

	for i := 0; i < numSubmissions; i++ {
		taskID := fmt.Sprintf("bench-task-%d", i)
		req := &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{
				Id:         taskID,
				TenantId:   "tenant-bench",
				Priority:   50,
				MaxRetries: 3,
			},
		}
		start := time.Now()
		_, err := h.clients[leaderIdx].SubmitTask(ctx, req)
		if err != nil {
			t.Fatalf("failed submitting task %d: %v", i, err)
		}
		submitLatencies = append(submitLatencies, time.Since(start))
	}
	s50, s95, s99 := computePercentiles(submitLatencies)
	t.Logf("[2] End-to-End Task Submission Latency (%d samples via gRPC+Raft):", numSubmissions)
	t.Logf("    p50: %v | p95: %v | p99: %v", s50, s95, s99)

	// -----------------------------------------------------------------
	// 3. Worker Assignment Latency (gRPC PullTask -> Raft Lease Grant -> FSM)
	// -----------------------------------------------------------------
	// Register bench worker
	_, err := h.wClients[leaderIdx].RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "bench-worker",
		SessionId: "bench-session-1",
		MaxSlots:  500,
	})
	if err != nil {
		t.Fatalf("failed registering bench worker: %v", err)
	}

	const numAssignments = 200
	pullLatencies := make([]time.Duration, 0, numAssignments)

	for i := 0; i < numAssignments; i++ {
		req := &schedulerv1.PullTaskRequest{
			WorkerId:       "bench-worker",
			SessionId:      "bench-session-1",
			AvailableSlots: 10,
		}
		start := time.Now()
		resp, err := h.wClients[leaderIdx].PullTask(ctx, req)
		if err != nil {
			t.Fatalf("failed pulling task %d: %v", i, err)
		}
		if resp.GetAssignment() == nil {
			t.Fatalf("expected assignment %d, got nil", i)
		}
		pullLatencies = append(pullLatencies, time.Since(start))
	}
	p50, p95, p99 := computePercentiles(pullLatencies)
	t.Logf("[3] Worker Assignment Latency (%d samples via gRPC PullTask+Raft):", numAssignments)
	t.Logf("    p50: %v | p95: %v | p99: %v", p50, p95, p99)

	// -----------------------------------------------------------------
	// 4. Replicated Command Throughput
	// -----------------------------------------------------------------
	throughputWindow := 1500 * time.Millisecond
	startTime := time.Now()
	opCount := 0
	for time.Since(startTime) < throughputWindow {
		cmd := &consensus.Command{
			Op: consensus.OpSetTenantQuota,
			TenantQuota: &domain.TenantQuota{
				TenantID:       "tenant-tp",
				MaxConcurrency: int32(opCount % 100),
				MaxQueueDepth:  100,
			},
			Timestamp: time.Now(),
		}
		if _, err := h.nodes[leaderIdx].Apply(cmd, 3*time.Second); err != nil {
			break
		}
		opCount++
	}
	elapsed := time.Since(startTime)
	throughput := float64(opCount) / elapsed.Seconds()
	t.Logf("[4] Replicated Command Throughput:")
	t.Logf("    Executed %d replicated commits in %v -> %.2f ops/sec", opCount, elapsed, throughput)

	// -----------------------------------------------------------------
	// 5. Leader Election / Failover Time
	// -----------------------------------------------------------------
	oldLeaderIdx := leaderIdx
	failoverStart := time.Now()

	// Kill leader node and disconnect its transport
	_ = h.nodes[oldLeaderIdx].Shutdown()
	h.servers[oldLeaderIdx].Stop()
	h.transports[oldLeaderIdx].DisconnectAll()
	h.nodes[oldLeaderIdx] = nil

	newLeaderIdx := -1
	for time.Since(failoverStart) < 5*time.Second {
		newLeaderIdx = h.findLeaderIndex()
		if newLeaderIdx >= 0 && newLeaderIdx != oldLeaderIdx {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	failoverDuration := time.Since(failoverStart)
	if newLeaderIdx < 0 {
		t.Fatalf("new leader was not elected within failover timeout")
	}
	t.Logf("[5] Leader Election / Failover Time:")
	t.Logf("    Failover detected and new leader (node %d) elected in %v", newLeaderIdx+1, failoverDuration)
	t.Log("=================================================================")
}
