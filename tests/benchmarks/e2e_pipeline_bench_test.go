package benchmarks

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/coordinator"
	"distributed-scheduler/internal/domain"
)

func computePercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]
	return
}

func TestE2EPipelineLatencyBreakdown(t *testing.T) {
	h, err := coordinator.NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create cluster harness: %v", err)
	}
	defer h.Close()

	leaderIdx, err := h.WaitLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader: %v", err)
	}

	client := h.Clients[leaderIdx]
	wClient := h.WClients[leaderIdx]
	leaderNode := h.Nodes[leaderIdx]
	leaderStore := h.Stores[leaderIdx]

	ctx := context.Background()

	// Register test worker
	regResp, err := wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "e2e-bench-worker-1",
		SessionId: "session-bench-1",
		Address:   "127.0.0.1:9099",
		MaxSlots:  20,
	})
	if err != nil || !regResp.Accepted {
		t.Fatalf("failed to register worker: %v", err)
	}

	const samples = 300
	submissionLatencies := make([]time.Duration, 0, samples)
	raftCommitLatencies := make([]time.Duration, 0, samples)
	schedulingLatencies := make([]time.Duration, 0, samples)
	assignmentLatencies := make([]time.Duration, 0, samples)
	executionLatencies := make([]time.Duration, 0, samples)
	totalLatencies := make([]time.Duration, 0, samples)

	for i := 0; i < samples; i++ {
		taskID := fmt.Sprintf("e2e-pipeline-task-%d", i)
		tenantID := fmt.Sprintf("tenant-%d", i%5)

		// --- STAGE 1: SUBMISSION LATENCY (gRPC client -> Leader coordinator) ---
		subStart := time.Now()
		subResp, err := client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{
				Id:             taskID,
				TenantId:       tenantID,
				Priority:       int32(i % 10),
				TimeoutSeconds: 30,
			},
		})
		subDuration := time.Since(subStart)
		if err != nil || subResp.TaskId != taskID {
			t.Fatalf("SubmitTask failed at sample %d: %v", i, err)
		}
		submissionLatencies = append(submissionLatencies, subDuration)

		// --- STAGE 2: DIRECT RAFT COMMIT LATENCY ---
		raftStart := time.Now()
		commitCmd := &consensus.Command{
			Op: consensus.OpSetTenantQuota,
			TenantQuota: &domain.TenantQuota{
				TenantID:       tenantID,
				MaxConcurrency: 50,
				MaxQueueDepth:  1000,
			},
			Timestamp: time.Now(),
		}
		_, applyErr := leaderNode.Apply(commitCmd, 3*time.Second)
		raftDuration := time.Since(raftStart)
		if applyErr != nil {
			t.Fatalf("Raft commit failed at sample %d: %v", i, applyErr)
		}
		raftCommitLatencies = append(raftCommitLatencies, raftDuration)

		// --- STAGE 3: SCHEDULING LATENCY (Evaluation against ReadyQueue) ---
		schedStart := time.Now()
		workerDomain, _ := leaderStore.GetWorker("e2e-bench-worker-1")
		// Consult store ready tasks
		readyTasks := leaderStore.GetReadyTasks()
		schedDuration := time.Since(schedStart)
		_ = readyTasks
		_ = workerDomain
		schedulingLatencies = append(schedulingLatencies, schedDuration)

		// --- STAGE 4: WORKER ASSIGNMENT LATENCY (PullTask RPC + Raft Lease Grant) ---
		assignStart := time.Now()
		pullResp, err := wClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
			WorkerId:  "e2e-bench-worker-1",
			SessionId: "session-bench-1",
		})
		assignDuration := time.Since(assignStart)
		if err != nil || !pullResp.HasTask {
			t.Fatalf("PullTask failed at sample %d: %v", i, err)
		}
		assignmentLatencies = append(assignmentLatencies, assignDuration)

		assignment := pullResp.GetAssignment()

		// --- STAGE 5: EXECUTION LATENCY (Ack -> Work -> ReportCompleted) ---
		execStart := time.Now()
		_, ackErr := wClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
			TaskId:     assignment.TaskId,
			WorkerId:   "e2e-bench-worker-1",
			SessionId:  "session-bench-1",
			LeaseEpoch: assignment.LeaseEpoch,
		})
		if ackErr != nil {
			t.Fatalf("AckTaskRunning failed at sample %d: %v", i, ackErr)
		}

		// Simulated payload work
		time.Sleep(200 * time.Microsecond)

		_, compErr := wClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
			TaskId:     assignment.TaskId,
			WorkerId:   "e2e-bench-worker-1",
			SessionId:  "session-bench-1",
			LeaseEpoch: assignment.LeaseEpoch,
			Result:     []byte(`{"status":"success"}`),
		})
		execDuration := time.Since(execStart)
		if compErr != nil {
			t.Fatalf("ReportTaskCompleted failed at sample %d: %v", i, compErr)
		}
		executionLatencies = append(executionLatencies, execDuration)

		// --- STAGE 6: TOTAL COMPLETION LATENCY ---
		totalDuration := time.Since(subStart)
		totalLatencies = append(totalLatencies, totalDuration)
	}

	subP50, subP95, subP99 := computePercentiles(submissionLatencies)
	raftP50, raftP95, raftP99 := computePercentiles(raftCommitLatencies)
	schedP50, schedP95, schedP99 := computePercentiles(schedulingLatencies)
	assignP50, assignP95, assignP99 := computePercentiles(assignmentLatencies)
	execP50, execP95, execP99 := computePercentiles(executionLatencies)
	totalP50, totalP95, totalP99 := computePercentiles(totalLatencies)

	fmt.Printf("\n==================================================================================\n")
	fmt.Printf("           END-TO-END PIPELINE LATENCY BREAKDOWN (300 SAMPLES)                    \n")
	fmt.Printf("==================================================================================\n")
	fmt.Printf("%-30s | %-12s | %-12s | %-12s\n", "Pipeline Stage", "p50", "p95", "p99")
	fmt.Printf("----------------------------------------------------------------------------------\n")
	fmt.Printf("%-30s | %-12s | %-12s | %-12s\n", "1. Submission Latency", subP50, subP95, subP99)
	fmt.Printf("%-30s | %-12s | %-12s | %-12s\n", "2. Raft Quorum Commit Latency", raftP50, raftP95, raftP99)
	fmt.Printf("%-30s | %-12s | %-12s | %-12s\n", "3. Scheduling Latency", schedP50, schedP95, schedP99)
	fmt.Printf("%-30s | %-12s | %-12s | %-12s\n", "4. Worker Assignment Latency", assignP50, assignP95, assignP99)
	fmt.Printf("%-30s | %-12s | %-12s | %-12s\n", "5. Execution Latency", execP50, execP95, execP99)
	fmt.Printf("%-30s | %-12s | %-12s | %-12s\n", "6. Total Completion Latency", totalP50, totalP95, totalP99)
	fmt.Printf("==================================================================================\n")
}
