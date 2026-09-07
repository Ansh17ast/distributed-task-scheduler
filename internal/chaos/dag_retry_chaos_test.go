package chaos

import (
	"context"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/domain"
)

// TestChaos_DAGFailover_DiamondPipeline tests killing leader across each stage of diamond DAG execution (A -> B/C -> D).
func TestChaos_DAGFailover_DiamondPipeline(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-DAG-FAILOVER", 4001)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	leaderIdx := h.WaitLeader(3 * time.Second)
	if leaderIdx < 0 {
		t.Fatalf("leader election failed")
	}

	dagID := "dag-diamond-chaos"
	tasks := []*schedulerv1.TaskSpec{
		{Id: "task-A", TenantId: "tenant-dag", Priority: 100},
		{Id: "task-B", TenantId: "tenant-dag", Priority: 50, Dependencies: []string{"task-A"}},
		{Id: "task-C", TenantId: "tenant-dag", Priority: 50, Dependencies: []string{"task-A"}},
		{Id: "task-D", TenantId: "tenant-dag", Priority: 10, Dependencies: []string{"task-B", "task-C"}},
	}

	_, err = h.Client(leaderIdx).SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    dagID,
		TenantId: "tenant-dag",
		Tasks:    tasks,
	})
	if err != nil {
		t.Fatalf("failed submitting DAG: %v", err)
	}

	// 1. Worker executes Task A
	wClient := h.WorkerClient(leaderIdx)
	_, _ = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "w-dag", SessionId: "sess-dag", MaxSlots: 5,
	})
	pullA, _ := wClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: "w-dag", SessionId: "sess-dag", AvailableSlots: 1,
	})
	if pullA.GetAssignment().GetTaskId() != "task-A" {
		t.Fatalf("expected task-A, got: %s", pullA.GetAssignment().GetTaskId())
	}
	_, _ = wClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: "w-dag", SessionId: "sess-dag", TaskId: "task-A", LeaseEpoch: pullA.GetAssignment().GetLeaseEpoch(),
	})
	_, _ = wClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId: "w-dag", SessionId: "sess-dag", TaskId: "task-A", LeaseEpoch: pullA.GetAssignment().GetLeaseEpoch(),
		Result: []byte("A done"),
	})

	// Wait for DAG promotion of B and C
	var tB, tC *domain.Task
	for attempt := 0; attempt < 30; attempt++ {
		tB, _ = h.Store(leaderIdx).GetTask("task-B")
		tC, _ = h.Store(leaderIdx).GetTask("task-C")
		if tB != nil && tC != nil && tB.State == domain.StateReady && tC.State == domain.StateReady {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	tD, _ := h.Store(leaderIdx).GetTask("task-D")
	if tB.State != domain.StateReady || tC.State != domain.StateReady || tD.State != domain.StateBlocked {
		t.Fatalf("invalid intermediate DAG states: B=%s, C=%s, D=%s", tB.State, tC.State, tD.State)
	}

	// 2. DISASTER: KILL LEADER WHILE B/C ARE READY
	h.KillNode(leaderIdx)

	// Elect new leader
	newLeaderIdx := -1
	for attempt := 0; attempt < 50; attempt++ {
		for i := 0; i < 3; i++ {
			if i != leaderIdx && h.Node(i) != nil && h.Node(i).IsLeader() {
				newLeaderIdx = i
				break
			}
		}
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeaderIdx < 0 {
		t.Fatalf("new leader election failed after killing leader mid-DAG")
	}

	// 3. Worker reconnects to new leader and executes B and C
	newWClient := h.WorkerClient(newLeaderIdx)
	_, _ = newWClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "w-dag-2", SessionId: "sess-dag-2", MaxSlots: 5,
	})

	for executed := 0; executed < 2; executed++ {
		pullResp, err := newWClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
			WorkerId: "w-dag-2", SessionId: "sess-dag-2", AvailableSlots: 1,
		})
		if err != nil || pullResp.GetAssignment() == nil {
			t.Fatalf("failed pulling parallel task %d on new leader: %v", executed+1, err)
		}
		tID := pullResp.GetAssignment().GetTaskId()
		ep := pullResp.GetAssignment().GetLeaseEpoch()
		_, _ = newWClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
			WorkerId: "w-dag-2", SessionId: "sess-dag-2", TaskId: tID, LeaseEpoch: ep,
		})
		_, _ = newWClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
			WorkerId: "w-dag-2", SessionId: "sess-dag-2", TaskId: tID, LeaseEpoch: ep,
			Result: []byte("done"),
		})
	}

	// Invariant: Now Task D must be READY
	var tDAfter *domain.Task
	for attempt := 0; attempt < 30; attempt++ {
		tDAfter, _ = h.Store(newLeaderIdx).GetTask("task-D")
		if tDAfter != nil && tDAfter.State == domain.StateReady {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if tDAfter == nil || tDAfter.State != domain.StateReady {
		t.Fatalf("task-D was not promoted to READY after B and C completed: state=%v", tDAfter)
	}

	// 4. Complete join task D
	pullD, _ := newWClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: "w-dag-2", SessionId: "sess-dag-2", AvailableSlots: 1,
	})
	if pullD.GetAssignment().GetTaskId() != "task-D" {
		t.Fatalf("expected task-D, got: %s", pullD.GetAssignment().GetTaskId())
	}
	_, _ = newWClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: "w-dag-2", SessionId: "sess-dag-2", TaskId: "task-D", LeaseEpoch: pullD.GetAssignment().GetLeaseEpoch(),
	})
	_, _ = newWClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId: "w-dag-2", SessionId: "sess-dag-2", TaskId: "task-D", LeaseEpoch: pullD.GetAssignment().GetLeaseEpoch(),
		Result: []byte("final D result"),
	})

	// Check final DAG completion
	dagResp, err := h.Client(newLeaderIdx).GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: dagID})
	if err != nil || dagResp.GetSucceededTasks() != 4 {
		t.Fatalf("DAG did not reach 4 succeeded tasks: resp=%+v, err=%v", dagResp, err)
	}

	h.Invariants.CheckDAGTopologyIntegrity(h.Store(newLeaderIdx), "task-A", "task-D")
}

// TestChaos_RetryWaitFailover tests leader death during RETRY_WAIT and verifies new leader reconstructs timer and promotes to READY.
func TestChaos_RetryWaitFailover(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-RETRY-FAILOVER", 4002)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	leaderIdx := h.WaitLeader(3 * time.Second)
	wClient := h.WorkerClient(leaderIdx)

	taskID := "task-retry-failover"
	_, _ = h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-retry", Priority: 50, MaxRetries: 2},
	})

	_, _ = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "w-retry", SessionId: "sess-retry", MaxSlots: 1,
	})
	pullResp, _ := wClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: "w-retry", SessionId: "sess-retry", AvailableSlots: 1,
	})
	epoch := pullResp.GetAssignment().GetLeaseEpoch()

	// Ack task running first
	_, _ = wClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: "w-retry", SessionId: "sess-retry", TaskId: taskID, LeaseEpoch: epoch,
	})

	// Worker reports retryable failure -> task enters RETRY_WAIT
	_, _ = wClient.ReportTaskFailed(ctx, &schedulerv1.ReportTaskFailedRequest{
		WorkerId: "w-retry", SessionId: "sess-retry", TaskId: taskID, LeaseEpoch: epoch,
		ErrorMessage: "transient network glitch", Retryable: true,
	})

	taskInRetry, _ := h.Store(leaderIdx).GetTask(taskID)
	if taskInRetry.State != domain.StateRetryWait {
		t.Fatalf("expected state RETRY_WAIT, got %s", taskInRetry.State)
	}

	// KILL LEADER DURING RETRY_WAIT
	h.KillNode(leaderIdx)

	// Wait for new leader
	newLeaderIdx := -1
	for attempt := 0; attempt < 50; attempt++ {
		for i := 0; i < 3; i++ {
			if i != leaderIdx && h.Node(i) != nil && h.Node(i).IsLeader() {
				newLeaderIdx = i
				break
			}
		}
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeaderIdx < 0 {
		t.Fatalf("new leader election failed")
	}

	// Wait for new leader's recovered retry timer to fire (backoff ~200-400ms)
	var promotedToReady bool
	for attempt := 0; attempt < 40; attempt++ {
		tRecovered, err := h.Store(newLeaderIdx).GetTask(taskID)
		if err == nil && tRecovered != nil && tRecovered.State == domain.StateReady {
			promotedToReady = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !promotedToReady {
		t.Fatalf("retry task was not promoted to READY on new leader after timer recovery")
	}
}
