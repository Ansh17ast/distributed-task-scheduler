package chaos

import (
	"context"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/domain"
)

// TestChaos_ZombieWorkerFencing tests Worker A (Epoch 1) partition, task reclamation, Worker B (Epoch 2) execution, and rejection of Worker A delayed report.
func TestChaos_ZombieWorkerFencing(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-ZOMBIE-FENCING", 3001)
	cfg.WorkerLease = 400 * time.Millisecond
	cfg.ReconcileDur = 200 * time.Millisecond

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

	wClient := h.WorkerClient(leaderIdx)

	// 1. Submit task
	taskID := "task-zombie-test"
	_, err = h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-zombie", Priority: 50, MaxRetries: 3},
	})
	if err != nil {
		t.Fatalf("failed submitting task: %v", err)
	}

	// 2. Register Worker A (session A)
	workerA := "worker-alpha"
	sessA := "sess-alpha-uuid"
	_, err = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: workerA, SessionId: sessA, MaxSlots: 2,
	})
	if err != nil {
		t.Fatalf("failed registering worker A: %v", err)
	}

	// 3. Worker A pulls task -> granted LeaseEpoch 1
	pullRespA, err := wClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: workerA, SessionId: sessA, AvailableSlots: 1,
	})
	if err != nil || pullRespA.GetAssignment() == nil {
		t.Fatalf("worker A failed pulling task: %v", err)
	}
	epoch1 := pullRespA.GetAssignment().GetLeaseEpoch()
	if epoch1 != 1 {
		t.Fatalf("expected epoch 1, got %d", epoch1)
	}

	// Worker A acks running
	_, _ = wClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: workerA, SessionId: sessA, TaskId: taskID, LeaseEpoch: epoch1,
	})

	// 4. Simulate Worker A network partition: stops heartbeating
	// Wait for lease duration + grace period to elapse and sweep expired leases
	time.Sleep(1 * time.Second)
	currLeader := h.FindLeaderIndex()
	if currLeader < 0 {
		currLeader = leaderIdx
	}
	h.Server(currLeader).SweepExpiredLeases()

	// Task should now be reclaimed to READY
	var reclaimedTask *domain.Task
	for attempt := 0; attempt < 20; attempt++ {
		reclaimedTask, err = h.Store(leaderIdx).GetTask(taskID)
		if err == nil && reclaimedTask != nil && reclaimedTask.State == domain.StateReady {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if reclaimedTask == nil || reclaimedTask.State != domain.StateReady {
		t.Fatalf("task was not reclaimed to READY: %+v", reclaimedTask)
	}

	// 5. Register Worker B (session B)
	workerB := "worker-beta"
	sessB := "sess-beta-uuid"
	_, err = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: workerB, SessionId: sessB, MaxSlots: 2,
	})
	if err != nil {
		t.Fatalf("failed registering worker B: %v", err)
	}

	// 6. Worker B pulls task -> granted LeaseEpoch 2
	pullRespB, err := wClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: workerB, SessionId: sessB, AvailableSlots: 1,
	})
	if err != nil || pullRespB.GetAssignment() == nil {
		t.Fatalf("worker B failed pulling task: %v", err)
	}
	epoch2 := pullRespB.GetAssignment().GetLeaseEpoch()
	if epoch2 != 2 {
		t.Fatalf("expected epoch 2, got %d", epoch2)
	}

	// Worker B acks running
	_, _ = wClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: workerB, SessionId: sessB, TaskId: taskID, LeaseEpoch: epoch2,
	})

	// Worker B completes task successfully
	_, err = wClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   workerB,
		SessionId:  sessB,
		TaskId:     taskID,
		LeaseEpoch: epoch2,
		Result:     []byte("worker B valid result"),
	})
	if err != nil {
		t.Fatalf("worker B failed completing task: %v", err)
	}

	// 7. ZOMBIE ATTACK: Worker A reconnects and attempts to report completion with stale Epoch 1
	_, zombieErr := wClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   workerA,
		SessionId:  sessA,
		TaskId:     taskID,
		LeaseEpoch: epoch1,
		Result:     []byte("zombie stale result"),
	})

	// Must be rejected!
	if zombieErr == nil {
		t.Fatalf("CRITICAL BUG: Coordinator accepted stale completion from Zombie Worker A!")
	}

	// 8. Invariant: Authoritative state must preserve Worker B's result
	h.Invariants.CheckZombieFencing(h.Store(leaderIdx), taskID, workerB, epoch2, zombieErr)
	h.Invariants.CheckSlotAccounting(h.Store(leaderIdx))

	finalTask, _ := h.Store(leaderIdx).GetTask(taskID)
	if string(finalTask.Result) != "worker B valid result" {
		t.Fatalf("expected worker B result, got: %s", string(finalTask.Result))
	}
}

// TestChaos_WorkerReconnectBeforeLeaseExpiry tests worker reconnecting within lease window.
func TestChaos_WorkerReconnectBeforeLeaseExpiry(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-WORKER-RECONNECT", 3002)
	cfg.WorkerLease = 3 * time.Second

	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	leaderIdx := h.WaitLeader(3 * time.Second)
	wClient := h.WorkerClient(leaderIdx)

	taskID := "task-reconnect-test"
	_, _ = h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-rec", Priority: 50},
	})

	workerID := "worker-reconnect"
	sessionID := "sess-rec-1"
	_, _ = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: workerID, SessionId: sessionID, MaxSlots: 1,
	})

	pullResp, _ := wClient.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: workerID, SessionId: sessionID, AvailableSlots: 1,
	})
	epoch := pullResp.GetAssignment().GetLeaseEpoch()

	// Ack task running
	_, _ = wClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: workerID, SessionId: sessionID, TaskId: taskID, LeaseEpoch: epoch,
	})

	// Worker pauses heartbeats for 500ms (less than 3s lease) then sends heartbeat
	time.Sleep(500 * time.Millisecond)
	hbResp, err := wClient.Heartbeat(ctx, &schedulerv1.HeartbeatRequest{
		WorkerId: workerID, SessionId: sessionID,
		ActiveLeases: []*schedulerv1.ActiveLeaseHeartbeat{{TaskId: taskID, LeaseEpoch: epoch}},
	})
	if err != nil || !hbResp.GetAcknowledged() || len(hbResp.GetRevokedTaskIds()) > 0 {
		t.Fatalf("heartbeat within lease window failed or revoked: err=%v, resp=%+v", err, hbResp)
	}

	// Worker finishes task
	_, err = wClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId: workerID, SessionId: sessionID, TaskId: taskID, LeaseEpoch: epoch,
		Result: []byte("success"),
	})
	if err != nil {
		t.Fatalf("failed completing task after reconnect: %v", err)
	}

	h.Invariants.CheckSlotAccounting(h.Store(leaderIdx))
}
