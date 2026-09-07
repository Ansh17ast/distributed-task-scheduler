package coordinator

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// TestLease_WorkerRestartSameWorkerID verifies that when a worker restarts with the same WorkerID
// but a new SessionID:
// 1. New SessionID establishes a distinct process incarnation.
// 2. Old Session cannot mutate worker state or revive ownership.
// 3. Stale completion from old Session is rejected.
// 4. Valid new Session communication proceeds normally.
func TestLease_WorkerRestartSameWorkerID(t *testing.T) {
	srv, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial coordinator: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// 1. Worker alpha registers with Session A
	const workerID = "worker-alpha"
	const sessionA = "session-uuid-AAA"
	const sessionB = "session-uuid-BBB"

	regA, err := client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  workerID,
		SessionId: sessionA,
		Address:   "127.0.0.1:9091",
		MaxSlots:  1,
	})
	if err != nil || !regA.GetAccepted() {
		t.Fatalf("failed to register Session A: %v", err)
	}

	// 2. Submit Task X
	subResp, err := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "task-restart-key",
			TenantId:    "tenant-lease",
			Priority:    10,
			PayloadType: "computation",
			Payload:     []byte("work-data"),
			MaxRetries:  2,
		},
	})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}
	taskID := subResp.GetTaskId()

	// 3. Worker alpha pulls Task X with Session A
	pullResp, err := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       workerID,
		SessionId:      sessionA,
		AvailableSlots: 1,
	})
	if err != nil || !pullResp.GetHasTask() {
		t.Fatalf("Session A failed to pull task: %v", err)
	}
	assignment := pullResp.GetAssignment()
	if assignment.GetTaskId() != taskID || assignment.GetLeaseEpoch() != 1 {
		t.Fatalf("unexpected assignment: task=%s epoch=%d", assignment.GetTaskId(), assignment.GetLeaseEpoch())
	}

	// Ack running
	_, err = client.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   workerID,
		SessionId:  sessionA,
		TaskId:     taskID,
		LeaseEpoch: 1,
	})
	if err != nil {
		t.Fatalf("Session A failed to ack running: %v", err)
	}

	// 4. Worker alpha restarts with new Session B
	regB, err := client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  workerID,
		SessionId: sessionB,
		Address:   "127.0.0.1:9091",
		MaxSlots:  1,
	})
	if err != nil || !regB.GetAccepted() {
		t.Fatalf("failed to register Session B: %v", err)
	}

	// Verify active session on worker record is Session B
	wRec, err := store.GetWorker(workerID)
	if err != nil || wRec.SessionID != sessionB {
		t.Fatalf("expected active session %s, got %v", sessionB, wRec)
	}

	// 5. Old Session A heartbeat must be rejected (cannot revive old ownership)
	_, err = client.Heartbeat(ctx, &schedulerv1.HeartbeatRequest{
		WorkerId:       workerID,
		SessionId:      sessionA,
		AvailableSlots: 1,
	})
	if err == nil {
		t.Fatal("expected heartbeat from stale Session A to be rejected, but it succeeded")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for stale heartbeat, got %v", err)
	}

	// 6. Stale completion from Session A must be rejected
	_, err = client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   workerID,
		SessionId:  sessionA,
		TaskId:     taskID,
		LeaseEpoch: 1,
		Result:     []byte("stale-result"),
	})
	if err == nil {
		t.Fatal("expected completion from stale Session A to be rejected, but it succeeded")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for stale completion, got %v", err)
	}

	// 7. Session B attempts completion for Task X with Epoch 1 (which belongs to Session A)
	_, err = client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   workerID,
		SessionId:  sessionB,
		TaskId:     taskID,
		LeaseEpoch: 1,
		Result:     []byte("session-b-result"),
	})
	if err == nil {
		t.Fatal("expected completion from Session B with unassigned task to be rejected")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for mismatched session, got %v", err)
	}

	// 8. Reclaim task and let Session B execute it with Epoch 2
	_, err = store.ReclaimTask(taskID)
	if err != nil {
		t.Fatalf("failed to reclaim task: %v", err)
	}

	pullB, err := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       workerID,
		SessionId:      sessionB,
		AvailableSlots: 1,
	})
	if err != nil || !pullB.GetHasTask() {
		t.Fatalf("Session B failed to pull reclaimed task: %v", err)
	}
	assignB := pullB.GetAssignment()
	if assignB.GetLeaseEpoch() != 2 || assignB.GetSessionId() != sessionB {
		t.Fatalf("expected epoch 2 with session B, got epoch=%d session=%s", assignB.GetLeaseEpoch(), assignB.GetSessionId())
	}

	// Session B acks running
	_, _ = client.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   workerID,
		SessionId:  sessionB,
		TaskId:     taskID,
		LeaseEpoch: 2,
	})

	// Session B completes task successfully
	compResp, err := client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   workerID,
		SessionId:  sessionB,
		TaskId:     taskID,
		LeaseEpoch: 2,
		Result:     []byte("valid-session-b-completion"),
	})
	if err != nil || !compResp.GetAcknowledged() {
		t.Fatalf("Session B completion failed: %v", err)
	}

	// Verify terminal state
	taskFinal, err := store.GetTask(taskID)
	if err != nil || taskFinal.State != domain.StateSucceeded {
		t.Fatalf("expected SUCCEEDED task, got %v", taskFinal)
	}
	_ = srv
}

// TestLease_StaleSessionRejection verifies that session mismatches trigger ErrStaleWorkerSession / codes.FailedPrecondition.
func TestLease_StaleSessionRejection(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial coordinator: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// Register worker with session-1
	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-fencing",
		SessionId: "session-1",
		MaxSlots:  2,
	})

	// Submit task
	subResp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "fence-task",
			TenantId:    "tenant-fence",
			Priority:    5,
			PayloadType: "compute",
			MaxRetries:  1,
		},
	})
	taskID := subResp.GetTaskId()

	// Pull task with session-1
	pullResp, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-fencing",
		SessionId:      "session-1",
		AvailableSlots: 2,
	})
	if !pullResp.GetHasTask() {
		t.Fatalf("expected task dispatched")
	}

	// Attempt AckTaskRunning with mismatched session-wrong
	_, err = client.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   "worker-fencing",
		SessionId:  "session-wrong",
		TaskId:     taskID,
		LeaseEpoch: 1,
	})
	if err == nil {
		t.Fatal("expected error for AckTaskRunning with wrong session")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", status.Code(err))
	}

	// Valid AckTaskRunning with session-1
	_, err = client.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   "worker-fencing",
		SessionId:  "session-1",
		TaskId:     taskID,
		LeaseEpoch: 1,
	})
	if err != nil {
		t.Fatalf("valid ack failed: %v", err)
	}

	// Attempt ReportTaskFailed with mismatched session
	_, err = client.ReportTaskFailed(ctx, &schedulerv1.ReportTaskFailedRequest{
		WorkerId:     "worker-fencing",
		SessionId:    "session-wrong",
		TaskId:       taskID,
		LeaseEpoch:   1,
		ErrorMessage: "fail",
	})
	if err == nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for ReportTaskFailed with wrong session, got %v", err)
	}

	_ = store
}

// TestLease_HeartbeatLoss tests that when a worker stops sending heartbeats, the lease reaper marks it DEAD.
func TestLease_HeartbeatLoss(t *testing.T) {
	srv, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	srv.cfg.WorkerLeaseDur = 200 * time.Millisecond
	srv.cfg.LeaseGraceWindow = 50 * time.Millisecond

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewWorkerServiceClient(conn)
	ctx := context.Background()

	_, err = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-hb-loss",
		SessionId: "sess-hb",
		MaxSlots:  2,
	})
	if err != nil {
		t.Fatalf("failed to register: %v", err)
	}

	// Initial status is HEALTHY
	w, err := store.GetWorker("worker-hb-loss")
	if err != nil || w.Status != domain.WorkerStatusHealthy {
		t.Fatalf("expected HEALTHY worker, got %v", w)
	}

	// Sleep past lease duration + grace window
	time.Sleep(300 * time.Millisecond)

	// Trigger sweep
	srv.SweepExpiredLeases()

	// Worker must be DEAD
	wAfter, err := store.GetWorker("worker-hb-loss")
	if err != nil {
		t.Fatalf("failed to get worker: %v", err)
	}
	if wAfter.Status != domain.WorkerStatusDead {
		t.Fatalf("expected worker DEAD, got %v", wAfter.Status)
	}
}

// TestLease_ExpirationAndReclamation tests that an expired lease is automatically reclaimed.
func TestLease_ExpirationAndReclamation(t *testing.T) {
	srv, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	srv.cfg.WorkerLeaseDur = 150 * time.Millisecond
	srv.cfg.LeaseGraceWindow = 50 * time.Millisecond

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-reclaim",
		SessionId: "sess-rec",
		MaxSlots:  1,
	})

	subResp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "rec-task-key",
			TenantId:    "tenant-rec",
			Priority:    1,
			PayloadType: "job",
			MaxRetries:  1,
		},
	})
	taskID := subResp.GetTaskId()

	pullResp, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-reclaim",
		SessionId:      "sess-rec",
		AvailableSlots: 1,
	})
	if !pullResp.GetHasTask() {
		t.Fatalf("expected task pulled")
	}

	// Sleep past timeout and sweep
	time.Sleep(250 * time.Millisecond)
	srv.SweepExpiredLeases()

	// Task must be reclaimed to READY
	task, err := store.GetTask(taskID)
	if err != nil {
		t.Fatalf("failed to get task: %v", err)
	}
	if task.State != domain.StateReady {
		t.Fatalf("expected task reclaimed to READY, got %s", task.State)
	}
	if task.AssignedWorkerID != "" || task.AssignedSessionID != "" {
		t.Fatalf("expected assigned worker and session to be cleared, got worker=%s session=%s",
			task.AssignedWorkerID, task.AssignedSessionID)
	}
}

// TestLease_WorkerCrashMidTask verifies crash recovery when a worker stops functioning mid-execution.
func TestLease_WorkerCrashMidTask(t *testing.T) {
	srv, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	srv.cfg.WorkerLeaseDur = 200 * time.Millisecond
	srv.cfg.LeaseGraceWindow = 50 * time.Millisecond

	coordClient := getCoordinatorClient(t, addr)
	ctx := context.Background()

	// Submit task with 2 retries
	subResp, err := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "crash-task-key",
			TenantId:    "tenant-crash",
			Priority:    5,
			PayloadType: "compute",
			MaxRetries:  2,
		},
	})
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}
	taskID := subResp.GetTaskId()

	// Worker A starts and crashes
	wCfgA := config.DefaultWorkerConfig("worker-A", addr)
	wCfgA.MaxSlots = 1
	wCfgA.SessionID = "sess-A"
	daemonA := worker.NewDaemon(wCfgA, func(ctx context.Context, payload []byte) ([]byte, error) {
		// Worker freezes / sleeps indefinitely
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(10 * time.Second):
			return []byte("done"), nil
		}
	}, nil)

	if err := daemonA.Start(ctx); err != nil {
		t.Fatalf("failed starting daemon A: %v", err)
	}

	// Wait for Worker A to pull and begin execution
	time.Sleep(50 * time.Millisecond)

	// Simulate crash: kill daemon A ungracefully
	daemonA.Kill()

	// Wait for lease expiration and run sweep
	time.Sleep(300 * time.Millisecond)
	srv.SweepExpiredLeases()

	// Verify task reclaimed to READY
	taskMid, _ := store.GetTask(taskID)
	if taskMid.State != domain.StateReady {
		t.Fatalf("expected task in READY, got %s", taskMid.State)
	}

	// Worker B starts and picks up task
	var workerBCompleted atomic.Bool
	wCfgB := config.DefaultWorkerConfig("worker-B", addr)
	wCfgB.MaxSlots = 1
	wCfgB.SessionID = "sess-B"
	daemonB := worker.NewDaemon(wCfgB, func(ctx context.Context, payload []byte) ([]byte, error) {
		workerBCompleted.Store(true)
		return []byte("worker-B-success"), nil
	}, nil)

	if err := daemonB.Start(ctx); err != nil {
		t.Fatalf("failed starting daemon B: %v", err)
	}
	defer daemonB.Stop()

	// Wait for Worker B to complete
	time.Sleep(200 * time.Millisecond)

	if !workerBCompleted.Load() {
		t.Fatal("expected Worker B to execute and complete reclaimed task")
	}

	taskFinal, _ := store.GetTask(taskID)
	if taskFinal.State != domain.StateSucceeded {
		t.Fatalf("expected SUCCEEDED state, got %s", taskFinal.State)
	}
	if taskFinal.LeaseEpoch != 2 {
		t.Fatalf("expected LeaseEpoch 2, got %d", taskFinal.LeaseEpoch)
	}
}

// TestLease_WorkerReconnect tests worker reconnect where coordinator revokes expired leases via Heartbeat.
func TestLease_WorkerReconnect(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// 1. Register worker
	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-recon",
		SessionId: "sess-recon",
		MaxSlots:  1,
	})

	// 2. Submit and pull task
	subResp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "recon-key",
			TenantId:    "tenant-recon",
			Priority:    1,
			PayloadType: "compute",
			MaxRetries:  1,
		},
	})
	taskID := subResp.GetTaskId()

	pullResp, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-recon",
		SessionId:      "sess-recon",
		AvailableSlots: 1,
	})
	if !pullResp.GetHasTask() {
		t.Fatalf("expected task pulled")
	}

	// 3. Coordinator authoritatively reclaims task
	_, err = store.ReclaimTask(taskID)
	if err != nil {
		t.Fatalf("reclaim failed: %v", err)
	}

	// 4. Worker sends Heartbeat claiming task with Epoch 1
	hbResp, err := client.Heartbeat(ctx, &schedulerv1.HeartbeatRequest{
		WorkerId:       "worker-recon",
		SessionId:      "sess-recon",
		AvailableSlots: 0,
		ActiveLeases: []*schedulerv1.ActiveLeaseHeartbeat{
			{TaskId: taskID, LeaseEpoch: 1},
		},
	})
	if err != nil {
		t.Fatalf("heartbeat failed: %v", err)
	}

	// 5. Coordinator must return taskID in revoked_task_ids
	if len(hbResp.GetRevokedTaskIds()) != 1 || hbResp.GetRevokedTaskIds()[0] != taskID {
		t.Fatalf("expected task %s in RevokedTaskIds, got %v", taskID, hbResp.GetRevokedTaskIds())
	}
}

// TestLease_StaleEpochRejection verifies that completions with stale epochs are rejected.
func TestLease_StaleEpochRejection(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-stale-epoch",
		SessionId: "sess-stale",
		MaxSlots:  2,
	})

	subResp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "stale-epoch-key",
			TenantId:    "tenant-epoch",
			Priority:    1,
			PayloadType: "compute",
			MaxRetries:  2,
		},
	})
	taskID := subResp.GetTaskId()

	// Pull task (Epoch 1)
	_, _ = client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-stale-epoch",
		SessionId:      "sess-stale",
		AvailableSlots: 2,
	})

	// Reclaim task to increment epoch on next pull
	_, _ = store.ReclaimTask(taskID)

	// Pull task again (Epoch 2)
	_, _ = client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-stale-epoch",
		SessionId:      "sess-stale",
		AvailableSlots: 2,
	})

	// Attempt completion with old Epoch 1
	_, err := client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "worker-stale-epoch",
		SessionId:  "sess-stale",
		TaskId:     taskID,
		LeaseEpoch: 1, // Stale!
		Result:     []byte("late-result"),
	})
	if err == nil {
		t.Fatal("expected stale epoch completion to be rejected")
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition, got %v", status.Code(err))
	}
}

// TestLease_StaleWorkerIdentityRejection verifies that a different worker cannot complete a task.
func TestLease_StaleWorkerIdentityRejection(t *testing.T) {
	_, addr, _, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-real",
		SessionId: "sess-real",
		MaxSlots:  1,
	})
	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-imposter",
		SessionId: "sess-imposter",
		MaxSlots:  1,
	})

	subResp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "ident-key",
			TenantId:    "tenant-ident",
			Priority:    1,
			PayloadType: "compute",
		},
	})
	taskID := subResp.GetTaskId()

	pullResp, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-real",
		SessionId:      "sess-real",
		AvailableSlots: 1,
	})
	epoch := pullResp.GetAssignment().GetLeaseEpoch()

	// Imposter attempts to complete
	_, err := client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "worker-imposter",
		SessionId:  "sess-imposter",
		TaskId:     taskID,
		LeaseEpoch: epoch,
		Result:     []byte("fraud"),
	})
	if err == nil {
		t.Fatal("expected imposter completion to be rejected")
	}
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected PermissionDenied, got %v", status.Code(err))
	}
}

// TestLease_DelayedCompletionVsReclamation tests the race where a worker completes locally
// but the RPC arrives after the coordinator has already reclaimed the task.
func TestLease_DelayedCompletionVsReclamation(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-delayed",
		SessionId: "sess-delayed",
		MaxSlots:  1,
	})

	subResp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "delayed-key",
			TenantId:    "tenant-delayed",
			Priority:    1,
			PayloadType: "compute",
			MaxRetries:  1,
		},
	})
	taskID := subResp.GetTaskId()

	_, _ = client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-delayed",
		SessionId:      "sess-delayed",
		AvailableSlots: 1,
	})

	// Coordinator reclaims task
	_, err := store.ReclaimTask(taskID)
	if err != nil {
		t.Fatalf("reclaim failed: %v", err)
	}

	// Delayed completion arrives with old lease epoch 1
	_, err = client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "worker-delayed",
		SessionId:  "sess-delayed",
		TaskId:     taskID,
		LeaseEpoch: 1,
		Result:     []byte("late-payload"),
	})
	if err == nil {
		t.Fatal("expected delayed completion to be rejected")
	}
	if status.Code(err) != codes.FailedPrecondition && status.Code(err) != codes.PermissionDenied {
		t.Fatalf("expected FailedPrecondition or PermissionDenied, got %v", status.Code(err))
	}
}

// TestLease_SlotRecoveryInvariant asserts available slots invariant 0 <= AvailableSlots <= MaxSlots
// through pulls, worker crash, reclamation, and reconnect.
func TestLease_SlotRecoveryInvariant(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	const maxSlots = 3
	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-slots",
		SessionId: "sess-slots-1",
		MaxSlots:  maxSlots,
	})

	// Submit 3 tasks
	var taskIDs []string
	for i := 0; i < maxSlots; i++ {
		resp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{
				Id:          fmt.Sprintf("slot-key-%d", i),
				TenantId:    "tenant-slots",
				Priority:    int32(i),
				PayloadType: "compute",
				MaxRetries:  1,
			},
		})
		taskIDs = append(taskIDs, resp.GetTaskId())
	}

	// Pull all 3 tasks
	for range taskIDs {
		pullResp, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
			WorkerId:       "worker-slots",
			SessionId:      "sess-slots-1",
			AvailableSlots: maxSlots,
		})
		if !pullResp.GetHasTask() {
			t.Fatalf("expected task pulled")
		}
	}

	w, _ := store.GetWorker("worker-slots")
	if w.AvailableSlots != 0 {
		t.Fatalf("expected 0 available slots after 3 pulls, got %d", w.AvailableSlots)
	}

	// Reclaim all 3 tasks
	for _, id := range taskIDs {
		_, err := store.ReclaimTask(id)
		if err != nil {
			t.Fatalf("reclaim failed for %s: %v", id, err)
		}
	}

	wAfter, _ := store.GetWorker("worker-slots")
	if wAfter.AvailableSlots != maxSlots {
		t.Fatalf("expected slots restored to %d, got %d", maxSlots, wAfter.AvailableSlots)
	}

	// Worker restarts with new Session 2: capacity must reset to maxSlots
	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-slots",
		SessionId: "sess-slots-2",
		MaxSlots:  maxSlots,
	})

	wRestart, _ := store.GetWorker("worker-slots")
	if wRestart.AvailableSlots != maxSlots {
		t.Fatalf("expected restarted worker to have %d slots, got %d", maxSlots, wRestart.AvailableSlots)
	}
}

// TestLease_RetryAfterWorkerLoss verifies that a task with retries surviving worker loss is correctly re-executed.
func TestLease_RetryAfterWorkerLoss(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-w1",
		SessionId: "sess-w1",
		MaxSlots:  1,
	})
	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-w2",
		SessionId: "sess-w2",
		MaxSlots:  1,
	})

	subResp, _ := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:          "retry-loss-key",
			TenantId:    "tenant-retry-loss",
			Priority:    5,
			PayloadType: "compute",
			MaxRetries:  2,
		},
	})
	taskID := subResp.GetTaskId()

	// W1 pulls task
	pull1, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-w1",
		SessionId:      "sess-w1",
		AvailableSlots: 1,
	})
	if pull1.GetAssignment().GetLeaseEpoch() != 1 {
		t.Fatalf("expected epoch 1 on initial pull")
	}

	// Reclaim (worker loss)
	resState, err := store.ReclaimTask(taskID)
	if err != nil || resState != domain.StateReady {
		t.Fatalf("expected task reclaimed to READY: state=%v err=%v", resState, err)
	}

	tReclaimed, _ := store.GetTask(taskID)
	if tReclaimed.AttemptCount != 1 {
		t.Fatalf("expected AttemptCount 1, got %d", tReclaimed.AttemptCount)
	}

	// W2 pulls task
	pull2, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-w2",
		SessionId:      "sess-w2",
		AvailableSlots: 1,
	})
	if pull2.GetAssignment().GetLeaseEpoch() != 2 {
		t.Fatalf("expected epoch 2 on retry pull, got %d", pull2.GetAssignment().GetLeaseEpoch())
	}

	// W2 acks running
	_, _ = client.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   "worker-w2",
		SessionId:  "sess-w2",
		TaskId:     taskID,
		LeaseEpoch: 2,
	})

	// W2 completes
	_, err = client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "worker-w2",
		SessionId:  "sess-w2",
		TaskId:     taskID,
		LeaseEpoch: 2,
		Result:     []byte("w2-success"),
	})
	if err != nil {
		t.Fatalf("w2 completion failed: %v", err)
	}

	tFinal, _ := store.GetTask(taskID)
	if tFinal.State != domain.StateSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", tFinal.State)
	}
}

// TestLease_DAGTaskRecoveryAfterWorkerLoss verifies that worker failure on an upstream DAG task
// is recovered, executed by another worker, and correctly unblocks downstream tasks.
func TestLease_DAGTaskRecoveryAfterWorkerLoss(t *testing.T) {
	srv, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, _ := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	defer conn.Close()
	client := schedulerv1.NewWorkerServiceClient(conn)
	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "w-dag-1",
		SessionId: "s-dag-1",
		MaxSlots:  1,
	})
	_, _ = client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "w-dag-2",
		SessionId: "s-dag-2",
		MaxSlots:  1,
	})

	// Submit DAG: Root -> Child
	dagResp, err := coordClient.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-loss-test",
		TenantId: "tenant-dag-loss",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "dag-root", Priority: 10, MaxRetries: 2},
			{Id: "dag-child", Dependencies: []string{"dag-root"}, Priority: 5},
		},
	})
	if err != nil {
		t.Fatalf("submit dag failed: %v", err)
	}
	dagID := dagResp.GetDagId()

	// w-dag-1 pulls root task
	pull1, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "w-dag-1",
		SessionId:      "s-dag-1",
		AvailableSlots: 1,
	})
	if !pull1.GetHasTask() || pull1.GetAssignment().GetTaskId() != "dag-root" {
		t.Fatalf("expected dag-root pulled by w-dag-1")
	}

	// w-dag-1 crashes; coordinator sweeps and reclaims
	srv.SweepExpiredLeases()
	_, _ = store.ReclaimTask("dag-root")

	// w-dag-2 pulls dag-root (Epoch 2) and succeeds
	pull2, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "w-dag-2",
		SessionId:      "s-dag-2",
		AvailableSlots: 1,
	})
	if !pull2.GetHasTask() || pull2.GetAssignment().GetTaskId() != "dag-root" {
		t.Fatalf("expected dag-root pulled by w-dag-2")
	}

	_, _ = client.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   "w-dag-2",
		SessionId:  "s-dag-2",
		TaskId:     "dag-root",
		LeaseEpoch: pull2.GetAssignment().GetLeaseEpoch(),
	})

	_, err = client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "w-dag-2",
		SessionId:  "s-dag-2",
		TaskId:     "dag-root",
		LeaseEpoch: pull2.GetAssignment().GetLeaseEpoch(),
		Result:     []byte("root-ok"),
	})
	if err != nil {
		t.Fatalf("dag-root completion failed: %v", err)
	}

	// dag-child must now be unblocked to READY
	childTask, _ := store.GetTask("dag-child")
	if childTask.State != domain.StateReady {
		t.Fatalf("expected dag-child promoted to READY, got %s", childTask.State)
	}

	// w-dag-2 pulls dag-child and succeeds
	pullChild, _ := client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "w-dag-2",
		SessionId:      "s-dag-2",
		AvailableSlots: 1,
	})
	if !pullChild.GetHasTask() || pullChild.GetAssignment().GetTaskId() != "dag-child" {
		t.Fatalf("expected dag-child pulled by w-dag-2")
	}

	_, _ = client.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   "w-dag-2",
		SessionId:  "s-dag-2",
		TaskId:     "dag-child",
		LeaseEpoch: pullChild.GetAssignment().GetLeaseEpoch(),
	})

	_, _ = client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "w-dag-2",
		SessionId:  "s-dag-2",
		TaskId:     "dag-child",
		LeaseEpoch: pullChild.GetAssignment().GetLeaseEpoch(),
		Result:     []byte("child-ok"),
	})

	dagFinal, _ := coordClient.GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: dagID})
	if dagFinal.GetSucceededTasks() != 2 {
		t.Fatalf("expected 2 tasks succeeded in DAG, got %d", dagFinal.GetSucceededTasks())
	}
}

// TestLease_DuplicateReclamation verifies that concurrent or duplicate ReclaimTask calls
// are safely rejected without state machine corruption or duplicate attempt increments.
func TestLease_DuplicateReclamation(t *testing.T) {
	_, _, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-dup",
		SessionID:      "sess-dup",
		MaxSlots:       1,
		AvailableSlots: 1,
		Status:         domain.WorkerStatusHealthy,
	})

	_, _ = store.SubmitTask(&domain.Task{
		ID:             "task-dup-rec",
		TenantID:       "tenant-dup",
		MaxRetries:     3,
		IdempotencyKey: "dup-rec-key",
	})
	_, _ = store.MarkTaskReady("task-dup-rec")

	leased, _ := store.GrantTaskLease("task-dup-rec", "worker-dup", "sess-dup", 5*time.Second)
	_ = store.MarkTaskRunning("task-dup-rec", "worker-dup", "sess-dup", leased.LeaseEpoch)

	const goroutines = 10
	var wg sync.WaitGroup
	var successCount atomic.Int32

	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resState, err := store.ReclaimTask("task-dup-rec")
			if err == nil && resState == domain.StateReady {
				successCount.Add(1)
			}
		}()
	}
	wg.Wait()

	if successCount.Load() != 1 {
		t.Fatalf("expected exactly 1 reclamation to succeed, got %d", successCount.Load())
	}

	task, _ := store.GetTask("task-dup-rec")
	if task.AttemptCount != 1 {
		t.Fatalf("expected AttemptCount 1, got %d", task.AttemptCount)
	}
	if task.State != domain.StateReady {
		t.Fatalf("expected state READY, got %s", task.State)
	}
}

// TestLease_ConcurrentCompletionVsReclamation tests the race condition between CompleteTask
// and ReclaimTask across 50 iterations, verifying consistent serialization.
func TestLease_ConcurrentCompletionVsReclamation(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		_, _, store, cleanup := setupTestCoordinator(t, nil)

		workerID := fmt.Sprintf("worker-race-%d", iter)
		sessionID := fmt.Sprintf("sess-race-%d", iter)
		taskID := fmt.Sprintf("task-race-%d", iter)

		_ = store.RegisterWorker(&domain.Worker{
			ID:             workerID,
			SessionID:      sessionID,
			MaxSlots:       1,
			AvailableSlots: 1,
			Status:         domain.WorkerStatusHealthy,
		})

		_, _ = store.SubmitTask(&domain.Task{
			ID:             taskID,
			TenantID:       "tenant-race",
			MaxRetries:     2,
			IdempotencyKey: fmt.Sprintf("race-key-%d", iter),
		})
		_, _ = store.MarkTaskReady(taskID)

		leased, err := store.GrantTaskLease(taskID, workerID, sessionID, 5*time.Second)
		if err != nil {
			t.Fatalf("grant lease failed: %v", err)
		}
		_ = store.MarkTaskRunning(taskID, workerID, sessionID, leased.LeaseEpoch)

		var wg sync.WaitGroup
		wg.Add(2)

		// Goroutine 1: CompleteTask
		go func() {
			defer wg.Done()
			_ = store.CompleteTask(taskID, workerID, sessionID, leased.LeaseEpoch, []byte("race-result"))
		}()

		// Goroutine 2: ReclaimTask
		go func() {
			defer wg.Done()
			_, _ = store.ReclaimTask(taskID)
		}()

		wg.Wait()

		// Final state MUST be either SUCCEEDED (completion won) or READY (reclamation won)
		finalTask, err := store.GetTask(taskID)
		if err != nil {
			t.Fatalf("failed to get task: %v", err)
		}

		if finalTask.State != domain.StateSucceeded && finalTask.State != domain.StateReady {
			t.Fatalf("iteration %d: invalid race state %s", iter, finalTask.State)
		}

		cleanup()
	}
}

func getCoordinatorClient(t *testing.T, addr string) schedulerv1.CoordinatorServiceClient {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return schedulerv1.NewCoordinatorServiceClient(conn)
}
