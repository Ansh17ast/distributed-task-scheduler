package coordinator

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
	"distributed-scheduler/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func setupTestCoordinator(t *testing.T, policy scheduler.Policy) (*Server, string, *state.Store, func()) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on random port: %v", err)
	}

	cfg := config.DefaultCoordinatorConfig("coord-test", 0, 0, "127.0.0.1:0")
	cfg.WorkerLeaseDur = 2 * time.Second
	cfg.LeaseGraceWindow = 500 * time.Millisecond
	cfg.ReaperInterval = 100 * time.Millisecond

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := state.NewStore(storage.NewMemoryAuditStore())

	srv := NewServer(cfg, store, policy, logger)
	go func() {
		_ = srv.Start(lis)
	}()

	cleanup := func() {
		srv.Stop()
		_ = lis.Close()
	}

	return srv, lis.Addr().String(), store, cleanup
}

func TestCoordinator_RegistrationAndSlotAccounting(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial coordinator: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewWorkerServiceClient(conn)
	ctx := context.Background()

	// 1. Register worker with 4 slots
	regResp, err := client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "worker-1",
		Address:  "127.0.0.1:9099",
		MaxSlots: 4,
	})
	if err != nil || !regResp.GetAccepted() {
		t.Fatalf("failed to register worker: %v", err)
	}

	// 2. Verify state store has 4 available slots
	w, err := store.GetWorker("worker-1")
	if err != nil {
		t.Fatalf("worker not found in store: %v", err)
	}
	if w.MaxSlots != 4 || w.AvailableSlots != 4 {
		t.Errorf("expected 4/4 slots, got %d max, %d avail", w.MaxSlots, w.AvailableSlots)
	}
}

func TestCoordinator_WorkerPullExecutionLifecycle(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, &scheduler.PriorityPolicy{})
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("failed to dial: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// 1. Start operational worker daemon
	workerCfg := config.DefaultWorkerConfig("worker-alpha", addr)
	workerCfg.MaxSlots = 4

	var taskExecuted atomic.Bool
	handler := func(ctx context.Context, payload []byte) ([]byte, error) {
		taskExecuted.Store(true)
		return []byte("execution-output-ok"), nil
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	daemon := worker.NewDaemon(workerCfg, handler, logger)
	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("failed to start worker daemon: %v", err)
	}
	defer daemon.Stop()

	// 2. Submit task
	submitResp, err := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:             "task-lifecycle-1",
			TenantId:       "tenant-main",
			Priority:       100,
			MaxRetries:     3,
			TimeoutSeconds: 5,
			Payload:        []byte("hello-distributed-task"),
		},
	})
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}
	if submitResp.GetTaskId() != "task-lifecycle-1" {
		t.Errorf("expected task-lifecycle-1, got %s", submitResp.GetTaskId())
	}

	// 3. Await worker to pull and complete the task
	deadline := time.Now().Add(3 * time.Second)
	var finalTask *domain.Task
	for time.Now().Before(deadline) {
		tCheck, err := store.GetTask("task-lifecycle-1")
		if err == nil && tCheck.State == domain.StateSucceeded {
			finalTask = tCheck
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if finalTask == nil {
		t.Fatalf("timed out waiting for task to reach SUCCEEDED state")
	}
	if !taskExecuted.Load() {
		t.Errorf("worker handler was not executed")
	}
	if string(finalTask.Result) != "execution-output-ok" {
		t.Errorf("expected output execution-output-ok, got %s", string(finalTask.Result))
	}
	if finalTask.LeaseEpoch != 1 {
		t.Errorf("expected LeaseEpoch 1, got %d", finalTask.LeaseEpoch)
	}

	// 4. Assert worker slots returned to full capacity (4)
	w, err := store.GetWorker("worker-alpha")
	if err != nil || w.AvailableSlots != 4 {
		t.Errorf("expected 4 available slots after task completion, got %d", w.AvailableSlots)
	}
}

func TestCoordinator_ExponentialBackoffRetry(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// Handler fails on attempt 1, succeeds on attempt 2
	var attempts atomic.Int32
	handler := func(ctx context.Context, payload []byte) ([]byte, error) {
		attempt := attempts.Add(1)
		if attempt == 1 {
			return nil, errors.New("simulated transient failure")
		}
		return []byte("retry-succeeded"), nil
	}

	workerCfg := config.DefaultWorkerConfig("worker-retry-test", addr)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	daemon := worker.NewDaemon(workerCfg, handler, logger)
	if err := daemon.Start(ctx); err != nil {
		t.Fatalf("failed to start worker: %v", err)
	}
	defer daemon.Stop()

	// Submit retryable task
	_, err = coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:             "task-retry-01",
			TenantId:       "tenant-retry",
			Priority:       50,
			MaxRetries:     3,
			TimeoutSeconds: 5,
		},
	})
	if err != nil {
		t.Fatalf("submission failed: %v", err)
	}

	// Await task completion after retry backoff
	deadline := time.Now().Add(4 * time.Second)
	var finalTask *domain.Task
	for time.Now().Before(deadline) {
		tCheck, err := store.GetTask("task-retry-01")
		if err == nil && tCheck.State == domain.StateSucceeded {
			finalTask = tCheck
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if finalTask == nil {
		t.Fatalf("task did not succeed after retry")
	}
	if attempts.Load() != 2 {
		t.Errorf("expected exactly 2 execution attempts, got %d", attempts.Load())
	}
	if string(finalTask.Result) != "retry-succeeded" {
		t.Errorf("expected retry-succeeded, got %s", string(finalTask.Result))
	}
}

func TestCoordinator_TenantQuotaBackpressure(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	// Set tenant limit: MaxQueueDepth = 2
	store.SetTenantQuota(&domain.TenantQuota{
		TenantID:       "tenant-restricted",
		MaxConcurrency: 2,
		MaxQueueDepth:  2,
	})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// Task 1: OK
	_, err = client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: "t1", TenantId: "tenant-restricted", Priority: 10},
	})
	if err != nil {
		t.Fatalf("task 1 failed: %v", err)
	}

	// Task 2: OK
	_, err = client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: "t2", TenantId: "tenant-restricted", Priority: 10},
	})
	if err != nil {
		t.Fatalf("task 2 failed: %v", err)
	}

	// Task 3: Exceeds MaxQueueDepth (2) -> must return ResourceExhausted
	_, err = client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: "t3", TenantId: "tenant-restricted", Priority: 10},
	})
	if err == nil {
		t.Fatalf("expected ResourceExhausted for task 3, got nil error")
	}
	st, ok := status.FromError(err)
	if !ok || st.Code() != codes.ResourceExhausted {
		t.Errorf("expected codes.ResourceExhausted, got %v", err)
	}
}

func TestCoordinator_DuplicateTaskSubmissionIdempotency(t *testing.T) {
	_, addr, _, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	client := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// First submission
	resp1, err := client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:             "task-orig",
			TenantId:       "tenant-idem",
			IdempotencyKey: "unique-idem-token-123",
		},
	})
	if err != nil {
		t.Fatalf("first submission failed: %v", err)
	}

	// Second duplicate submission with different Task ID but same IdempotencyKey
	resp2, err := client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:             "task-duplicate-attempt",
			TenantId:       "tenant-idem",
			IdempotencyKey: "unique-idem-token-123",
		},
	})
	if err != nil {
		t.Fatalf("second submission failed: %v", err)
	}

	// Must return the original task ID idempotently
	if resp2.GetTaskId() != resp1.GetTaskId() {
		t.Errorf("idempotency broken! expected task id %s, got %s", resp1.GetTaskId(), resp2.GetTaskId())
	}
}

func TestCoordinator_ConcurrentWorkerPulls(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, &scheduler.PriorityPolicy{})
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// Submit 20 distinct tasks
	taskCount := 20
	for i := 0; i < taskCount; i++ {
		_, err := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{
				Id:             fmt.Sprintf("concurrent-task-%02d", i),
				TenantId:       "tenant-concurrent",
				Priority:       int32(i),
				TimeoutSeconds: 5,
			},
		})
		if err != nil {
			t.Fatalf("failed submitting task %d: %v", i, err)
		}
	}

	// Spawn 3 worker daemons with 2 slots each = 6 concurrent execution slots
	var executedTasks sync.Map
	handler := func(ctx context.Context, payload []byte) ([]byte, error) {
		time.Sleep(30 * time.Millisecond)
		return []byte("done"), nil
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	var daemons []*worker.Daemon
	for wIdx := 1; wIdx <= 3; wIdx++ {
		wCfg := config.DefaultWorkerConfig(fmt.Sprintf("worker-pool-%d", wIdx), addr)
		wCfg.MaxSlots = 2
		d := worker.NewDaemon(wCfg, func(ctx context.Context, payload []byte) ([]byte, error) {
			executedTasks.Store(fmt.Sprintf("%p", ctx), true)
			return handler(ctx, payload)
		}, logger)
		if err := d.Start(ctx); err != nil {
			t.Fatalf("failed to start worker %d: %v", wIdx, err)
		}
		daemons = append(daemons, d)
	}
	defer func() {
		for _, d := range daemons {
			d.Stop()
		}
	}()

	// Await all 20 tasks to reach SUCCEEDED
	deadline := time.Now().Add(6 * time.Second)
	allSucceeded := false
	for time.Now().Before(deadline) {
		succeededCount := 0
		for i := 0; i < taskCount; i++ {
			tCheck, err := store.GetTask(fmt.Sprintf("concurrent-task-%02d", i))
			if err == nil && tCheck.State == domain.StateSucceeded {
				succeededCount++
			}
		}
		if succeededCount == taskCount {
			allSucceeded = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	if !allSucceeded {
		t.Fatalf("not all 20 concurrent tasks reached SUCCEEDED within deadline")
	}

	// Verify each task has LeaseEpoch == 1 (no duplicate assignment)
	for i := 0; i < taskCount; i++ {
		tCheck, _ := store.GetTask(fmt.Sprintf("concurrent-task-%02d", i))
		if tCheck.LeaseEpoch != 1 {
			t.Errorf("task %s was assigned multiple times! LeaseEpoch = %d", tCheck.ID, tCheck.LeaseEpoch)
		}
	}
}

func TestCoordinator_WorkerDisappearanceAndLeaseReclamation(t *testing.T) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}

	cfg := config.DefaultCoordinatorConfig("coord-reclaim-test", 0, 0, "127.0.0.1:0")
	cfg.WorkerLeaseDur = 150 * time.Millisecond
	cfg.LeaseGraceWindow = 50 * time.Millisecond
	cfg.ReaperInterval = 40 * time.Millisecond

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := state.NewStore(storage.NewMemoryAuditStore())

	srv := NewServer(cfg, store, &scheduler.PriorityPolicy{}, logger)
	go func() { _ = srv.Start(lis) }()
	defer func() {
		srv.Stop()
		_ = lis.Close()
	}()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	workerClient := schedulerv1.NewWorkerServiceClient(conn)
	ctx := context.Background()

	// 1. Worker 1 registers
	_, err = workerClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "worker-failing",
		MaxSlots: 2,
	})
	if err != nil {
		t.Fatalf("failed to register worker 1: %v", err)
	}

	// 2. Submit a task
	_, err = coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:             "task-reclaim-01",
			TenantId:       "tenant-reclaim",
			Priority:       50,
			MaxRetries:     3,
			TimeoutSeconds: 5,
		},
	})
	if err != nil {
		t.Fatalf("submit task failed: %v", err)
	}

	// 3. Worker 1 pulls task and enters RUNNING
	pullResp, err := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-failing"})
	if err != nil || !pullResp.GetHasTask() {
		t.Fatalf("expected task for worker 1: %v", err)
	}
	taskAssignment := pullResp.GetAssignment()
	if taskAssignment.GetTaskId() != "task-reclaim-01" {
		t.Fatalf("expected task-reclaim-01, got %s", taskAssignment.GetTaskId())
	}
	if taskAssignment.GetLeaseEpoch() != 1 {
		t.Errorf("expected epoch 1, got %d", taskAssignment.GetLeaseEpoch())
	}

	_, err = workerClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		TaskId:     "task-reclaim-01",
		WorkerId:   "worker-failing",
		LeaseEpoch: 1,
	})
	if err != nil {
		t.Fatalf("ack running failed: %v", err)
	}

	// 4. Worker 1 crashes (stops heartbeating). Wait for lease timeout + reaper sweep.
	time.Sleep(300 * time.Millisecond)

	// 5. Assert Worker 1 is marked DEAD and task is reclaimed back to READY
	w1, err := store.GetWorker("worker-failing")
	if err != nil || w1.Status != domain.WorkerStatusDead {
		t.Errorf("expected worker 1 to be DEAD, got status %v", w1.Status)
	}

	tReclaimed, err := store.GetTask("task-reclaim-01")
	if err != nil || tReclaimed.State != domain.StateReady {
		t.Fatalf("expected task-reclaim-01 to be READY, got %v", tReclaimed.State)
	}

	// 6. Worker 2 registers and pulls the reclaimed task
	_, err = workerClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "worker-healthy",
		MaxSlots: 2,
	})
	if err != nil {
		t.Fatalf("failed to register worker 2: %v", err)
	}

	pullResp2, err := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-healthy"})
	if err != nil || !pullResp2.GetHasTask() {
		t.Fatalf("expected worker 2 to pull reclaimed task: %v", err)
	}
	if pullResp2.GetAssignment().GetLeaseEpoch() != 2 {
		t.Errorf("expected incremented LeaseEpoch 2, got %d", pullResp2.GetAssignment().GetLeaseEpoch())
	}

	// 7. Worker 2 acks task running then successfully completes task
	_, err = workerClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		TaskId:     "task-reclaim-01",
		WorkerId:   "worker-healthy",
		LeaseEpoch: 2,
	})
	if err != nil {
		t.Fatalf("worker 2 ack running failed: %v", err)
	}

	_, err = workerClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		TaskId:     "task-reclaim-01",
		WorkerId:   "worker-healthy",
		LeaseEpoch: 2,
		Result:     []byte("reclaimed-and-completed"),
	})
	if err != nil {
		t.Fatalf("worker 2 completion failed: %v", err)
	}

	finalTask, _ := store.GetTask("task-reclaim-01")
	if finalTask.State != domain.StateSucceeded {
		t.Errorf("expected SUCCEEDED, got %v", finalTask.State)
	}
}

func TestCoordinator_WorkerCapacitySlotAccountingInvariant(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, &scheduler.PriorityPolicy{})
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	workerClient := schedulerv1.NewWorkerServiceClient(conn)
	ctx := context.Background()

	// Register worker with 3 slots
	const maxSlots = int32(3)
	_, err = workerClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "worker-slots",
		MaxSlots: maxSlots,
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	assertSlots := func(expected int32, step string) {
		w, err := store.GetWorker("worker-slots")
		if err != nil {
			t.Fatalf("[%s] worker not found: %v", step, err)
		}
		if w.AvailableSlots != expected {
			t.Errorf("[%s] expected AvailableSlots=%d, got %d", step, expected, w.AvailableSlots)
		}
		if w.AvailableSlots < 0 || w.AvailableSlots > w.MaxSlots {
			t.Errorf("[%s] INVARIANT VIOLATION: AvailableSlots=%d out of [0, %d]", step, w.AvailableSlots, w.MaxSlots)
		}
	}

	assertSlots(3, "Initial")

	// Submit 3 tasks
	for i := 1; i <= 3; i++ {
		_, err := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{
				Id:       fmt.Sprintf("slot-task-%d", i),
				TenantId: "tenant-slots",
				Priority: int32(i),
			},
		})
		if err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	// Pull 1st task
	p1, _ := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-slots"})
	if !p1.GetHasTask() {
		t.Fatalf("p1 expected task")
	}
	assertSlots(2, "After 1st Pull")

	// Pull 2nd task
	p2, _ := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-slots"})
	if !p2.GetHasTask() {
		t.Fatalf("p2 expected task")
	}
	assertSlots(1, "After 2nd Pull")

	// Pull 3rd task
	p3, _ := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-slots"})
	if !p3.GetHasTask() {
		t.Fatalf("p3 expected task")
	}
	assertSlots(0, "After 3rd Pull (Saturated)")

	// 4th pull: Worker is fully saturated (0 slots available)
	p4, err := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-slots"})
	if err != nil || p4.GetHasTask() {
		t.Errorf("expected HasTask=false when worker is saturated")
	}
	assertSlots(0, "After Saturated Pull")

	// Complete 1st task -> releases 1 slot
	_, _ = workerClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		TaskId:     p1.GetAssignment().GetTaskId(),
		WorkerId:   "worker-slots",
		LeaseEpoch: p1.GetAssignment().GetLeaseEpoch(),
	})
	_, err = workerClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		TaskId:     p1.GetAssignment().GetTaskId(),
		WorkerId:   "worker-slots",
		LeaseEpoch: p1.GetAssignment().GetLeaseEpoch(),
	})
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}
	assertSlots(1, "After Completing 1 Task")

	// Cancel 2nd task -> releases 1 slot
	_, err = coordClient.CancelTask(ctx, &schedulerv1.CancelTaskRequest{
		TaskId: p2.GetAssignment().GetTaskId(),
		Reason: "user cancelled",
	})
	if err != nil {
		t.Fatalf("cancel failed: %v", err)
	}
	assertSlots(2, "After Cancelling 2nd Task")

	// Fail 3rd task with non-retryable error -> releases 1 slot
	_, _ = workerClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		TaskId:     p3.GetAssignment().GetTaskId(),
		WorkerId:   "worker-slots",
		LeaseEpoch: p3.GetAssignment().GetLeaseEpoch(),
	})
	_, err = workerClient.ReportTaskFailed(ctx, &schedulerv1.ReportTaskFailedRequest{
		TaskId:       p3.GetAssignment().GetTaskId(),
		WorkerId:     "worker-slots",
		LeaseEpoch:   p3.GetAssignment().GetLeaseEpoch(),
		ErrorMessage: "fatal business error",
		Retryable:    false,
	})
	if err != nil {
		t.Fatalf("fail failed: %v", err)
	}
	assertSlots(3, "After Failing 3rd Task Non-Retryable")
}

func TestCoordinator_TenantConcurrencyQuotaEnforcement(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, &scheduler.PriorityPolicy{})
	defer cleanup()

	// Tenant A limited to concurrency 2
	store.SetTenantQuota(&domain.TenantQuota{
		TenantID:       "tenant-limited",
		MaxConcurrency: 2,
		MaxQueueDepth:  10,
	})

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	workerClient := schedulerv1.NewWorkerServiceClient(conn)
	ctx := context.Background()

	// Worker has 10 available slots
	_, err = workerClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "worker-quota-test",
		MaxSlots: 10,
	})
	if err != nil {
		t.Fatalf("register failed: %v", err)
	}

	// Submit 3 tasks for tenant-limited
	for i := 1; i <= 3; i++ {
		_, err := coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{
				Id:       fmt.Sprintf("limited-task-%d", i),
				TenantId: "tenant-limited",
				Priority: int32(i * 10),
			},
		})
		if err != nil {
			t.Fatalf("submit failed: %v", err)
		}
	}

	// Submit 1 task for tenant-unlimited
	_, err = coordClient.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:       "unlimited-task-1",
			TenantId: "tenant-unlimited",
			Priority: 5, // Lower priority than limited-task-1/2/3
		},
	})
	if err != nil {
		t.Fatalf("submit failed: %v", err)
	}

	// Pull 1: should receive limited-task-3 (highest priority)
	p1, _ := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-quota-test"})
	if !p1.GetHasTask() || p1.GetAssignment().GetTaskId() != "limited-task-3" {
		t.Fatalf("p1 expected limited-task-3, got %v", p1.GetAssignment())
	}
	_, _ = workerClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		TaskId:     p1.GetAssignment().GetTaskId(),
		WorkerId:   "worker-quota-test",
		LeaseEpoch: p1.GetAssignment().GetLeaseEpoch(),
	})

	// Pull 2: should receive limited-task-2 (second highest priority)
	p2, _ := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-quota-test"})
	if !p2.GetHasTask() || p2.GetAssignment().GetTaskId() != "limited-task-2" {
		t.Fatalf("p2 expected limited-task-2, got %v", p2.GetAssignment())
	}
	_, _ = workerClient.AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		TaskId:     p2.GetAssignment().GetTaskId(),
		WorkerId:   "worker-quota-test",
		LeaseEpoch: p2.GetAssignment().GetLeaseEpoch(),
	})

	// At this point, tenant-limited has 2 active running tasks == MaxConcurrency (2).
	// Pull 3: despite limited-task-1 having higher priority (10) than unlimited-task-1 (5),
	// limited-task-1 MUST be bypassed due to tenant concurrency limit!
	p3, _ := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-quota-test"})
	if !p3.GetHasTask() {
		t.Fatalf("p3 expected task for tenant-unlimited")
	}
	if p3.GetAssignment().GetTaskId() != "unlimited-task-1" {
		t.Errorf("fairness violation! expected unlimited-task-1 to avoid starvation, got %s", p3.GetAssignment().GetTaskId())
	}

	// Complete one task of tenant-limited
	_, err = workerClient.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		TaskId:     "limited-task-3",
		WorkerId:   "worker-quota-test",
		LeaseEpoch: p1.GetAssignment().GetLeaseEpoch(),
	})
	if err != nil {
		t.Fatalf("complete failed: %v", err)
	}

	// Pull 4: now that tenant-limited has 1 active running (< 2), limited-task-1 can be dispatched!
	p4, _ := workerClient.PullTask(ctx, &schedulerv1.PullTaskRequest{WorkerId: "worker-quota-test"})
	if !p4.GetHasTask() || p4.GetAssignment().GetTaskId() != "limited-task-1" {
		t.Errorf("p4 expected limited-task-1 after quota capacity freed, got %v", p4.GetAssignment())
	}
}
