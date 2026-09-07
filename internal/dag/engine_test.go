package dag

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
)

func setupTestStoreAndEngine() (*state.Store, *Engine) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	store := state.NewStore(storage.NewMemoryAuditStore())
	engine := NewEngine(store, logger)
	return store, engine
}

func submitTestDAG(t *testing.T, store *state.Store, dagID string, tasks []*domain.Task) {
	if err := ValidateDAG(dagID, "tenant-dag-test", tasks); err != nil {
		t.Fatalf("validation failed: %v", err)
	}

	dagObj := &domain.DAG{
		ID:        dagID,
		TenantID:  "tenant-dag-test",
		CreatedAt: time.Now(),
	}

	if err := store.SubmitDAG(dagObj, tasks); err != nil {
		t.Fatalf("submit dag failed: %v", err)
	}
}

func simulateExecuteAndComplete(t *testing.T, store *state.Store, engine *Engine, taskID string, workerID string) {
	ctx := context.Background()

	// 1. Grant lease
	leased, err := store.GrantTaskLease(taskID, workerID, "sess-test", 5*time.Second)
	if err != nil {
		t.Fatalf("failed granting lease for %s: %v", taskID, err)
	}

	// 2. Ack running
	if err := store.MarkTaskRunning(taskID, workerID, "sess-test", leased.LeaseEpoch); err != nil {
		t.Fatalf("failed ack running for %s: %v", taskID, err)
	}

	// 3. Complete task
	if err := store.CompleteTask(taskID, workerID, "sess-test", leased.LeaseEpoch, []byte("result-"+taskID)); err != nil {
		t.Fatalf("failed completing %s: %v", taskID, err)
	}

	// 4. Trigger DAG engine
	task, _ := store.GetTask(taskID)
	_, _ = engine.OnTaskCompleted(ctx, task)
}

func TestDAGEngine_DiamondFanOutFanIn(t *testing.T) {
	store, engine := setupTestStoreAndEngine()
	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-1",
		MaxSlots:       10,
		AvailableSlots: 10,
		Status:         domain.WorkerStatusHealthy,
	})

	// Diamond: A -> (B, C) -> D
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}},
		{ID: "task-C", Dependencies: []string{"task-A"}},
		{ID: "task-D", Dependencies: []string{"task-B", "task-C"}},
	}
	submitTestDAG(t, store, "dag-diamond-1", tasks)

	// Step 1: Initial state
	tA, _ := store.GetTask("task-A")
	tB, _ := store.GetTask("task-B")
	tC, _ := store.GetTask("task-C")
	tD, _ := store.GetTask("task-D")

	if tA.State != domain.StateReady {
		t.Errorf("expected A to be READY, got %v", tA.State)
	}
	if tB.State != domain.StateBlocked || tC.State != domain.StateBlocked || tD.State != domain.StateBlocked {
		t.Errorf("expected B, C, D to be BLOCKED initially")
	}

	// Step 2: Execute A to SUCCEEDED
	simulateExecuteAndComplete(t, store, engine, "task-A", "worker-1")

	// Verify B and C are now READY, while D is still BLOCKED
	tB, _ = store.GetTask("task-B")
	tC, _ = store.GetTask("task-C")
	tD, _ = store.GetTask("task-D")
	if tB.State != domain.StateReady || tC.State != domain.StateReady {
		t.Errorf("expected B and C to become READY after A completed, got B=%v, C=%v", tB.State, tC.State)
	}
	if tD.State != domain.StateBlocked {
		t.Errorf("expected D to remain BLOCKED, got %v", tD.State)
	}

	// Step 3: Execute B to SUCCEEDED
	simulateExecuteAndComplete(t, store, engine, "task-B", "worker-1")

	// D must still be BLOCKED because C has not completed!
	tD, _ = store.GetTask("task-D")
	if tD.State != domain.StateBlocked {
		t.Errorf("fan-in violation: expected D to remain BLOCKED until C finishes, got %v", tD.State)
	}

	// Step 4: Execute C to SUCCEEDED
	simulateExecuteAndComplete(t, store, engine, "task-C", "worker-1")

	// D has all dependencies satisfied; must now be READY!
	tD, _ = store.GetTask("task-D")
	if tD.State != domain.StateReady {
		t.Errorf("expected D to become READY after both B and C completed, got %v", tD.State)
	}

	// Step 5: Execute D to SUCCEEDED
	simulateExecuteAndComplete(t, store, engine, "task-D", "worker-1")
	tD, _ = store.GetTask("task-D")
	if tD.State != domain.StateSucceeded {
		t.Errorf("expected D to be SUCCEEDED, got %v", tD.State)
	}
}

func TestDAGEngine_FailurePropagation(t *testing.T) {
	store, engine := setupTestStoreAndEngine()
	ctx := context.Background()
	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-fail",
		MaxSlots:       10,
		AvailableSlots: 10,
		Status:         domain.WorkerStatusHealthy,
	})

	// Pipeline: A -> B -> C -> D
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}, MaxRetries: 0},
		{ID: "task-C", Dependencies: []string{"task-B"}},
		{ID: "task-D", Dependencies: []string{"task-C"}},
	}
	submitTestDAG(t, store, "dag-fail-prop", tasks)

	// Complete A
	simulateExecuteAndComplete(t, store, engine, "task-A", "worker-fail")

	// B is now READY. Lease and Fail B permanently (MaxRetries = 0)
	leasedB, _ := store.GrantTaskLease("task-B", "worker-fail", "sess-test", 5*time.Second)
	_ = store.MarkTaskRunning("task-B", "worker-fail", "sess-test", leasedB.LeaseEpoch)
	willRetry, err := store.FailTask("task-B", "worker-fail", "sess-test", leasedB.LeaseEpoch, "fatal runtime error", false)
	if err != nil || willRetry {
		t.Fatalf("expected permanent failure for B: willRetry=%v, err=%v", willRetry, err)
	}

	// Trigger failure propagation
	tB, _ := store.GetTask("task-B")
	cancelled, err := engine.OnTaskFailed(ctx, tB)
	if err != nil {
		t.Fatalf("OnTaskFailed error: %v", err)
	}

	if len(cancelled) != 2 {
		t.Fatalf("expected 2 tasks cancelled (C and D), got %v", cancelled)
	}

	tC, _ := store.GetTask("task-C")
	tD, _ := store.GetTask("task-D")
	if tC.State != domain.StateCancelled || tD.State != domain.StateCancelled {
		t.Errorf("expected C and D to be CANCELLED, got C=%v, D=%v", tC.State, tD.State)
	}
}

func TestDAGEngine_RetryInteraction(t *testing.T) {
	store, engine := setupTestStoreAndEngine()
	ctx := context.Background()
	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-retry",
		MaxSlots:       10,
		AvailableSlots: 10,
		Status:         domain.WorkerStatusHealthy,
	})

	// A -> B -> C (B allows 2 retries)
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}, MaxRetries: 2},
		{ID: "task-C", Dependencies: []string{"task-B"}},
	}
	submitTestDAG(t, store, "dag-retry-test", tasks)

	simulateExecuteAndComplete(t, store, engine, "task-A", "worker-retry")

	// B is READY. Lease and fail B retryably (attempt 1)
	leasedB, _ := store.GrantTaskLease("task-B", "worker-retry", "sess-test", 5*time.Second)
	_ = store.MarkTaskRunning("task-B", "worker-retry", "sess-test", leasedB.LeaseEpoch)
	willRetry, err := store.FailTask("task-B", "worker-retry", "sess-test", leasedB.LeaseEpoch, "transient error", true)
	if err != nil || !willRetry {
		t.Fatalf("expected willRetry=true for B: %v", err)
	}

	// Assert B is in RETRY_WAIT and C is NOT cancelled (stays BLOCKED)
	tB, _ := store.GetTask("task-B")
	tC, _ := store.GetTask("task-C")
	if tB.State != domain.StateRetryWait {
		t.Errorf("expected B in RETRY_WAIT, got %v", tB.State)
	}
	if tC.State != domain.StateBlocked {
		t.Errorf("expected C to remain BLOCKED during retry wait, got %v", tC.State)
	}

	// Retry B: promote back to READY and complete successfully
	_, _ = store.MarkTaskReady("task-B")
	leasedB2, _ := store.GrantTaskLease("task-B", "worker-retry", "sess-test", 5*time.Second)
	_ = store.MarkTaskRunning("task-B", "worker-retry", "sess-test", leasedB2.LeaseEpoch)
	_ = store.CompleteTask("task-B", "worker-retry", "sess-test", leasedB2.LeaseEpoch, []byte("retry-success"))

	tB, _ = store.GetTask("task-B")
	_, _ = engine.OnTaskCompleted(ctx, tB)

	// Now C must be promoted to READY!
	tC, _ = store.GetTask("task-C")
	if tC.State != domain.StateReady {
		t.Errorf("expected C to become READY after B retry succeeded, got %v", tC.State)
	}
}

func TestDAGEngine_BranchCancellation(t *testing.T) {
	store, engine := setupTestStoreAndEngine()
	ctx := context.Background()
	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-cancel",
		MaxSlots:       10,
		AvailableSlots: 10,
		Status:         domain.WorkerStatusHealthy,
	})

	// Diamond: A -> (B, C) -> D
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}},
		{ID: "task-C", Dependencies: []string{"task-A"}},
		{ID: "task-D", Dependencies: []string{"task-B", "task-C"}},
	}
	submitTestDAG(t, store, "dag-cancel-branch", tasks)

	simulateExecuteAndComplete(t, store, engine, "task-A", "worker-cancel")

	// Cancel branch B
	tB, _ := store.GetTask("task-B")
	_ = store.CancelTask("task-B", "user cancelled branch B")
	cancelled, err := engine.OnTaskCancelled(ctx, tB)
	if err != nil {
		t.Fatalf("OnTaskCancelled error: %v", err)
	}

	// D must be transitively cancelled
	if len(cancelled) != 1 || cancelled[0] != "task-D" {
		t.Errorf("expected task-D cancelled, got %v", cancelled)
	}
	tD, _ := store.GetTask("task-D")
	if tD.State != domain.StateCancelled {
		t.Errorf("expected D to be CANCELLED, got %v", tD.State)
	}

	// Sibling branch C can still complete independently
	simulateExecuteAndComplete(t, store, engine, "task-C", "worker-cancel")
	tC, _ := store.GetTask("task-C")
	if tC.State != domain.StateSucceeded {
		t.Errorf("expected C to be SUCCEEDED, got %v", tC.State)
	}
}

func TestDAGEngine_ConcurrentUpstreamCompletion(t *testing.T) {
	store, engine := setupTestStoreAndEngine()
	ctx := context.Background()

	const upstreamCount = 30
	var tasks []*domain.Task
	var upIDs []string

	for i := 0; i < upstreamCount; i++ {
		uID := fmt.Sprintf("up-%02d", i)
		tasks = append(tasks, &domain.Task{ID: uID})
		upIDs = append(upIDs, uID)
	}
	tasks = append(tasks, &domain.Task{
		ID:           "fanin-target",
		Dependencies: upIDs,
	})

	submitTestDAG(t, store, "dag-concurrent-fanin", tasks)

	store.SetTenantQuota(&domain.TenantQuota{
		TenantID:       "tenant-dag-test",
		MaxConcurrency: 100,
		MaxQueueDepth:  1000,
	})

	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-concurrent",
		MaxSlots:       int32(upstreamCount + 5),
		AvailableSlots: int32(upstreamCount + 5),
		Status:         domain.WorkerStatusHealthy,
	})

	// Pre-lease and ack all 30 upstream tasks
	type activeTask struct {
		id    string
		epoch uint64
	}
	var actives []activeTask
	for _, uID := range upIDs {
		leased, err := store.GrantTaskLease(uID, "worker-concurrent", "sess-test", 5*time.Second)
		if err != nil {
			t.Fatalf("grant lease failed: %v", err)
		}
		_ = store.MarkTaskRunning(uID, "worker-concurrent", "sess-test", leased.LeaseEpoch)
		actives = append(actives, activeTask{id: uID, epoch: leased.LeaseEpoch})
	}

	// Simultaneously complete all 30 tasks from 30 goroutines
	var wg sync.WaitGroup
	for _, a := range actives {
		wg.Add(1)
		go func(act activeTask) {
			defer wg.Done()
			_ = store.CompleteTask(act.id, "worker-concurrent", "sess-test", act.epoch, []byte("done"))
			tObj, _ := store.GetTask(act.id)
			_, _ = engine.OnTaskCompleted(ctx, tObj)
		}(a)
	}
	wg.Wait()

	// Verify fanin-target became READY exactly once and is ready for scheduling
	target, err := store.GetTask("fanin-target")
	if err != nil {
		t.Fatalf("failed to get target: %v", err)
	}
	if target.State != domain.StateReady {
		t.Errorf("expected target to be READY after all 30 completed, got %v", target.State)
	}
}

func TestDAGEngine_DuplicateCompletionIdempotency(t *testing.T) {
	store, engine := setupTestStoreAndEngine()
	ctx := context.Background()
	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-idem",
		MaxSlots:       4,
		AvailableSlots: 4,
		Status:         domain.WorkerStatusHealthy,
	})

	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}},
	}
	submitTestDAG(t, store, "dag-idem", tasks)

	simulateExecuteAndComplete(t, store, engine, "task-A", "worker-idem")

	tA, _ := store.GetTask("task-A")

	// Call OnTaskCompleted 5 more times for task-A
	for i := 0; i < 5; i++ {
		promoted, err := engine.OnTaskCompleted(ctx, tA)
		if err != nil {
			t.Errorf("subsequent call returned error: %v", err)
		}
		if len(promoted) != 0 {
			t.Errorf("expected 0 promoted on idempotent re-evaluation, got %v", promoted)
		}
	}

	tB, _ := store.GetTask("task-B")
	if tB.State != domain.StateReady {
		t.Errorf("expected task-B to remain READY, got %v", tB.State)
	}
}

func TestDAGEngine_AdversarialConflictingCompletionVsCancellation(t *testing.T) {
	store, engine := setupTestStoreAndEngine()
	ctx := context.Background()

	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-adv",
		MaxSlots:       4,
		AvailableSlots: 4,
		Status:         domain.WorkerStatusHealthy,
	})

	tasks := []*domain.Task{
		{ID: "task-root"},
		{ID: "task-target", Dependencies: []string{"task-root"}},
	}
	submitTestDAG(t, store, "dag-adversarial", tasks)

	leasedRoot, _ := store.GrantTaskLease("task-root", "worker-adv", "sess-test", 5*time.Second)
	_ = store.MarkTaskRunning("task-root", "worker-adv", "sess-test", leasedRoot.LeaseEpoch)

	var wg sync.WaitGroup
	wg.Add(2)

	// Goroutine 1: Completes task-root and calls OnTaskCompleted (trying to promote task-target to READY)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = store.CompleteTask("task-root", "worker-adv", "sess-test", leasedRoot.LeaseEpoch, []byte("ok"))
		tRoot, _ := store.GetTask("task-root")
		_, _ = engine.OnTaskCompleted(ctx, tRoot)
	}()

	// Goroutine 2: Cancels task-target directly (moving it to CANCELLED)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)
		_ = store.CancelTask("task-target", "concurrent cancel request")
	}()

	wg.Wait()

	// Assert final state of task-target: must be either READY or CANCELLED, never invalid or corrupt
	target, err := store.GetTask("task-target")
	if err != nil {
		t.Fatalf("failed getting target: %v", err)
	}

	if target.State != domain.StateReady && target.State != domain.StateCancelled {
		t.Errorf("adversarial invariant violated: task-target in unexpected state %v", target.State)
	}
}
