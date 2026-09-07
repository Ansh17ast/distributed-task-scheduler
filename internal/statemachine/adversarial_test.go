package statemachine

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"distributed-scheduler/internal/domain"
)

// TestAdversarial_LeaseExpirationVsCompletion tests the classic distributed race:
// The coordinator reclaims an expired task while the worker simultaneously reports completion.
func TestAdversarial_LeaseExpirationVsCompletion(t *testing.T) {
	engine := NewTransitionEngine()
	now := time.Now()

	task := &domain.Task{
		ID:               "task-race-01",
		State:            domain.StateRunning,
		AssignedWorkerID: "worker-1",
		LeaseEpoch:       1,
		AttemptCount:     1,
		MaxRetries:       3,
		CreatedAt:        now,
	}

	// 1. Lease reaper runs first: expires lease and reclaims task
	err := engine.ApplyTransition(task, TransitionRequest{
		CommandType: "RECLAIM_TASK",
		TargetState: domain.StateLost,
	})
	if err != nil {
		t.Fatalf("reclamation failed: %v", err)
	}

	// Task enters RECOVERING -> READY (epoch incremented for reassignment)
	_ = engine.ApplyTransition(task, TransitionRequest{TargetState: domain.StateRecovering})
	_ = engine.ApplyTransition(task, TransitionRequest{TargetState: domain.StateReady})
	_ = engine.ApplyTransition(task, TransitionRequest{
		CommandType: "GRANT_LEASE",
		TargetState: domain.StateLeased,
		WorkerID:    "worker-2",
	})

	if task.LeaseEpoch != 2 {
		t.Fatalf("expected epoch 2 after reassignment, got %d", task.LeaseEpoch)
	}
	if task.AssignedWorkerID != "worker-2" {
		t.Fatalf("expected worker-2, got %s", task.AssignedWorkerID)
	}

	// 2. Worker-1 finally delivers its delayed completion with Epoch=1
	err = engine.ApplyTransition(task, TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    "worker-1",
		LeaseEpoch:  1, // Stale!
		Result:      []byte("late-output"),
	})

	if !errors.Is(err, domain.ErrStaleLeaseEpoch) {
		t.Fatalf("expected ErrStaleLeaseEpoch for late worker-1 completion, got %v", err)
	}

	// 3. Task state must remain LEASED to worker-2, unaffected by worker-1
	if task.State != domain.StateLeased {
		t.Errorf("task state was corrupted! expected LEASED, got %s", task.State)
	}
	if task.AssignedWorkerID != "worker-2" {
		t.Errorf("task worker was corrupted! expected worker-2, got %s", task.AssignedWorkerID)
	}
}

// TestAdversarial_DuplicateACKStorm tests 50 concurrent duplicate completion RPCs.
func TestAdversarial_DuplicateACKStorm(t *testing.T) {
	engine := NewTransitionEngine()
	now := time.Now()

	task := &domain.Task{
		ID:               "task-storm-01",
		State:            domain.StateRunning,
		AssignedWorkerID: "worker-1",
		LeaseEpoch:       5,
		CreatedAt:        now,
	}

	concurrency := 50
	var wg sync.WaitGroup
	var successCount atomic.Int32
	var errorCount atomic.Int32

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			err := engine.ApplyTransition(task, TransitionRequest{
				CommandType: "COMPLETE_TASK",
				TargetState: domain.StateSucceeded,
				WorkerID:    "worker-1",
				LeaseEpoch:  5,
				Result:      []byte("authoritative-result"),
				Now:         now,
			})
			if err == nil {
				successCount.Add(1)
			} else {
				errorCount.Add(1)
			}
		}(i)
	}

	wg.Wait()

	if successCount.Load() != int32(concurrency) {
		t.Errorf("expected all %d concurrent duplicate completions to succeed idempotently, succeeded: %d, errors: %d",
			concurrency, successCount.Load(), errorCount.Load())
	}
	if task.State != domain.StateSucceeded {
		t.Errorf("expected final state SUCCEEDED, got %s", task.State)
	}
	if string(task.Result) != "authoritative-result" {
		t.Errorf("expected authoritative-result, got %s", string(task.Result))
	}
}

// TestAdversarial_StaleWorkerImposter verifies that imposter workers or stale epochs cannot hijack a task.
func TestAdversarial_StaleWorkerImposter(t *testing.T) {
	engine := NewTransitionEngine()
	now := time.Now()

	task := &domain.Task{
		ID:               "task-imposter-01",
		State:            domain.StateRunning,
		AssignedWorkerID: "worker-authorized",
		LeaseEpoch:       4,
		CreatedAt:        now,
	}

	// Case A: Correct epoch, wrong worker
	err := engine.ApplyTransition(task, TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    "worker-imposter",
		LeaseEpoch:  4,
	})
	if !errors.Is(err, domain.ErrWorkerMismatch) {
		t.Errorf("expected ErrWorkerMismatch, got %v", err)
	}

	// Case B: Correct worker, stale epoch
	err = engine.ApplyTransition(task, TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    "worker-authorized",
		LeaseEpoch:  3,
	})
	if !errors.Is(err, domain.ErrStaleLeaseEpoch) {
		t.Errorf("expected ErrStaleLeaseEpoch, got %v", err)
	}

	// Task must remain RUNNING
	if task.State != domain.StateRunning {
		t.Errorf("expected state to remain RUNNING, got %s", task.State)
	}
}

// TestAdversarial_RetryExhaustion asserts that once attempt count reaches MaxRetries,
// recovery moves strictly to FAILED and terminates.
func TestAdversarial_RetryExhaustion(t *testing.T) {
	engine := NewTransitionEngine()
	now := time.Now()

	maxRetries := int32(2)
	task := &domain.Task{
		ID:           "task-retry-01",
		State:        domain.StateReady,
		MaxRetries:   maxRetries,
		AttemptCount: 0,
		CreatedAt:    now,
	}

	// Cycle 1
	_ = engine.ApplyTransition(task, TransitionRequest{TargetState: domain.StateLeased, WorkerID: "w1"})
	_ = engine.ApplyTransition(task, TransitionRequest{CommandType: "ACK_RUNNING", TargetState: domain.StateRunning, WorkerID: "w1", LeaseEpoch: 1})
	_ = engine.ApplyTransition(task, TransitionRequest{CommandType: "RECLAIM_TASK", TargetState: domain.StateLost})
	_ = engine.ApplyTransition(task, TransitionRequest{TargetState: domain.StateRecovering})
	task.AttemptCount++
	_ = engine.ApplyTransition(task, TransitionRequest{CommandType: "MARK_READY", TargetState: domain.StateReady})

	if task.AttemptCount != 1 {
		t.Fatalf("expected attempt 1, got %d", task.AttemptCount)
	}

	// Cycle 2
	_ = engine.ApplyTransition(task, TransitionRequest{TargetState: domain.StateLeased, WorkerID: "w2"})
	_ = engine.ApplyTransition(task, TransitionRequest{CommandType: "ACK_RUNNING", TargetState: domain.StateRunning, WorkerID: "w2", LeaseEpoch: 2})
	_ = engine.ApplyTransition(task, TransitionRequest{CommandType: "RECLAIM_TASK", TargetState: domain.StateLost})
	_ = engine.ApplyTransition(task, TransitionRequest{TargetState: domain.StateRecovering})
	task.AttemptCount++

	// Now AttemptCount (2) >= MaxRetries (2) -> must transition to FAILED
	err := engine.ApplyTransition(task, TransitionRequest{
		CommandType:  "FAIL_TASK",
		TargetState:  domain.StateFailed,
		ErrorMessage: "retries exhausted",
	})
	if err != nil {
		t.Fatalf("failed transitioning exhausted task to FAILED: %v", err)
	}

	if task.State != domain.StateFailed {
		t.Fatalf("expected terminal state FAILED, got %s", task.State)
	}

	// Cannot re-open a terminal task
	err = engine.ApplyTransition(task, TransitionRequest{
		CommandType: "MARK_READY",
		TargetState: domain.StateReady,
	})
	if !errors.Is(err, domain.ErrTerminalState) {
		t.Errorf("expected ErrTerminalState, got %v", err)
	}
}

// TestConcurrency_MassiveContention spawns 100 goroutines attempting conflicting
// state mutations simultaneously to prove strict synchronization and zero panics.
func TestConcurrency_MassiveContention(t *testing.T) {
	engine := NewTransitionEngine()
	now := time.Now()

	task := &domain.Task{
		ID:               "task-contention-01",
		State:            domain.StateRunning,
		AssignedWorkerID: "worker-primary",
		LeaseEpoch:       1,
		CreatedAt:        now,
	}

	var wg sync.WaitGroup
	workers := 100

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(workerIndex int) {
			defer wg.Done()
			switch workerIndex % 4 {
			case 0:
				// Worker completion attempt
				_ = engine.ApplyTransition(task, TransitionRequest{
					CommandType: "COMPLETE_TASK",
					TargetState: domain.StateSucceeded,
					WorkerID:    "worker-primary",
					LeaseEpoch:  1,
					Result:      []byte("winner-result"),
				})
			case 1:
				// Worker failure attempt
				_ = engine.ApplyTransition(task, TransitionRequest{
					CommandType:  "FAIL_TASK",
					TargetState:  domain.StateFailed,
					WorkerID:     "worker-primary",
					LeaseEpoch:   1,
					ErrorMessage: "node crash",
				})
			case 2:
				// Coordinator cancellation attempt
				_ = engine.ApplyTransition(task, TransitionRequest{
					CommandType: "CANCEL_TASK",
					TargetState: domain.StateCancelled,
				})
			case 3:
				// Coordinator lease expiration attempt
				_ = engine.ApplyTransition(task, TransitionRequest{
					CommandType: "RECLAIM_TASK",
					TargetState: domain.StateLost,
				})
			}
		}(i)
	}

	wg.Wait()

	// Invariant Check: The task MUST end in one of the valid outcomes:
	// SUCCEEDED, FAILED, CANCELLED, or LOST
	validFinalStates := map[domain.State]bool{
		domain.StateSucceeded: true,
		domain.StateFailed:    true,
		domain.StateCancelled: true,
		domain.StateLost:      true,
	}

	if !validFinalStates[task.State] {
		t.Errorf("task ended in invalid state under contention: %s", task.State)
	}

	// If it reached terminal state, verify immutability
	if task.State.IsTerminal() {
		err := engine.ApplyTransition(task, TransitionRequest{
			TargetState: domain.StateReady,
		})
		if !errors.Is(err, domain.ErrTerminalState) {
			t.Errorf("expected ErrTerminalState on terminal task, got %v", err)
		}
	}
}
