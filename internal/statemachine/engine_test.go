package statemachine

import (
	"errors"
	"testing"
	"time"

	"distributed-scheduler/internal/domain"
)

func TestTransitionEngine_Matrix(t *testing.T) {
	engine := NewTransitionEngine()

	allStates := []domain.State{
		domain.StatePending,
		domain.StateBlocked,
		domain.StateReady,
		domain.StateLeased,
		domain.StateRunning,
		domain.StateSucceeded,
		domain.StateFailed,
		domain.StateRetryWait,
		domain.StateCancelled,
		domain.StateLost,
		domain.StateRecovering,
	}

	expectedValid := map[domain.State]map[domain.State]bool{
		domain.StatePending: {
			domain.StateBlocked:   true,
			domain.StateReady:     true,
			domain.StateCancelled: true,
		},
		domain.StateBlocked: {
			domain.StateReady:     true,
			domain.StateCancelled: true,
		},
		domain.StateReady: {
			domain.StateLeased:    true,
			domain.StateCancelled: true,
		},
		domain.StateLeased: {
			domain.StateRunning:   true,
			domain.StateLost:      true,
			domain.StateCancelled: true,
		},
		domain.StateRunning: {
			domain.StateSucceeded: true,
			domain.StateFailed:    true,
			domain.StateRetryWait: true,
			domain.StateLost:      true,
			domain.StateCancelled: true,
		},
		domain.StateRetryWait: {
			domain.StateReady:     true,
			domain.StateCancelled: true,
		},
		domain.StateLost: {
			domain.StateRecovering: true,
			domain.StateCancelled:  true,
		},
		domain.StateRecovering: {
			domain.StateReady:     true,
			domain.StateFailed:    true,
			domain.StateCancelled: true,
		},
		domain.StateSucceeded: {},
		domain.StateFailed:    {},
		domain.StateCancelled: {},
	}

	for _, from := range allStates {
		for _, to := range allStates {
			t.Run(string(from)+"_to_"+string(to), func(t *testing.T) {
				can := engine.CanTransition(from, to)
				err := engine.ValidateTransition(from, to)

				if from == to {
					// Same-state transition without command context is NOT a valid transition path
					if can {
						t.Errorf("expected same-state transition %s -> %s CanTransition to be false", from, to)
					}
					if !errors.Is(err, domain.ErrInvalidTransition) {
						t.Errorf("expected same-state transition %s -> %s to return ErrInvalidTransition, got %v", from, to, err)
					}
					return
				}

				shouldBeValid := expectedValid[from][to]
				if shouldBeValid {
					if !can {
						t.Errorf("expected transition %s -> %s to be allowed", from, to)
					}
					if err != nil {
						t.Errorf("expected transition %s -> %s to return nil, got %v", from, to, err)
					}
				} else {
					if can {
						t.Errorf("expected transition %s -> %s to be FORBIDDEN", from, to)
					}
					if err == nil {
						t.Errorf("expected transition %s -> %s to return error, got nil", from, to)
					}
				}
			})
		}
	}
}

func TestTransitionEngine_IdempotentReplays(t *testing.T) {
	engine := NewTransitionEngine()
	now := time.Now()

	// 1. Valid duplicate completion
	task := &domain.Task{
		ID:               "task-100",
		State:            domain.StateSucceeded,
		AssignedWorkerID: "worker-1",
		LeaseEpoch:       5,
		Result:           []byte("initial-result"),
		CompletedAt:      &now,
	}

	err := engine.ApplyTransition(task, TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    "worker-1",
		LeaseEpoch:  5,
		Result:      []byte("initial-result"),
	})
	if err != nil {
		t.Fatalf("expected duplicate completion with identical epoch to succeed idempotently, got %v", err)
	}

	// 2. Duplicate completion with stale epoch must be rejected
	err = engine.ApplyTransition(task, TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    "worker-1",
		LeaseEpoch:  4, // Stale!
	})
	if !errors.Is(err, domain.ErrStaleLeaseEpoch) {
		t.Errorf("expected ErrStaleLeaseEpoch for stale epoch replay, got %v", err)
	}

	// 3. Duplicate completion with worker mismatch must be rejected
	err = engine.ApplyTransition(task, TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    "worker-imposter",
		LeaseEpoch:  5,
	})
	if !errors.Is(err, domain.ErrWorkerMismatch) {
		t.Errorf("expected ErrWorkerMismatch for imposter worker replay, got %v", err)
	}

	// 4. Duplicate failure replay
	failedTask := &domain.Task{
		ID:               "task-101",
		State:            domain.StateFailed,
		AssignedWorkerID: "worker-1",
		LeaseEpoch:       3,
		ErrorMessage:     "out of memory",
		CompletedAt:      &now,
	}
	err = engine.ApplyTransition(failedTask, TransitionRequest{
		CommandType:  "FAIL_TASK",
		TargetState:  domain.StateFailed,
		WorkerID:     "worker-1",
		LeaseEpoch:   3,
		ErrorMessage: "out of memory",
	})
	if err != nil {
		t.Fatalf("expected duplicate failure replay to succeed idempotently, got %v", err)
	}

	// 5. Duplicate cancellation replay
	cancelledTask := &domain.Task{
		ID:    "task-102",
		State: domain.StateCancelled,
	}
	err = engine.ApplyTransition(cancelledTask, TransitionRequest{
		CommandType: "CANCEL_TASK",
		TargetState: domain.StateCancelled,
	})
	if err != nil {
		t.Fatalf("expected duplicate cancellation replay to succeed idempotently, got %v", err)
	}

	// 6. Malformed same-state mutation (RUNNING -> RUNNING without valid command)
	runningTask := &domain.Task{
		ID:               "task-103",
		State:            domain.StateRunning,
		AssignedWorkerID: "worker-1",
		LeaseEpoch:       1,
	}
	err = engine.ApplyTransition(runningTask, TransitionRequest{
		CommandType: "ARBITRARY_MUTATION",
		TargetState: domain.StateRunning,
		WorkerID:    "worker-1",
		LeaseEpoch:  1,
	})
	if !errors.Is(err, domain.ErrMalformedTransition) {
		t.Errorf("expected ErrMalformedTransition for arbitrary RUNNING->RUNNING mutation, got %v", err)
	}
}

func TestTransitionEngine_TerminalStateImmutability(t *testing.T) {
	engine := NewTransitionEngine()
	now := time.Now()

	succeededTask := &domain.Task{
		ID:          "task-200",
		State:       domain.StateSucceeded,
		CompletedAt: &now,
	}

	// Cannot transition SUCCEEDED to RUNNING
	err := engine.ApplyTransition(succeededTask, TransitionRequest{
		CommandType: "ACK_RUNNING",
		TargetState: domain.StateRunning,
	})
	if !errors.Is(err, domain.ErrTerminalState) {
		t.Errorf("expected ErrTerminalState when mutating SUCCEEDED task, got %v", err)
	}

	// Cannot transition FAILED to READY
	failedTask := &domain.Task{
		ID:          "task-201",
		State:       domain.StateFailed,
		CompletedAt: &now,
	}
	err = engine.ApplyTransition(failedTask, TransitionRequest{
		CommandType: "MARK_READY",
		TargetState: domain.StateReady,
	})
	if !errors.Is(err, domain.ErrTerminalState) {
		t.Errorf("expected ErrTerminalState when mutating FAILED task, got %v", err)
	}
}
