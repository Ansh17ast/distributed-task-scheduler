package statemachine

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	"distributed-scheduler/internal/domain"
)

// TransitionEngine validates and executes state transitions for tasks with command-semantic idempotency.
type TransitionEngine struct {
	mu               sync.RWMutex
	validTransitions map[domain.State]map[domain.State]bool
}

// NewTransitionEngine creates and initializes the formal transition engine.
func NewTransitionEngine() *TransitionEngine {
	e := &TransitionEngine{
		validTransitions: make(map[domain.State]map[domain.State]bool),
	}
	e.initTransitions()
	return e
}

func (e *TransitionEngine) initTransitions() {
	// PENDING can transition to BLOCKED (has dependencies), READY (0 dependencies), or CANCELLED
	e.allow(domain.StatePending, domain.StateBlocked)
	e.allow(domain.StatePending, domain.StateReady)
	e.allow(domain.StatePending, domain.StateCancelled)

	// BLOCKED can transition to READY (all deps succeeded) or CANCELLED (upstream failed/cancelled)
	e.allow(domain.StateBlocked, domain.StateReady)
	e.allow(domain.StateBlocked, domain.StateCancelled)

	// READY can transition to LEASED (dispatched to worker) or CANCELLED
	e.allow(domain.StateReady, domain.StateLeased)
	e.allow(domain.StateReady, domain.StateCancelled)

	// LEASED can transition to RUNNING (worker ACK), LOST (lease timeout before ACK), or CANCELLED
	e.allow(domain.StateLeased, domain.StateRunning)
	e.allow(domain.StateLeased, domain.StateLost)
	e.allow(domain.StateLeased, domain.StateCancelled)

	// RUNNING can transition to SUCCEEDED, FAILED, RETRY_WAIT, LOST, or CANCELLED
	e.allow(domain.StateRunning, domain.StateSucceeded)
	e.allow(domain.StateRunning, domain.StateFailed)
	e.allow(domain.StateRunning, domain.StateRetryWait)
	e.allow(domain.StateRunning, domain.StateLost)
	e.allow(domain.StateRunning, domain.StateCancelled)

	// RETRY_WAIT can transition to READY (backoff elapsed) or CANCELLED
	e.allow(domain.StateRetryWait, domain.StateReady)
	e.allow(domain.StateRetryWait, domain.StateCancelled)

	// LOST can transition to RECOVERING (scanner picked it up) or CANCELLED
	e.allow(domain.StateLost, domain.StateRecovering)
	e.allow(domain.StateLost, domain.StateCancelled)

	// RECOVERING can transition to READY (retries remain), FAILED (retries exhausted), or CANCELLED
	e.allow(domain.StateRecovering, domain.StateReady)
	e.allow(domain.StateRecovering, domain.StateFailed)
	e.allow(domain.StateRecovering, domain.StateCancelled)
}

func (e *TransitionEngine) allow(from, to domain.State) {
	if _, exists := e.validTransitions[from]; !exists {
		e.validTransitions[from] = make(map[domain.State]bool)
	}
	e.validTransitions[from][to] = true
}

// CanTransition checks if moving from 'from' to 'to' is structurally valid (for different states).
func (e *TransitionEngine) CanTransition(from, to domain.State) bool {
	if from == to {
		return false // Same-state transition is NOT a valid state change path
	}
	if from.IsTerminal() {
		return false // Terminal states are strictly immutable
	}
	targets, exists := e.validTransitions[from]
	if !exists {
		return false
	}
	return targets[to]
}

// ValidateTransition verifies whether a state transition path is allowed.
func (e *TransitionEngine) ValidateTransition(from, to domain.State) error {
	if from == to {
		return fmt.Errorf("%w: same-state transition from %s to %s without command replay context", domain.ErrInvalidTransition, from, to)
	}
	if from.IsTerminal() {
		return fmt.Errorf("%w: cannot transition from terminal state %s to %s", domain.ErrTerminalState, from, to)
	}
	if !e.CanTransition(from, to) {
		return fmt.Errorf("%w: cannot transition from %s to %s", domain.ErrInvalidTransition, from, to)
	}
	return nil
}

// TransitionRequest encapsulates parameters for a state mutation or command replay.
type TransitionRequest struct {
	CommandType      string       // e.g. "COMPLETE_TASK", "FAIL_TASK", "ACK_RUNNING", "CANCEL_TASK", "GRANT_LEASE"
	TargetState      domain.State // The expected resulting state
	WorkerID         string       // Calling worker ID
	SessionID        string       // Calling worker process incarnation session UUID
	LeaseEpoch       uint64       // Worker lease epoch token
	Result           []byte       // Task output
	ErrorMessage     string       // Task error
	RaftAppliedIndex uint64       // Raft log index
	RaftAppliedTerm  uint64       // Raft log term
	Now              time.Time    // Wall-clock audit timestamp
}

// checkIdempotentReplay verifies whether an incoming command on a task that already occupies
// TargetState represents a legitimate, identical command replay versus a malformed mutation.
func (e *TransitionEngine) checkIdempotentReplay(task *domain.Task, req TransitionRequest) error {
	switch task.State {
	case domain.StateSucceeded:
		// Valid replay: same command, matching LeaseEpoch, matching assigned worker
		if req.CommandType == "COMPLETE_TASK" {
			if req.LeaseEpoch != task.LeaseEpoch {
				return fmt.Errorf("%w: duplicate completion carries epoch %d, task committed with epoch %d",
					domain.ErrStaleLeaseEpoch, req.LeaseEpoch, task.LeaseEpoch)
			}
			if req.SessionID != "" && task.AssignedSessionID != "" && req.SessionID != task.AssignedSessionID {
				return fmt.Errorf("%w: duplicate completion carries session %s, task committed with session %s",
					domain.ErrStaleWorkerSession, req.SessionID, task.AssignedSessionID)
			}
			if req.WorkerID != "" && task.AssignedWorkerID != "" && req.WorkerID != task.AssignedWorkerID {
				return fmt.Errorf("%w: duplicate completion worker %s does not match task worker %s",
					domain.ErrWorkerMismatch, req.WorkerID, task.AssignedWorkerID)
			}
			// Safe idempotent replay
			return nil
		}
		return fmt.Errorf("%w: task is in terminal state %s; cannot apply command %s",
			domain.ErrTerminalState, task.State, req.CommandType)

	case domain.StateFailed:
		if req.CommandType == "FAIL_TASK" {
			if req.LeaseEpoch != task.LeaseEpoch {
				return fmt.Errorf("%w: duplicate failure carries epoch %d, task committed with epoch %d",
					domain.ErrStaleLeaseEpoch, req.LeaseEpoch, task.LeaseEpoch)
			}
			return nil
		}
		return fmt.Errorf("%w: task is in terminal state %s; cannot apply command %s",
			domain.ErrTerminalState, task.State, req.CommandType)

	case domain.StateCancelled:
		if req.CommandType == "CANCEL_TASK" {
			return nil // Idempotent cancellation
		}
		return fmt.Errorf("%w: task is in terminal state %s; cannot apply command %s",
			domain.ErrTerminalState, task.State, req.CommandType)

	case domain.StateRunning:
		if req.CommandType == "ACK_RUNNING" {
			if req.SessionID != "" && task.AssignedSessionID != "" && req.SessionID != task.AssignedSessionID {
				return fmt.Errorf("%w: duplicate running ack carries session %s, task committed with session %s",
					domain.ErrStaleWorkerSession, req.SessionID, task.AssignedSessionID)
			}
			if req.LeaseEpoch == task.LeaseEpoch && (req.WorkerID == "" || req.WorkerID == task.AssignedWorkerID) {
				return nil // Idempotent start ack
			}
			return fmt.Errorf("%w: duplicate running ack has epoch %d, expected %d",
				domain.ErrStaleLeaseEpoch, req.LeaseEpoch, task.LeaseEpoch)
		}
		return fmt.Errorf("%w: cannot transition RUNNING to RUNNING without ACK_RUNNING command",
			domain.ErrMalformedTransition)

	case domain.StateReady:
		if req.CommandType == "MARK_READY" {
			return nil // Idempotent mark ready
		}
		return fmt.Errorf("%w: cannot transition READY to READY without MARK_READY command",
			domain.ErrMalformedTransition)

	default:
		return fmt.Errorf("%w: unsupported same-state replay for state %s with command %s",
			domain.ErrMalformedTransition, task.State, req.CommandType)
	}
}

// ApplyTransition executes a state mutation on a Task, enforcing command-semantic idempotency and fencing.
func (e *TransitionEngine) ApplyTransition(task *domain.Task, req TransitionRequest) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 1. Check same-state command replay vs malformed mutation
	if task.State == req.TargetState {
		return e.checkIdempotentReplay(task, req)
	}

	// 2. Strict terminal state immutability check
	if task.State.IsTerminal() {
		return fmt.Errorf("%w: task %s is in terminal state (%s) and cannot transition to %s",
			domain.ErrTerminalState, task.ID, task.State, req.TargetState)
	}

	// 3. Fencing token check for worker-driven transitions
	isWorkerDriven := (req.TargetState == domain.StateRunning ||
		req.TargetState == domain.StateSucceeded ||
		req.TargetState == domain.StateRetryWait ||
		(req.TargetState == domain.StateFailed && task.State == domain.StateRunning))

	if isWorkerDriven {
		if req.WorkerID != "" && req.WorkerID != task.AssignedWorkerID {
			if req.LeaseEpoch != task.LeaseEpoch {
				return fmt.Errorf("%w: incoming epoch %d does not match task epoch %d",
					domain.ErrStaleLeaseEpoch, req.LeaseEpoch, task.LeaseEpoch)
			}
			return fmt.Errorf("%w: worker %s does not hold lease (held by %s)",
				domain.ErrWorkerMismatch, req.WorkerID, task.AssignedWorkerID)
		}
		if req.SessionID != "" && task.AssignedSessionID != "" && req.SessionID != task.AssignedSessionID {
			return fmt.Errorf("%w: incoming session %s does not match task lease session %s",
				domain.ErrStaleWorkerSession, req.SessionID, task.AssignedSessionID)
		}
		if req.LeaseEpoch != task.LeaseEpoch {
			return fmt.Errorf("%w: incoming epoch %d does not match task epoch %d",
				domain.ErrStaleLeaseEpoch, req.LeaseEpoch, task.LeaseEpoch)
		}
	}

	// 4. Validate transition path
	if err := e.ValidateTransition(task.State, req.TargetState); err != nil {
		return err
	}

	// 5. Apply state mutations
	task.State = req.TargetState
	task.RaftAppliedIndex = req.RaftAppliedIndex
	task.RaftAppliedTerm = req.RaftAppliedTerm

	now := req.Now
	if now.IsZero() {
		now = time.Now()
	}

	switch req.TargetState {
	case domain.StateLeased:
		task.AssignedWorkerID = req.WorkerID
		task.AssignedSessionID = req.SessionID
		task.LeaseEpoch++
	case domain.StateRunning:
		if task.StartedAt == nil {
			task.StartedAt = &now
		}
	case domain.StateSucceeded:
		task.CompletedAt = &now
		task.Result = req.Result
		task.ErrorMessage = ""
	case domain.StateFailed:
		task.CompletedAt = &now
		task.ErrorMessage = req.ErrorMessage
	case domain.StateRetryWait:
		task.AttemptCount++
		task.ErrorMessage = req.ErrorMessage
		task.AssignedWorkerID = ""
		task.AssignedSessionID = ""
	case domain.StateReady:
		task.AssignedWorkerID = ""
		task.AssignedSessionID = ""
	case domain.StateCancelled:
		task.CompletedAt = &now
		if req.ErrorMessage != "" {
			task.ErrorMessage = req.ErrorMessage
		} else {
			task.ErrorMessage = "task cancelled"
		}
	case domain.StateLost:
		task.ErrorMessage = "worker lease expired"
	case domain.StateRecovering:
		// Transitioning through recovery
	}

	return nil
}

// IsResultEqual compares two byte slices.
func IsResultEqual(a, b []byte) bool {
	return bytes.Equal(a, b)
}
