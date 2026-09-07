package domain

import (
	"context"
	"errors"
	"time"
)

// ============================================================================
// ARCHITECTURAL INVARIANT: REPLICATED LOGICAL STATE VS RUNTIME STATE
//
// 1. REPLICATED LOGICAL STATE (Task, DAG, TenantQuota):
//    - Strictly deterministic, portable data replicated across the Raft log
//      and stored in the replicated Finite State Machine (FSM).
//    - Contains ONLY logical values: IDs, states, LeaseEpoch, logical lease
//      durations, attempt counts, and deterministic payloads/results.
//    - Wall-clock timestamps (CreatedAt, StartedAt, CompletedAt) are stored
//      purely for historical auditing and client reporting.
//    - ABSOLUTELY ZERO process-local monotonic clock readings are ever
//      serialized, replicated through Raft, or shared across network boundaries.
//
// 2. PROCESS-LOCAL RUNTIME STATE (TaskRuntimeState, WorkerRuntimeState):
//    - Maintained exclusively in volatile process memory on the active leader
//      or worker daemon.
//    - Tracks CPU-specific monotonic deadlines (time.Now() monotonic component),
//      execution context cancellation funcs, in-flight channels, and TCP state.
//    - Discarded on process termination; never persisted to Raft or database.
// ============================================================================

// State represents the explicit lifecycle state of a task.
type State string

const (
	StatePending    State = "PENDING"
	StateBlocked    State = "BLOCKED"
	StateReady      State = "READY"
	StateLeased     State = "LEASED"
	StateRunning    State = "RUNNING"
	StateSucceeded  State = "SUCCEEDED"
	StateFailed     State = "FAILED"
	StateRetryWait  State = "RETRY_WAIT"
	StateCancelled  State = "CANCELLED"
	StateLost       State = "LOST"
	StateRecovering State = "RECOVERING"
)

// IsTerminal returns true if the state is terminal and cannot undergo further operational transitions.
func (s State) IsTerminal() bool {
	return s == StateSucceeded || s == StateFailed || s == StateCancelled
}

// Common domain errors.
var (
	ErrInvalidTransition   = errors.New("invalid state transition")
	ErrStaleLeaseEpoch     = errors.New("stale lease epoch: worker token mismatch")
	ErrStaleWorkerSession  = errors.New("stale worker session: worker process incarnation mismatch")
	ErrTerminalState       = errors.New("task is in terminal state and cannot be mutated")
	ErrWorkerMismatch      = errors.New("worker id mismatch: lease not held by this worker")
	ErrTenantQuotaExceeded = errors.New("tenant concurrency or queue quota exceeded")
	ErrTaskNotFound        = errors.New("task not found")
	ErrDAGNotFound         = errors.New("dag not found")
	ErrWorkerNotFound      = errors.New("worker not found")
	ErrDuplicateTask       = errors.New("duplicate task with same idempotency key")
	ErrMalformedTransition = errors.New("malformed transition: same-state transition without valid command replay semantics")
	ErrCycleDetected       = errors.New("cycle detected in dag dependencies")
	ErrMissingDependency   = errors.New("missing upstream dependency in dag")
	ErrSelfDependency      = errors.New("task cannot depend on itself")
	ErrEmptyDAG            = errors.New("dag must contain at least one task")
)

// Task represents the authoritative, replicated logical state of a discrete unit of work.
// All fields here are deterministic and safe for Raft consensus log replication.
type Task struct {
	ID                string        `json:"id"`
	DAGID             string        `json:"dag_id,omitempty"`
	TenantID          string        `json:"tenant_id"`
	State             State         `json:"state"`
	Priority          int32         `json:"priority"` // Higher number = higher priority
	PayloadType       string        `json:"payload_type"`
	Payload           []byte        `json:"payload"`
	IdempotencyKey    string        `json:"idempotency_key"`
	Dependencies      []string      `json:"dependencies"` // Upstream task IDs
	Dependents        []string      `json:"dependents"`   // Downstream task IDs
	AssignedWorkerID  string        `json:"assigned_worker_id,omitempty"`
	AssignedSessionID string        `json:"assigned_session_id,omitempty"` // Unique process incarnation holding lease
	LeaseEpoch        uint64        `json:"lease_epoch"`                   // Fencing token incremented on each lease grant
	LeaseDuration     time.Duration `json:"lease_duration_ms"`             // Logical lease duration
	AttemptCount      int32         `json:"attempt_count"`
	MaxRetries        int32         `json:"max_retries"`
	Timeout           time.Duration `json:"timeout"`
	Result            []byte        `json:"result,omitempty"`
	ErrorMessage      string        `json:"error_message,omitempty"`
	RaftAppliedIndex  uint64        `json:"raft_applied_index,omitempty"`
	RaftAppliedTerm   uint64        `json:"raft_applied_term,omitempty"`
	CreatedAt         time.Time     `json:"created_at"`             // Wall-clock audit timestamp
	StartedAt         *time.Time    `json:"started_at,omitempty"`   // Wall-clock audit timestamp
	CompletedAt       *time.Time    `json:"completed_at,omitempty"` // Wall-clock audit timestamp
}

// TaskRuntimeState holds leader-local or worker-local volatile runtime state.
// This struct is NEVER serialized, NEVER persisted to disk, and NEVER replicated via Raft.
type TaskRuntimeState struct {
	TaskID                 string
	LocalMonotonicDeadline time.Time          // Leader or worker CPU monotonic clock reading
	CancelFunc             context.CancelFunc // Worker execution cancellation func
}

// DAG represents the replicated logical definition of a workflow.
type DAG struct {
	ID        string    `json:"id"`
	TenantID  string    `json:"tenant_id"`
	TaskIDs   []string  `json:"task_ids"`
	CreatedAt time.Time `json:"created_at"`
}

// WorkerStatus represents the health status of a registered worker.
type WorkerStatus string

const (
	WorkerStatusHealthy WorkerStatus = "HEALTHY"
	WorkerStatusSuspect WorkerStatus = "SUSPECT"
	WorkerStatusDead    WorkerStatus = "DEAD"
)

// Worker represents the replicated logical registration record of an execution node.
type Worker struct {
	ID             string            `json:"id"`
	SessionID      string            `json:"session_id,omitempty"` // Unique process incarnation UUID
	Address        string            `json:"address"`
	MaxSlots       int32             `json:"max_slots"`
	AvailableSlots int32             `json:"available_slots"`
	Status         WorkerStatus      `json:"status"`
	Metadata       map[string]string `json:"metadata"`
	RegisteredAt   time.Time         `json:"registered_at"`
}

// WorkerRuntimeState holds volatile, process-local liveness state for a worker.
// Maintained in-memory exclusively on the current coordinator leader using its own CPU monotonic clock.
type WorkerRuntimeState struct {
	WorkerID                    string
	SessionID                   string    // Active process incarnation UUID
	LastLocalMonotonicHeartbeat time.Time // Process-local monotonic instant of last heartbeat
	ActiveLeaseCount            int32
}

// TenantQuota defines replicated resource bounds for a tenant.
type TenantQuota struct {
	TenantID       string `json:"tenant_id"`
	MaxConcurrency int32  `json:"max_concurrency"`
	MaxQueueDepth  int32  `json:"max_queue_depth"`
}
