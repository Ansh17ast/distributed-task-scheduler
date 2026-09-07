package consensus

import (
	"encoding/json"
	"fmt"
	"time"

	"distributed-scheduler/internal/domain"
)

// OpType identifies the authoritative state mutation operation.
type OpType string

const (
	OpSubmitTask     OpType = "SUBMIT_TASK"
	OpSubmitDAG      OpType = "SUBMIT_DAG"
	OpMarkTaskReady  OpType = "MARK_TASK_READY"
	OpGrantTaskLease OpType = "GRANT_TASK_LEASE"
	OpAckTaskRunning OpType = "ACK_TASK_RUNNING"
	OpCompleteTask   OpType = "COMPLETE_TASK"
	OpFailTask       OpType = "FAIL_TASK"
	OpReclaimTask    OpType = "RECLAIM_TASK"
	OpCancelTask     OpType = "CANCEL_TASK"
	OpRegisterWorker OpType = "REGISTER_WORKER"
	OpExpireWorker   OpType = "EXPIRE_WORKER"
	OpSetTenantQuota OpType = "SET_TENANT_QUOTA"
)

// Command carries the authoritative mutation request replicated across Raft.
// DETERMINISM GUARANTEE:
// All fields required for state transition—including timestamps, retry delays, and lease epochs—
// are generated ONCE by the proposing leader and carried deterministically within the Command.
// FSM.Apply MUST NOT invoke wall-clock or random number generators.
type Command struct {
	Op            OpType              `json:"op"`
	Task          *domain.Task        `json:"task,omitempty"`
	Tasks         []*domain.Task      `json:"tasks,omitempty"` // For DAG task arrays
	DAG           *domain.DAG         `json:"dag,omitempty"`
	TaskID        string              `json:"task_id,omitempty"`
	WorkerID      string              `json:"worker_id,omitempty"`
	SessionID     string              `json:"session_id,omitempty"`
	LeaseEpoch    uint64              `json:"lease_epoch,omitempty"`
	LeaseDuration time.Duration       `json:"lease_duration,omitempty"`
	Result        []byte              `json:"result,omitempty"`
	ErrorMessage  string              `json:"error_message,omitempty"`
	Reason        string              `json:"reason,omitempty"`
	NextState     domain.State        `json:"next_state,omitempty"`
	RetryDelay    time.Duration       `json:"retry_delay,omitempty"`
	Worker        *domain.Worker      `json:"worker,omitempty"`
	TenantQuota   *domain.TenantQuota `json:"tenant_quota,omitempty"`
	Timestamp     time.Time           `json:"timestamp"` // Leader-generated deterministic audit timestamp
}

// Encode serializes the Command to bytes for Raft log replication.
func (c *Command) Encode() ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("cannot encode nil command")
	}
	return json.Marshal(c)
}

// DecodeCommand deserializes bytes from the Raft log into a Command.
func DecodeCommand(data []byte) (*Command, error) {
	var cmd Command
	if err := json.Unmarshal(data, &cmd); err != nil {
		return nil, fmt.Errorf("failed to decode raft command: %w", err)
	}
	return &cmd, nil
}
