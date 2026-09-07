package chaos

import (
	"fmt"
	"sync"
	"time"
)

// EventType categorizes structured events in the chaos experiment timeline.
type EventType string

const (
	EventExperimentStarted EventType = "EXPERIMENT_STARTED"
	EventFaultInjected     EventType = "FAULT_INJECTED"
	EventLeaderChanged     EventType = "LEADER_CHANGED"
	EventPartitionCreated  EventType = "PARTITION_CREATED"
	EventPartitionHealed   EventType = "PARTITION_HEALED"
	EventWorkerKilled      EventType = "WORKER_KILLED"
	EventWorkerIsolated    EventType = "WORKER_ISOLATED"
	EventWorkerReconnected EventType = "WORKER_RECONNECTED"
	EventTaskLeased        EventType = "TASK_LEASED"
	EventTaskReclaimed     EventType = "TASK_RECLAIMED"
	EventNodeKilled        EventType = "NODE_KILLED"
	EventNodeRestarted     EventType = "NODE_RESTARTED"
	EventSnapshotCreated   EventType = "SNAPSHOT_CREATED"
	EventSnapshotRestored  EventType = "SNAPSHOT_RESTORED"
	EventStaleCommand      EventType = "STALE_COMMAND_REJECTED"
	EventInvariantChecked  EventType = "INVARIANT_CHECKED"
	EventExperimentPassed  EventType = "EXPERIMENT_PASSED"
	EventExperimentFailed  EventType = "EXPERIMENT_FAILED"
)

// JournalEvent represents an immutable structured log record.
type JournalEvent struct {
	Index        int       `json:"index"`
	Timestamp    time.Time `json:"timestamp"`
	ExperimentID string    `json:"experiment_id"`
	Type         EventType `json:"type"`
	NodeID       string    `json:"node_id,omitempty"`
	WorkerID     string    `json:"worker_id,omitempty"`
	TaskID       string    `json:"task_id,omitempty"`
	Term         uint64    `json:"term,omitempty"`
	RaftIndex    uint64    `json:"raft_index,omitempty"`
	Details      string    `json:"details"`
}

// Journal provides thread-safe append-only logging of chaos events.
type Journal struct {
	experimentID string
	seed         int64
	mu           sync.RWMutex
	events       []JournalEvent
}

// NewJournal creates a new chaos event journal.
func NewJournal(experimentID string, seed int64) *Journal {
	return &Journal{
		experimentID: experimentID,
		seed:         seed,
		events:       make([]JournalEvent, 0, 128),
	}
}

// Record appends a new event to the journal.
func (j *Journal) Record(eventType EventType, nodeID, workerID, taskID string, term, raftIndex uint64, format string, args ...interface{}) JournalEvent {
	j.mu.Lock()
	defer j.mu.Unlock()

	evt := JournalEvent{
		Index:        len(j.events) + 1,
		Timestamp:    time.Now().UTC(),
		ExperimentID: j.experimentID,
		Type:         eventType,
		NodeID:       nodeID,
		WorkerID:     workerID,
		TaskID:       taskID,
		Term:         term,
		RaftIndex:    raftIndex,
		Details:      fmt.Sprintf(format, args...),
	}
	j.events = append(j.events, evt)
	return evt
}

// Events returns a copy of all recorded events.
func (j *Journal) Events() []JournalEvent {
	j.mu.RLock()
	defer j.mu.RUnlock()
	res := make([]JournalEvent, len(j.events))
	copy(res, j.events)
	return res
}

// PrintSummary prints a formatted execution log of the journal.
func (j *Journal) PrintSummary() {
	j.mu.RLock()
	defer j.mu.RUnlock()

	fmt.Printf("=== CHAOS JOURNAL: %s (Seed: %d) | %d Events ===\n", j.experimentID, j.seed, len(j.events))
	for _, e := range j.events {
		prefix := fmt.Sprintf("[%s] %-22s", e.Timestamp.Format("15:04:05.000"), e.Type)
		var meta string
		if e.NodeID != "" {
			meta += fmt.Sprintf(" node=%s", e.NodeID)
		}
		if e.Term > 0 {
			meta += fmt.Sprintf(" term=%d", e.Term)
		}
		if e.TaskID != "" {
			meta += fmt.Sprintf(" task=%s", e.TaskID)
		}
		fmt.Printf("%s%s : %s\n", prefix, meta, e.Details)
	}
}
