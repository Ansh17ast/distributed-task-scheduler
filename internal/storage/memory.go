package storage

import (
	"context"
	"sync"
	"time"

	"distributed-scheduler/internal/domain"
)

// JournalEvent represents an immutable historical record of a state transition.
type JournalEvent struct {
	EventID   int64     `json:"event_id"`
	Timestamp time.Time `json:"timestamp"`
	RaftIndex uint64    `json:"raft_index"`
	RaftTerm  uint64    `json:"raft_term"`
	EventType string    `json:"event_type"`
	EntityID  string    `json:"entity_id"`
	TenantID  string    `json:"tenant_id"`
	Payload   []byte    `json:"payload"`
}

// AuditStore defines the interface for durable audit and historical queries.
type AuditStore interface {
	AppendEvent(ctx context.Context, event *JournalEvent) error
	BatchAppendEvents(ctx context.Context, events []*JournalEvent) error
	GetEventsForTask(ctx context.Context, taskID string) ([]*JournalEvent, error)
	ArchiveTask(ctx context.Context, task *domain.Task) error
	GetArchivedTask(ctx context.Context, taskID string) (*domain.Task, error)
	Close() error
}

// MemoryAuditStore provides a thread-safe in-memory test double for unit and integration tests.
type MemoryAuditStore struct {
	mu     sync.RWMutex
	events []*JournalEvent
	tasks  map[string]*domain.Task
}

// NewMemoryAuditStore creates a new in-memory audit store.
func NewMemoryAuditStore() *MemoryAuditStore {
	return &MemoryAuditStore{
		events: make([]*JournalEvent, 0),
		tasks:  make(map[string]*domain.Task),
	}
}

func (m *MemoryAuditStore) AppendEvent(ctx context.Context, event *JournalEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, event)
	return nil
}

func (m *MemoryAuditStore) BatchAppendEvents(ctx context.Context, events []*JournalEvent) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.events = append(m.events, events...)
	return nil
}

func (m *MemoryAuditStore) GetEventsForTask(ctx context.Context, taskID string) ([]*JournalEvent, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var out []*JournalEvent
	for _, e := range m.events {
		if e.EntityID == taskID {
			out = append(out, e)
		}
	}
	return out, nil
}

func (m *MemoryAuditStore) ArchiveTask(ctx context.Context, task *domain.Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Deep copy to prevent mutation
	copied := *task
	m.tasks[task.ID] = &copied
	return nil
}

func (m *MemoryAuditStore) GetArchivedTask(ctx context.Context, taskID string) (*domain.Task, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	task, exists := m.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}
	copied := *task
	return &copied, nil
}

func (m *MemoryAuditStore) Close() error {
	return nil
}
