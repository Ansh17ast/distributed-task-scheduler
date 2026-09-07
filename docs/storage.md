# Storage Architecture & Persistence Model

This document specifies the persistence architecture, the clean storage interface boundary, the PostgreSQL production schema, and the test double strategy for the distributed scheduler.

---

## 1. Storage Boundaries & Separation of Concerns

A common flaw in distributed systems is overloading a single database to handle consensus, hot scheduling queues, high-frequency heartbeats, and historical audits simultaneously. We enforce a strict separation of concerns across four data tiers:

```
+========================================================================================+
| Tier 1: Authoritative Active State                                                     |
| Storage: In-Memory Raft FSM + On-Disk Raft Log & Snapshots                             |
| Consistency: Linearizable (Strong Consistency via Raft Quorum)                        |
| Latency: < 2ms commit latency                                                          |
| Failure Behavior: 100% durable across node crashes; replayed from Raft WAL             |
+========================================================================================+
                                            |
                                            | Async Stream of Committed Events
                                            v
+========================================================================================+
| Tier 2: Durable Audit Journal & Historical Analytics                                   |
| Storage: PostgreSQL Relational Tables                                                  |
| Consistency: Read-committed / Append-only                                              |
| Latency: Batch committed (50ms - 100ms async buffer)                                   |
| Failure Behavior: Guaranteed audit trail; failure does not block scheduling loop       |
+========================================================================================+
                                            |
+========================================================================================+
| Tier 3: Worker Liveness & Lease Deadlines                                              |
| Storage: In-Memory Monotonic Clock Table (Leader only)                                 |
| Consistency: Local monotonic time                                                      |
| Latency: Sub-microsecond (RAM lookup)                                                  |
| Failure Behavior: Re-established via heartbeats and failover grace periods             |
+========================================================================================+
```

---

## 2. The Clean Storage Interface

All durable audit and archival operations are abstracted behind a minimal Go interface:

```go
package storage

import (
    "context"
    "time"
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

// TaskRecord represents an archived historical task state.
type TaskRecord struct {
    TaskID           string     `json:"task_id"`
    DAGID            string     `json:"dag_id,omitempty"`
    TenantID         string     `json:"tenant_id"`
    State            string     `json:"state"`
    Priority         int32      `json:"priority"`
    AttemptCount     int32      `json:"attempt_count"`
    AssignedWorkerID string     `json:"assigned_worker_id,omitempty"`
    CreatedAt        time.Time  `json:"created_at"`
    StartedAt        *time.Time `json:"started_at,omitempty"`
    CompletedAt      *time.Time `json:"completed_at,omitempty"`
    Result           []byte     `json:"result,omitempty"`
    ErrorMessage     string     `json:"error_message,omitempty"`
}

// AuditStore abstracts durable event journaling and task history queries.
type AuditStore interface {
    // AppendEvent appends an immutable event to the journal.
    AppendEvent(ctx context.Context, event *JournalEvent) error

    // BatchAppendEvents writes multiple events in a single transactional batch.
    BatchAppendEvents(ctx context.Context, events []*JournalEvent) error

    // GetEventsForTask returns the complete chronological audit trail for a task.
    GetEventsForTask(ctx context.Context, taskID string) ([]*JournalEvent, error)

    // ArchiveTask upserts a snapshot of a completed or failed task.
    ArchiveTask(ctx context.Context, task *TaskRecord) error

    // Close releases database connections.
    Close() error
}
```

---

## 3. Production PostgreSQL Schema

For production deployments, the system persists to PostgreSQL using the following optimized, indexed schema:

```sql
-- Migration: 001_create_scheduler_tables.sql

-- Enable UUID extension if required
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- 1. Tenant Configurations
CREATE TABLE IF NOT EXISTS tenants (
    tenant_id VARCHAR(64) PRIMARY KEY,
    max_concurrency INT NOT NULL DEFAULT 10,
    max_queue_depth INT NOT NULL DEFAULT 1000,
    priority_multiplier DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- 2. Audit / State Event Journal (Append-Only)
CREATE TABLE IF NOT EXISTS audit_events (
    event_id BIGSERIAL PRIMARY KEY,
    timestamp TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    raft_index BIGINT NOT NULL,
    raft_term BIGINT NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    entity_id VARCHAR(128) NOT NULL,
    tenant_id VARCHAR(64) NOT NULL,
    payload JSONB,
    CONSTRAINT fk_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

-- Optimized index for chronological task tracing
CREATE INDEX IF NOT EXISTS idx_audit_events_entity_id_time 
    ON audit_events (entity_id, timestamp ASC);

-- Optimized index for tenant audit queries
CREATE INDEX IF NOT EXISTS idx_audit_events_tenant_time 
    ON audit_events (tenant_id, timestamp DESC);

-- 3. Archived Tasks (Historical Analytics)
CREATE TABLE IF NOT EXISTS task_history (
    task_id VARCHAR(128) PRIMARY KEY,
    dag_id VARCHAR(128),
    tenant_id VARCHAR(64) NOT NULL,
    state VARCHAR(32) NOT NULL,
    priority INT NOT NULL,
    attempt_count INT NOT NULL,
    assigned_worker_id VARCHAR(128),
    created_at TIMESTAMPTZ NOT NULL,
    started_at TIMESTAMPTZ,
    completed_at TIMESTAMPTZ,
    result BYTEA,
    error_message TEXT,
    CONSTRAINT fk_task_tenant FOREIGN KEY (tenant_id) REFERENCES tenants (tenant_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_task_history_tenant_state 
    ON task_history (tenant_id, state);

CREATE INDEX IF NOT EXISTS idx_task_history_dag_id 
    ON task_history (dag_id) WHERE dag_id IS NOT NULL;
```

---

## 4. Test Double Strategy (In-Memory Audit Store)

Per project constraints, we do **not** build a redundant second database (e.g. SQLite/bbolt) in Phase 1. Instead, for automated unit and integration testing where PostgreSQL is not present, we implement a thread-safe in-memory test double conforming to `AuditStore`:

```go
package storage

import (
    "context"
    "sync"
)

type MemoryAuditStore struct {
    mu     sync.RWMutex
    events []*JournalEvent
    tasks  map[string]*TaskRecord
}

func NewMemoryAuditStore() *MemoryAuditStore {
    return &MemoryAuditStore{
        events: make([]*JournalEvent, 0),
        tasks:  make(map[string]*TaskRecord),
    }
}

func (m *MemoryAuditStore) AppendEvent(ctx context.Context, event *JournalEvent) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.events = append(m.events, event)
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
```

This guarantees fast, reliable, zero-flake test execution across all developer platforms.
