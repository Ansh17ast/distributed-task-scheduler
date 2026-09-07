package storage

import (
	"context"
	"testing"
	"time"

	"distributed-scheduler/internal/domain"
)

func TestMemoryAuditStore(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryAuditStore()

	// 1. Append single event
	event1 := &JournalEvent{
		EventID:   1,
		Timestamp: time.Now(),
		EventType: "TASK_SUBMITTED",
		EntityID:  "task-1",
		TenantID:  "tenant-a",
	}
	if err := store.AppendEvent(ctx, event1); err != nil {
		t.Fatalf("failed to append event: %v", err)
	}

	// 2. Batch append
	event2 := &JournalEvent{
		EventID:   2,
		Timestamp: time.Now(),
		EventType: "TASK_READY",
		EntityID:  "task-1",
		TenantID:  "tenant-a",
	}
	event3 := &JournalEvent{
		EventID:   3,
		Timestamp: time.Now(),
		EventType: "TASK_SUBMITTED",
		EntityID:  "task-2",
		TenantID:  "tenant-b",
	}
	if err := store.BatchAppendEvents(ctx, []*JournalEvent{event2, event3}); err != nil {
		t.Fatalf("failed to batch append: %v", err)
	}

	// 3. Query events for task-1
	events, err := store.GetEventsForTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("failed to get events for task-1: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("expected 2 events for task-1, got %d", len(events))
	}

	// 4. Archive task
	task := &domain.Task{
		ID:       "task-1",
		TenantID: "tenant-a",
		State:    domain.StateSucceeded,
	}
	if err := store.ArchiveTask(ctx, task); err != nil {
		t.Fatalf("failed to archive task: %v", err)
	}

	archived, err := store.GetArchivedTask(ctx, "task-1")
	if err != nil {
		t.Fatalf("failed to get archived task: %v", err)
	}
	if archived.State != domain.StateSucceeded {
		t.Errorf("expected state SUCCEEDED, got %s", archived.State)
	}
}
