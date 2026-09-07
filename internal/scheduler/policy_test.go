package scheduler

import (
	"context"
	"fmt"
	"testing"
	"time"

	"distributed-scheduler/internal/domain"
)

func TestFIFOPolicy_Schedule(t *testing.T) {
	ctx := context.Background()
	policy := &FIFOPolicy{}

	t1 := &domain.Task{ID: "task-1", TenantID: "tA", CreatedAt: time.Now().Add(-10 * time.Minute)}
	t2 := &domain.Task{ID: "task-2", TenantID: "tA", CreatedAt: time.Now().Add(-5 * time.Minute)}
	t3 := &domain.Task{ID: "task-3", TenantID: "tA", CreatedAt: time.Now()}

	workers := []*domain.Worker{
		{ID: "worker-1", AvailableSlots: 2, MaxSlots: 2, Status: domain.WorkerStatusHealthy},
	}

	assignments, err := policy.Schedule(ctx, []*domain.Task{t3, t1, t2}, workers, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(assignments))
	}
	if assignments[0].TaskID != "task-1" {
		t.Errorf("expected first task to be task-1, got %s", assignments[0].TaskID)
	}
	if assignments[1].TaskID != "task-2" {
		t.Errorf("expected second task to be task-2, got %s", assignments[1].TaskID)
	}
}

func TestPriorityPolicy_Schedule(t *testing.T) {
	ctx := context.Background()
	policy := &PriorityPolicy{}
	now := time.Now()

	low := &domain.Task{ID: "task-low", Priority: 10, TenantID: "tA", CreatedAt: now.Add(-10 * time.Minute)}
	high := &domain.Task{ID: "task-high", Priority: 100, TenantID: "tA", CreatedAt: now}
	mid := &domain.Task{ID: "task-mid", Priority: 50, TenantID: "tA", CreatedAt: now.Add(-5 * time.Minute)}

	workers := []*domain.Worker{
		{ID: "worker-1", AvailableSlots: 2, MaxSlots: 2, Status: domain.WorkerStatusHealthy},
	}

	assignments, err := policy.Schedule(ctx, []*domain.Task{low, high, mid}, workers, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(assignments) != 2 {
		t.Fatalf("expected 2 assignments, got %d", len(assignments))
	}
	if assignments[0].TaskID != "task-high" {
		t.Errorf("expected first task to be task-high (priority 100), got %s", assignments[0].TaskID)
	}
	if assignments[1].TaskID != "task-mid" {
		t.Errorf("expected second task to be task-mid (priority 50), got %s", assignments[1].TaskID)
	}
}

func TestFairSharePolicy_NoisyTenantStarvationPrevention(t *testing.T) {
	ctx := context.Background()
	policy := NewFairSharePolicy(1) // 1 task quantum per round
	now := time.Now()

	// Tenant Noisy submits 5 tasks
	var noisyTasks []*domain.Task
	for i := 1; i <= 5; i++ {
		noisyTasks = append(noisyTasks, &domain.Task{
			ID:        fmt.Sprintf("noisy-%d", i),
			TenantID:  "tenant-noisy",
			Priority:  50,
			CreatedAt: now,
		})
	}

	// Tenant Quiet submits 2 tasks
	quiet1 := &domain.Task{ID: "quiet-1", TenantID: "tenant-quiet", Priority: 50, CreatedAt: now}
	quiet2 := &domain.Task{ID: "quiet-2", TenantID: "tenant-quiet", Priority: 50, CreatedAt: now}

	allTasks := append(noisyTasks, quiet1, quiet2)

	// Available worker capacity = 4 slots
	workers := []*domain.Worker{
		{ID: "worker-1", AvailableSlots: 2, MaxSlots: 2, Status: domain.WorkerStatusHealthy},
		{ID: "worker-2", AvailableSlots: 2, MaxSlots: 2, Status: domain.WorkerStatusHealthy},
	}

	assignments, err := policy.Schedule(ctx, allTasks, workers, nil, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(assignments) != 4 {
		t.Fatalf("expected 4 assignments, got %d", len(assignments))
	}

	// Count assignments per tenant
	noisyCount := 0
	quietCount := 0
	for _, a := range assignments {
		if a.TaskID[:5] == "noisy" {
			noisyCount++
		} else {
			quietCount++
		}
	}

	// Under Fair-Share DRR, both tenants must receive fair allocation (2 each of the 4 slots)
	if noisyCount != 2 || quietCount != 2 {
		t.Errorf("fair share failed! expected 2 noisy and 2 quiet tasks, got %d noisy and %d quiet", noisyCount, quietCount)
	}
}
