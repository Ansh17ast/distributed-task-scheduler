package scheduler

import (
	"context"
	"fmt"
	"sort"

	"distributed-scheduler/internal/domain"
)

// Assignment pairs a candidate task with a worker node.
type Assignment struct {
	TaskID   string `json:"task_id"`
	WorkerID string `json:"worker_id"`
	Reason   string `json:"reason"`
}

// Policy defines the pluggable scheduling algorithm interface.
type Policy interface {
	Name() string
	Schedule(
		ctx context.Context,
		readyTasks []*domain.Task,
		workers []*domain.Worker,
		tenantQuotas map[string]*domain.TenantQuota,
		currentRunning map[string]int32,
	) ([]Assignment, error)
}

// FIFOPolicy schedules tasks strictly in order of arrival (CreatedAt ascending).
type FIFOPolicy struct{}

func (p *FIFOPolicy) Name() string { return "FIFO" }

func (p *FIFOPolicy) Schedule(
	ctx context.Context,
	readyTasks []*domain.Task,
	workers []*domain.Worker,
	tenantQuotas map[string]*domain.TenantQuota,
	currentRunning map[string]int32,
) ([]Assignment, error) {
	if len(readyTasks) == 0 || len(workers) == 0 {
		return nil, nil
	}

	// Copy and sort tasks by CreatedAt ascending
	sorted := make([]*domain.Task, len(readyTasks))
	copy(sorted, readyTasks)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})

	return greedyAssign(sorted, workers, tenantQuotas, currentRunning, "FIFO arrival order")
}

// PriorityPolicy schedules tasks by Priority descending, then CreatedAt ascending.
type PriorityPolicy struct{}

func (p *PriorityPolicy) Name() string { return "Priority" }

func (p *PriorityPolicy) Schedule(
	ctx context.Context,
	readyTasks []*domain.Task,
	workers []*domain.Worker,
	tenantQuotas map[string]*domain.TenantQuota,
	currentRunning map[string]int32,
) ([]Assignment, error) {
	if len(readyTasks) == 0 || len(workers) == 0 {
		return nil, nil
	}

	sorted := make([]*domain.Task, len(readyTasks))
	copy(sorted, readyTasks)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Priority != sorted[j].Priority {
			return sorted[i].Priority > sorted[j].Priority // Higher priority first
		}
		return sorted[i].CreatedAt.Before(sorted[j].CreatedAt)
	})

	return greedyAssign(sorted, workers, tenantQuotas, currentRunning, "Priority order")
}

// FairSharePolicy implements Deficit Round Robin (DRR) across tenants to prevent starvation.
type FairSharePolicy struct {
	deficits map[string]int32
	quantum  int32
}

// NewFairSharePolicy creates a DRR Fair-Share scheduler.
func NewFairSharePolicy(quantum int32) *FairSharePolicy {
	if quantum <= 0 {
		quantum = 1
	}
	return &FairSharePolicy{
		deficits: make(map[string]int32),
		quantum:  quantum,
	}
}

func (p *FairSharePolicy) Name() string { return "FairShare-DRR" }

func (p *FairSharePolicy) Schedule(
	ctx context.Context,
	readyTasks []*domain.Task,
	workers []*domain.Worker,
	tenantQuotas map[string]*domain.TenantQuota,
	currentRunning map[string]int32,
) ([]Assignment, error) {
	if len(readyTasks) == 0 || len(workers) == 0 {
		return nil, nil
	}

	// 1. Group tasks by tenant
	tenantTasks := make(map[string][]*domain.Task)
	for _, t := range readyTasks {
		tenantTasks[t.TenantID] = append(tenantTasks[t.TenantID], t)
	}

	// Sort tasks within each tenant by priority
	for tenantID := range tenantTasks {
		tasks := tenantTasks[tenantID]
		sort.Slice(tasks, func(i, j int) bool {
			if tasks[i].Priority != tasks[j].Priority {
				return tasks[i].Priority > tasks[j].Priority
			}
			return tasks[i].CreatedAt.Before(tasks[j].CreatedAt)
		})
	}

	// 2. Compute total available worker capacity
	workerSlots := make(map[string]int32)
	totalCapacity := int32(0)
	for _, w := range workers {
		if w.Status == domain.WorkerStatusHealthy && w.AvailableSlots > 0 {
			workerSlots[w.ID] = w.AvailableSlots
			totalCapacity += w.AvailableSlots
		}
	}
	if totalCapacity == 0 {
		return nil, nil
	}

	runningCopy := make(map[string]int32)
	for k, v := range currentRunning {
		runningCopy[k] = v
	}

	var assignments []Assignment

	// 3. Multi-tenant DRR rounds
	tenants := make([]string, 0, len(tenantTasks))
	for tid := range tenantTasks {
		tenants = append(tenants, tid)
	}
	sort.Strings(tenants) // Deterministic iteration order

	activeTenants := len(tenants)
	for totalCapacity > 0 && activeTenants > 0 {
		madeProgress := false

		for _, tid := range tenants {
			tasks := tenantTasks[tid]
			if len(tasks) == 0 {
				continue
			}

			// Add quantum to deficit counter
			p.deficits[tid] += p.quantum

			// Check tenant concurrency quota
			quota, hasQuota := tenantQuotas[tid]
			if hasQuota && quota.MaxConcurrency > 0 && runningCopy[tid] >= quota.MaxConcurrency {
				// Tenant has reached concurrency cap; skip
				continue
			}

			for len(tasks) > 0 && p.deficits[tid] >= 1 && totalCapacity > 0 {
				if hasQuota && quota.MaxConcurrency > 0 && runningCopy[tid] >= quota.MaxConcurrency {
					break
				}

				// Find a worker with available capacity
				assignedWorker := ""
				for _, w := range workers {
					if workerSlots[w.ID] > 0 {
						assignedWorker = w.ID
						break
					}
				}
				if assignedWorker == "" {
					break
				}

				taskToAssign := tasks[0]
				tasks = tasks[1:]
				tenantTasks[tid] = tasks

				assignments = append(assignments, Assignment{
					TaskID:   taskToAssign.ID,
					WorkerID: assignedWorker,
					Reason:   fmt.Sprintf("Fair-Share DRR allocation for tenant %s", tid),
				})

				p.deficits[tid] -= 1
				workerSlots[assignedWorker]--
				totalCapacity--
				runningCopy[tid]++
				madeProgress = true
			}
		}

		if !madeProgress {
			break
		}
	}

	return assignments, nil
}

// greedyAssign assigns sorted tasks to available workers while honoring tenant limits.
func greedyAssign(
	tasks []*domain.Task,
	workers []*domain.Worker,
	tenantQuotas map[string]*domain.TenantQuota,
	currentRunning map[string]int32,
	reason string,
) ([]Assignment, error) {
	workerSlots := make(map[string]int32)
	for _, w := range workers {
		if w.Status == domain.WorkerStatusHealthy && w.AvailableSlots > 0 {
			workerSlots[w.ID] = w.AvailableSlots
		}
	}

	runningCopy := make(map[string]int32)
	for k, v := range currentRunning {
		runningCopy[k] = v
	}

	var assignments []Assignment

	for _, task := range tasks {
		// Check tenant concurrency cap
		quota, exists := tenantQuotas[task.TenantID]
		if exists && quota.MaxConcurrency > 0 {
			if runningCopy[task.TenantID] >= quota.MaxConcurrency {
				continue // Skip task; tenant concurrency limit reached
			}
		}

		// Find next worker with slot
		assignedWorker := ""
		for _, w := range workers {
			if workerSlots[w.ID] > 0 {
				assignedWorker = w.ID
				break
			}
		}
		if assignedWorker == "" {
			break // All worker slots exhausted
		}

		assignments = append(assignments, Assignment{
			TaskID:   task.ID,
			WorkerID: assignedWorker,
			Reason:   reason,
		})

		workerSlots[assignedWorker]--
		runningCopy[task.TenantID]++
	}

	return assignments, nil
}
