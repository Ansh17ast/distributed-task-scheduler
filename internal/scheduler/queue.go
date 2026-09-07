package scheduler

import (
	"context"
	"sync"

	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
)

// ReadyQueue manages in-memory candidate tasks ready for scheduling.
type ReadyQueue struct {
	mu     sync.RWMutex
	policy Policy
	store  *state.Store
}

// NewReadyQueue creates a new ReadyQueue with the given policy and state store.
func NewReadyQueue(policy Policy, store *state.Store) *ReadyQueue {
	if policy == nil {
		policy = &PriorityPolicy{}
	}
	return &ReadyQueue{
		policy: policy,
		store:  store,
	}
}

// SetPolicy updates the active scheduling algorithm dynamically.
func (q *ReadyQueue) SetPolicy(policy Policy) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.policy = policy
}

// SelectNextTask evaluates ready tasks against worker capacity and tenant quotas,
// returning the best task assignment for a pulling worker.
func (q *ReadyQueue) SelectNextTask(ctx context.Context, worker *domain.Worker) (*domain.Task, string, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	readyTasks := q.store.GetReadyTasks()
	if len(readyTasks) == 0 {
		return nil, "", nil
	}

	// Read quotas and active tenant running counts
	tenantQuotas := make(map[string]*domain.TenantQuota)
	currentRunning := make(map[string]int32)

	for _, t := range readyTasks {
		if _, exists := tenantQuotas[t.TenantID]; !exists {
			tenantQuotas[t.TenantID] = q.store.GetTenantQuota(t.TenantID)
			currentRunning[t.TenantID] = q.store.GetTenantActiveRunning(t.TenantID)
		}
	}

	// Schedule against single worker
	workers := []*domain.Worker{worker}
	assignments, err := q.policy.Schedule(ctx, readyTasks, workers, tenantQuotas, currentRunning)
	if err != nil {
		return nil, "", err
	}

	if len(assignments) == 0 {
		return nil, "", nil
	}

	assignment := assignments[0]
	task, err := q.store.GetTask(assignment.TaskID)
	if err != nil {
		return nil, "", err
	}

	return task, assignment.Reason, nil
}

// Len returns the number of ready tasks currently waiting for scheduling.
func (q *ReadyQueue) Len() int {
	return len(q.store.GetReadyTasks())
}
