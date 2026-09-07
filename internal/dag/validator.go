package dag

import (
	"errors"
	"fmt"

	"distributed-scheduler/internal/domain"
)

// ValidateDAG validates the structural integrity and acyclic properties of a DAG submission.
// It detects cycles using Kahn's topological sort algorithm, validates dependency existence,
// disallows self-dependencies, ensures uniqueness of task IDs, and sets up reverse adjacency (Dependents).
func ValidateDAG(dagID string, tenantID string, tasks []*domain.Task) error {
	if dagID == "" {
		return errors.New("dag_id cannot be empty")
	}
	if tenantID == "" {
		return errors.New("tenant_id cannot be empty")
	}
	if len(tasks) == 0 {
		return domain.ErrEmptyDAG
	}

	taskMap := make(map[string]*domain.Task, len(tasks))
	inDegree := make(map[string]int, len(tasks))
	dependents := make(map[string][]string, len(tasks))

	// 1. Check ID uniqueness and non-empty IDs
	for _, t := range tasks {
		if t.ID == "" {
			return errors.New("task id cannot be empty within dag")
		}
		if _, exists := taskMap[t.ID]; exists {
			return fmt.Errorf("duplicate task id %q in dag %q", t.ID, dagID)
		}
		taskMap[t.ID] = t
		t.DAGID = dagID
		t.TenantID = tenantID
		inDegree[t.ID] = len(t.Dependencies)
	}

	// 2. Validate dependencies referential integrity and self-dependency
	for _, t := range tasks {
		seenDep := make(map[string]bool)
		for _, depID := range t.Dependencies {
			if depID == t.ID {
				return fmt.Errorf("%w: task %q depends on itself", domain.ErrSelfDependency, t.ID)
			}
			if _, exists := taskMap[depID]; !exists {
				return fmt.Errorf("%w: task %q depends on non-existent task %q", domain.ErrMissingDependency, t.ID, depID)
			}
			if seenDep[depID] {
				// Deduplicate dependency declarations
				inDegree[t.ID]--
				continue
			}
			seenDep[depID] = true
			dependents[depID] = append(dependents[depID], t.ID)
		}
	}

	// 3. Populate reverse adjacency list (Dependents) on each task
	for id, t := range taskMap {
		t.Dependents = dependents[id]
	}

	// 4. Kahn's Topological Sort Cycle Detection
	// Enqueue all tasks with in-degree 0 (roots)
	var queue []string
	for id, deg := range inDegree {
		if deg == 0 {
			queue = append(queue, id)
		}
	}

	visitedCount := 0
	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]
		visitedCount++

		for _, downstreamID := range dependents[currID] {
			inDegree[downstreamID]--
			if inDegree[downstreamID] == 0 {
				queue = append(queue, downstreamID)
			}
		}
	}

	if visitedCount != len(tasks) {
		return fmt.Errorf("%w: expected %d topologically sorted tasks, only visited %d",
			domain.ErrCycleDetected, len(tasks), visitedCount)
	}

	return nil
}
