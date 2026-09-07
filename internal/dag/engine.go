package dag

import (
	"context"
	"fmt"
	"log/slog"

	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
)

// Engine manages dependency tracking, fan-in/fan-out, and failure propagation for DAG workflows.
// ARCHITECTURAL RULE: The Engine is a stateless calculation and triggering engine.
// It is NOT a second source of truth. All authoritative state transitions are executed
// strictly through state.Store and statemachine.TransitionEngine.
type Engine struct {
	store  *state.Store
	logger *slog.Logger
}

// NewEngine initializes a DAG dependency Engine.
func NewEngine(store *state.Store, logger *slog.Logger) *Engine {
	if logger == nil {
		logger = slog.Default()
	}
	return &Engine{
		store:  store,
		logger: logger,
	}
}

// OnTaskCompleted evaluates downstream dependents when a task reaches SUCCEEDED.
// If all upstream dependencies for a downstream dependent have SUCCEEDED, it promotes
// the dependent task from BLOCKED to READY.
func (e *Engine) OnTaskCompleted(ctx context.Context, task *domain.Task) ([]string, error) {
	if task.DAGID == "" || len(task.Dependents) == 0 {
		return nil, nil
	}

	var promotedIDs []string

	for _, depID := range task.Dependents {
		depTask, err := e.store.GetTask(depID)
		if err != nil {
			e.logger.Error("failed to get downstream dependent task", "task_id", depID, "error", err)
			continue
		}

		// Only evaluate tasks currently waiting in BLOCKED
		if depTask.State != domain.StateBlocked {
			continue
		}

		// Check if all upstream dependencies of depTask are SUCCEEDED
		allSatisfied := true
		for _, upstreamID := range depTask.Dependencies {
			upstreamTask, err := e.store.GetTask(upstreamID)
			if err != nil || upstreamTask.State != domain.StateSucceeded {
				allSatisfied = false
				break
			}
		}

		if allSatisfied {
			_, err := e.store.MarkTaskReady(depID)
			if err != nil {
				e.logger.Error("failed promoting dependent task to READY", "task_id", depID, "error", err)
				continue
			}

			e.logger.Info("dag dependency satisfied; promoted task to READY",
				"task_id", depID,
				"dag_id", task.DAGID,
				"unblocked_by", task.ID,
			)
			promotedIDs = append(promotedIDs, depID)
		}
	}

	return promotedIDs, nil
}

// OnTaskFailed handles transitive failure propagation when a task reaches terminal FAILED.
// All reachable downstream tasks in PENDING or BLOCKED are cancelled.
func (e *Engine) OnTaskFailed(ctx context.Context, task *domain.Task) ([]string, error) {
	if task.DAGID == "" || len(task.Dependents) == 0 {
		return nil, nil
	}

	return e.propagateCancellation(task.Dependents, fmt.Sprintf("upstream dependency %s failed", task.ID))
}

// OnTaskCancelled handles transitive cancellation when a task is cancelled.
// All reachable downstream tasks in PENDING or BLOCKED are cancelled.
func (e *Engine) OnTaskCancelled(ctx context.Context, task *domain.Task) ([]string, error) {
	if task.DAGID == "" || len(task.Dependents) == 0 {
		return nil, nil
	}

	return e.propagateCancellation(task.Dependents, fmt.Sprintf("upstream dependency %s was cancelled", task.ID))
}

// propagateCancellation performs BFS traversal over downstream dependents, cancelling any in BLOCKED or PENDING.
func (e *Engine) propagateCancellation(roots []string, reason string) ([]string, error) {
	var cancelledIDs []string
	visited := make(map[string]bool)
	queue := append([]string{}, roots...)

	for len(queue) > 0 {
		currID := queue[0]
		queue = queue[1:]

		if visited[currID] {
			continue
		}
		visited[currID] = true

		currTask, err := e.store.GetTask(currID)
		if err != nil {
			continue
		}

		// Cancel task if waiting in BLOCKED or PENDING
		if currTask.State == domain.StateBlocked || currTask.State == domain.StatePending {
			err := e.store.CancelTask(currID, reason)
			if err == nil {
				e.logger.Warn("dag task transitively cancelled",
					"task_id", currID,
					"dag_id", currTask.DAGID,
					"reason", reason,
				)
				cancelledIDs = append(cancelledIDs, currID)
			}
		}

		// Continue cascading down the dependency tree
		for _, downstreamID := range currTask.Dependents {
			if !visited[downstreamID] {
				queue = append(queue, downstreamID)
			}
		}
	}

	return cancelledIDs, nil
}
