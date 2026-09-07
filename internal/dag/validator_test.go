package dag

import (
	"errors"
	"fmt"
	"testing"

	"distributed-scheduler/internal/domain"
)

func TestValidateDAG_ValidLinearChain(t *testing.T) {
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}},
		{ID: "task-C", Dependencies: []string{"task-B"}},
	}

	err := ValidateDAG("dag-linear", "tenant-test", tasks)
	if err != nil {
		t.Fatalf("expected valid linear dag, got error: %v", err)
	}

	// Verify reverse adjacency (Dependents)
	if len(tasks[0].Dependents) != 1 || tasks[0].Dependents[0] != "task-B" {
		t.Errorf("expected A.Dependents = [task-B], got %v", tasks[0].Dependents)
	}
	if len(tasks[1].Dependents) != 1 || tasks[1].Dependents[0] != "task-C" {
		t.Errorf("expected B.Dependents = [task-C], got %v", tasks[1].Dependents)
	}
}

func TestValidateDAG_ValidDiamond(t *testing.T) {
	// A -> B, C -> D
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}},
		{ID: "task-C", Dependencies: []string{"task-A"}},
		{ID: "task-D", Dependencies: []string{"task-B", "task-C"}},
	}

	err := ValidateDAG("dag-diamond", "tenant-test", tasks)
	if err != nil {
		t.Fatalf("expected valid diamond dag, got error: %v", err)
	}

	if len(tasks[0].Dependents) != 2 {
		t.Errorf("expected A to have 2 dependents (B, C), got %v", tasks[0].Dependents)
	}
}

func TestValidateDAG_SelfDependency(t *testing.T) {
	tasks := []*domain.Task{
		{ID: "task-self", Dependencies: []string{"task-self"}},
	}

	err := ValidateDAG("dag-self", "tenant-test", tasks)
	if err == nil {
		t.Fatalf("expected error for self-dependency, got nil")
	}
	if !errors.Is(err, domain.ErrSelfDependency) {
		t.Errorf("expected ErrSelfDependency, got %v", err)
	}
}

func TestValidateDAG_MissingDependency(t *testing.T) {
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-ghost"}},
	}

	err := ValidateDAG("dag-missing", "tenant-test", tasks)
	if err == nil {
		t.Fatalf("expected error for missing dependency, got nil")
	}
	if !errors.Is(err, domain.ErrMissingDependency) {
		t.Errorf("expected ErrMissingDependency, got %v", err)
	}
}

func TestValidateDAG_DirectCycle(t *testing.T) {
	// A -> B -> A
	tasks := []*domain.Task{
		{ID: "task-A", Dependencies: []string{"task-B"}},
		{ID: "task-B", Dependencies: []string{"task-A"}},
	}

	err := ValidateDAG("dag-cycle-2", "tenant-test", tasks)
	if err == nil {
		t.Fatalf("expected cycle detection error, got nil")
	}
	if !errors.Is(err, domain.ErrCycleDetected) {
		t.Errorf("expected ErrCycleDetected, got %v", err)
	}
}

func TestValidateDAG_ThreeNodeCycle(t *testing.T) {
	// A -> B -> C -> A
	tasks := []*domain.Task{
		{ID: "task-A", Dependencies: []string{"task-C"}},
		{ID: "task-B", Dependencies: []string{"task-A"}},
		{ID: "task-C", Dependencies: []string{"task-B"}},
	}

	err := ValidateDAG("dag-cycle-3", "tenant-test", tasks)
	if err == nil {
		t.Fatalf("expected cycle detection error, got nil")
	}
	if !errors.Is(err, domain.ErrCycleDetected) {
		t.Errorf("expected ErrCycleDetected, got %v", err)
	}
}

func TestValidateDAG_DiamondWithCrossCycle(t *testing.T) {
	// A -> B -> D -> B
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A", "task-D"}},
		{ID: "task-C", Dependencies: []string{"task-A"}},
		{ID: "task-D", Dependencies: []string{"task-B", "task-C"}},
	}

	err := ValidateDAG("dag-cross-cycle", "tenant-test", tasks)
	if err == nil {
		t.Fatalf("expected cycle detection error, got nil")
	}
	if !errors.Is(err, domain.ErrCycleDetected) {
		t.Errorf("expected ErrCycleDetected, got %v", err)
	}
}

func TestValidateDAG_DisconnectedComponents(t *testing.T) {
	// Two disjoint subgraphs: (A -> B) and (C -> D)
	tasks := []*domain.Task{
		{ID: "task-A"},
		{ID: "task-B", Dependencies: []string{"task-A"}},
		{ID: "task-C"},
		{ID: "task-D", Dependencies: []string{"task-C"}},
	}

	err := ValidateDAG("dag-disjoint", "tenant-test", tasks)
	if err != nil {
		t.Fatalf("expected valid disjoint dag, got error: %v", err)
	}
}

func TestValidateDAG_EmptyDAG(t *testing.T) {
	err := ValidateDAG("dag-empty", "tenant-test", nil)
	if !errors.Is(err, domain.ErrEmptyDAG) {
		t.Errorf("expected ErrEmptyDAG, got %v", err)
	}
}

func TestValidateDAG_LargeGraph500Nodes(t *testing.T) {
	const nodeCount = 500
	var tasks []*domain.Task

	// Stage 0: 10 roots
	for i := 0; i < 10; i++ {
		tasks = append(tasks, &domain.Task{ID: fmt.Sprintf("node-0-%d", i)})
	}

	// 49 stages with 10 nodes each, fully connected to previous stage
	for stage := 1; stage < 50; stage++ {
		for i := 0; i < 10; i++ {
			nodeID := fmt.Sprintf("node-%d-%d", stage, i)
			var deps []string
			for prev := 0; prev < 10; prev++ {
				deps = append(deps, fmt.Sprintf("node-%d-%d", stage-1, prev))
			}
			tasks = append(tasks, &domain.Task{
				ID:           nodeID,
				Dependencies: deps,
			})
		}
	}

	if len(tasks) != nodeCount {
		t.Fatalf("expected 500 tasks, got %d", len(tasks))
	}

	err := ValidateDAG("dag-large-500", "tenant-test", tasks)
	if err != nil {
		t.Fatalf("large dag validation failed: %v", err)
	}
}
