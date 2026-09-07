package benchmarks

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"testing"
	"time"

	"distributed-scheduler/internal/dag"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
)

// BenchmarkSchedulingDecisionLatency measures policy evaluation duration for 1,000 ready tasks.
func BenchmarkSchedulingDecisionLatency(b *testing.B) {
	ctx := context.Background()
	policy := &scheduler.PriorityPolicy{}

	// Setup 1,000 tasks with varying priorities
	var tasks []*domain.Task
	for i := 0; i < 1000; i++ {
		tasks = append(tasks, &domain.Task{
			ID:        fmt.Sprintf("bench-task-%d", i),
			TenantID:  fmt.Sprintf("tenant-%d", i%10),
			Priority:  int32(i % 100),
			CreatedAt: time.Now(),
		})
	}

	// Setup 20 workers with 4 slots each = 80 capacity slots
	var workers []*domain.Worker
	for i := 0; i < 20; i++ {
		workers = append(workers, &domain.Worker{
			ID:             fmt.Sprintf("bench-worker-%d", i),
			MaxSlots:       4,
			AvailableSlots: 4,
			Status:         domain.WorkerStatusHealthy,
		})
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		assignments, err := policy.Schedule(ctx, tasks, workers, nil, nil)
		if err != nil {
			b.Fatalf("scheduling failed: %v", err)
		}
		if len(assignments) != 80 {
			b.Fatalf("expected 80 assignments, got %d", len(assignments))
		}
	}
}

// BenchmarkTaskDispatchLatency measures end-to-end lease grant duration in the state store.
func BenchmarkTaskDispatchLatency(b *testing.B) {
	store := state.NewStore(storage.NewMemoryAuditStore())
	store.SetTenantQuota(&domain.TenantQuota{
		TenantID:       "tenant-bench",
		MaxConcurrency: 10000000,
		MaxQueueDepth:  10000000,
	})

	// Pre-register worker with b.N slots
	maxSlots := int32(b.N + 10)
	_ = store.RegisterWorker(&domain.Worker{
		ID:             "worker-bench",
		MaxSlots:       maxSlots,
		AvailableSlots: maxSlots,
		Status:         domain.WorkerStatusHealthy,
	})

	// Pre-create tasks in READY
	for i := 0; i < b.N; i++ {
		tID := fmt.Sprintf("task-dispatch-%d", i)
		_, _ = store.SubmitTask(&domain.Task{
			ID:        tID,
			TenantID:  "tenant-bench",
			Priority:  50,
			CreatedAt: time.Now(),
		})
		_, _ = store.MarkTaskReady(tID)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tID := fmt.Sprintf("task-dispatch-%d", i)
		_, err := store.GrantTaskLease(tID, "worker-bench", "bench-sess", 10*time.Second)
		if err != nil {
			b.Fatalf("grant lease failed at %d: %v", i, err)
		}
	}
}

// BenchmarkQueueThroughput measures rate of task submission and readiness promotion.
func BenchmarkQueueThroughput(b *testing.B) {
	store := state.NewStore(storage.NewMemoryAuditStore())
	store.SetTenantQuota(&domain.TenantQuota{
		TenantID:       "tenant-throughput",
		MaxConcurrency: 10000000,
		MaxQueueDepth:  10000000,
	})

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		tID := fmt.Sprintf("throughput-task-%d", i)
		_, err := store.SubmitTask(&domain.Task{
			ID:        tID,
			TenantID:  "tenant-throughput",
			Priority:  10,
			CreatedAt: time.Now(),
		})
		if err != nil {
			b.Fatalf("submit failed: %v", err)
		}
		_, err = store.MarkTaskReady(tID)
		if err != nil {
			b.Fatalf("mark ready failed: %v", err)
		}
	}
}

// BenchmarkDAGValidationKahnAlgorithm measures topological sort and cycle detection on a 100-node DAG.
func BenchmarkDAGValidationKahnAlgorithm(b *testing.B) {
	// Build 100-node multi-tier DAG
	const nodeCount = 100
	var template []*domain.Task
	for i := 0; i < 10; i++ {
		template = append(template, &domain.Task{ID: fmt.Sprintf("bench-node-0-%d", i)})
	}
	for stage := 1; stage < 10; stage++ {
		for i := 0; i < 10; i++ {
			nodeID := fmt.Sprintf("bench-node-%d-%d", stage, i)
			var deps []string
			for prev := 0; prev < 10; prev++ {
				deps = append(deps, fmt.Sprintf("bench-node-%d-%d", stage-1, prev))
			}
			template = append(template, &domain.Task{
				ID:           nodeID,
				Dependencies: deps,
			})
		}
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		// Deep clone task slice for isolation
		cloned := make([]*domain.Task, nodeCount)
		for idx, t := range template {
			cloned[idx] = &domain.Task{
				ID:           t.ID,
				Dependencies: t.Dependencies,
			}
		}
		if err := dag.ValidateDAG("bench-dag", "tenant-bench", cloned); err != nil {
			b.Fatalf("dag validation failed: %v", err)
		}
	}
}

// BenchmarkDAGFanOutDependencyPromotion measures time to evaluate 50 fan-out child dependencies.
func BenchmarkDAGFanOutDependencyPromotion(b *testing.B) {
	ctx := context.Background()
	store := state.NewStore(storage.NewMemoryAuditStore())
	store.SetTenantQuota(&domain.TenantQuota{TenantID: "tenant-bench", MaxConcurrency: 1000, MaxQueueDepth: 1000})
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine := dag.NewEngine(store, logger)

	const fanOutCount = 50
	var tasks []*domain.Task
	root := &domain.Task{ID: "root"}
	tasks = append(tasks, root)
	for c := 0; c < fanOutCount; c++ {
		childID := fmt.Sprintf("child-%d", c)
		tasks = append(tasks, &domain.Task{
			ID:           childID,
			Dependencies: []string{"root"},
		})
	}
	_ = dag.ValidateDAG("bench-fanout", "tenant-bench", tasks)
	_ = store.SubmitDAG(&domain.DAG{ID: "bench-fanout", TenantID: "tenant-bench"}, tasks)
	_ = store.RegisterWorker(&domain.Worker{ID: "w1", SessionID: "s1", MaxSlots: 100, AvailableSlots: 100, Status: domain.WorkerStatusHealthy})
	leased, _ := store.GrantTaskLease("root", "w1", "s1", 5*time.Second)
	_ = store.MarkTaskRunning("root", "w1", "s1", leased.LeaseEpoch)
	_ = store.CompleteTask("root", "w1", "s1", leased.LeaseEpoch, []byte("ok"))
	rootTask, _ := store.GetTask("root")

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, _ = engine.OnTaskCompleted(ctx, rootTask)
	}
}
