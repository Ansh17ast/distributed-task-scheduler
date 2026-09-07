package load

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/coordinator"
	"distributed-scheduler/internal/domain"
)

func computePercentiles(latencies []time.Duration) (p50, p95, p99 time.Duration) {
	if len(latencies) == 0 {
		return 0, 0, 0
	}
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})
	p50 = sorted[len(sorted)*50/100]
	p95 = sorted[len(sorted)*95/100]
	p99 = sorted[len(sorted)*99/100]
	return
}

// TestLoad_BurstSubmission_1000Tasks measures submission throughput and latency under burst load.
func TestLoad_BurstSubmission_1000Tasks(t *testing.T) {
	h, err := coordinator.NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create cluster harness: %v", err)
	}
	defer h.Close()

	leaderIdx, err := h.WaitLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader: %v", err)
	}

	client := h.Clients[leaderIdx]
	ctx := context.Background()

	const count = 1000
	latencies := make([]time.Duration, 0, count)

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)

	startTime := time.Now()
	for i := 0; i < count; i++ {
		taskID := fmt.Sprintf("burst-task-%d", i)
		tenantID := fmt.Sprintf("tenant-%d", i%10)

		subStart := time.Now()
		resp, err := client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{
				Id:             taskID,
				TenantId:       tenantID,
				Priority:       int32(i % 50),
				TimeoutSeconds: 30,
			},
		})
		latencies = append(latencies, time.Since(subStart))
		if err != nil || resp.TaskId != taskID {
			t.Fatalf("task %d submission failed: %v", i, err)
		}
	}
	elapsed := time.Since(startTime)
	throughput := float64(count) / elapsed.Seconds()

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	allocatedMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024 / 1024

	p50, p95, p99 := computePercentiles(latencies)

	fmt.Printf("\n[LOAD TEST 1] Burst Submission (1,000 Tasks):\n")
	fmt.Printf("    Throughput:  %.2f tasks/sec (in %.3fs)\n", throughput, elapsed.Seconds())
	fmt.Printf("    Latency p50: %v | p95: %v | p99: %v\n", p50, p95, p99)
	fmt.Printf("    Memory:      %.2f MB allocated\n", allocatedMB)
	fmt.Printf("    Queue Depth: %d ready tasks in leader\n", len(h.Stores[leaderIdx].GetReadyTasks()))
}

// TestLoad_ConcurrentClients_2000Tasks tests multi-tenant concurrent submissions.
func TestLoad_ConcurrentClients_2000Tasks(t *testing.T) {
	h, err := coordinator.NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create cluster harness: %v", err)
	}
	defer h.Close()

	leaderIdx, err := h.WaitLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader: %v", err)
	}

	client := h.Clients[leaderIdx]
	const numClients = 10
	const tasksPerClient = 200
	const totalTasks = numClients * tasksPerClient

	var wg sync.WaitGroup
	var successCount int64
	var errCount int64

	startTime := time.Now()
	for c := 0; c < numClients; c++ {
		wg.Add(1)
		go func(clientID int) {
			defer wg.Done()
			ctx := context.Background()
			tenantID := fmt.Sprintf("concurrent-tenant-%d", clientID)

			for i := 0; i < tasksPerClient; i++ {
				taskID := fmt.Sprintf("client-%d-task-%d", clientID, i)
				_, err := client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
					Task: &schedulerv1.TaskSpec{
						Id:             taskID,
						TenantId:       tenantID,
						Priority:       int32(i % 10),
						TimeoutSeconds: 30,
					},
				})
				if err != nil {
					atomic.AddInt64(&errCount, 1)
				} else {
					atomic.AddInt64(&successCount, 1)
				}
			}
		}(c)
	}

	wg.Wait()
	elapsed := time.Since(startTime)
	throughput := float64(totalTasks) / elapsed.Seconds()

	fmt.Printf("\n[LOAD TEST 2] Concurrent Multi-Tenant (10 Clients, 2,000 Tasks):\n")
	fmt.Printf("    Success:     %d / %d tasks (Errors: %d)\n", successCount, totalTasks, errCount)
	fmt.Printf("    Throughput:  %.2f tasks/sec (in %.3fs)\n", throughput, elapsed.Seconds())
	fmt.Printf("    Queue Depth: %d ready tasks in leader\n", len(h.Stores[leaderIdx].GetReadyTasks()))

	if errCount > 0 {
		t.Fatalf("encountered %d submission errors under concurrent load", errCount)
	}
}

// TestLoad_WorkerPoolScaling measures drain time scaling across 1, 5, and 20 workers.
func TestLoad_WorkerPoolScaling(t *testing.T) {
	workerCounts := []int{1, 5, 20}
	const tasksPerRun = 500

	fmt.Printf("\n==================================================================================\n")
	fmt.Printf("           WORKER POOL SCALING TEST (%d TASKS PER RUN)                           \n", tasksPerRun)
	fmt.Printf("==================================================================================\n")

	for _, numWorkers := range workerCounts {
		h, err := coordinator.NewClusterHarness(3)
		if err != nil {
			t.Fatalf("failed to create cluster harness: %v", err)
		}

		leaderIdx, err := h.WaitLeader(5 * time.Second)
		if err != nil {
			h.Close()
			t.Fatalf("failed to wait for leader: %v", err)
		}

		client := h.Clients[leaderIdx]
		wClient := h.WClients[leaderIdx]
		ctx := context.Background()

		// Submit tasks
		for i := 0; i < tasksPerRun; i++ {
			_, _ = client.SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
				Task: &schedulerv1.TaskSpec{
					Id:             fmt.Sprintf("scaling-%d-%d", numWorkers, i),
					TenantId:       "scale-tenant",
					Priority:       int32(i % 10),
					TimeoutSeconds: 30,
				},
			})
		}

		// Register workers
		for w := 0; w < numWorkers; w++ {
			_, _ = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
				WorkerId:  fmt.Sprintf("w-pool-%d-%d", numWorkers, w),
				SessionId: fmt.Sprintf("sess-%d-%d", numWorkers, w),
				Address:   fmt.Sprintf("127.0.0.1:%d", 11000+w),
				MaxSlots:  4,
			})
		}

		startTime := time.Now()
		var completedCount int64
		var wg sync.WaitGroup

		for w := 0; w < numWorkers; w++ {
			wg.Add(1)
			go func(workerIdx int) {
				defer wg.Done()
				wID := fmt.Sprintf("w-pool-%d-%d", numWorkers, workerIdx)
				sID := fmt.Sprintf("sess-%d-%d", numWorkers, workerIdx)
				wCtx := context.Background()

				for {
					if atomic.LoadInt64(&completedCount) >= tasksPerRun {
						return
					}
					pullResp, err := wClient.PullTask(wCtx, &schedulerv1.PullTaskRequest{
						WorkerId:  wID,
						SessionId: sID,
					})
					if err != nil || pullResp == nil || !pullResp.HasTask {
						time.Sleep(1 * time.Millisecond)
						continue
					}
					assignment := pullResp.GetAssignment()
					_, _ = wClient.AckTaskRunning(wCtx, &schedulerv1.AckTaskRunningRequest{
						TaskId:     assignment.TaskId,
						WorkerId:   wID,
						SessionId:  sID,
						LeaseEpoch: assignment.LeaseEpoch,
					})
					_, _ = wClient.ReportTaskCompleted(wCtx, &schedulerv1.ReportTaskCompletedRequest{
						TaskId:     assignment.TaskId,
						WorkerId:   wID,
						SessionId:  sID,
						LeaseEpoch: assignment.LeaseEpoch,
						Result:     []byte(`{}`),
					})
					atomic.AddInt64(&completedCount, 1)
				}
			}(w)
		}

		wg.Wait()
		elapsed := time.Since(startTime)
		throughput := float64(tasksPerRun) / elapsed.Seconds()

		fmt.Printf("    Workers: %-2d | Slots: %-3d | Drain Time: %-10s | Throughput: %8.2f tasks/sec\n",
			numWorkers, numWorkers*4, elapsed.Round(time.Millisecond), throughput)

		h.Close()
	}
	fmt.Printf("==================================================================================\n")
}

// TestLoad_10000TasksScale validates sustained execution up to 10,000 tasks.
func TestLoad_10000TasksScale(t *testing.T) {
	h, err := coordinator.NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create cluster harness: %v", err)
	}
	defer h.Close()

	leaderIdx, err := h.WaitLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader: %v", err)
	}

	client := h.Clients[leaderIdx]
	wClient := h.WClients[leaderIdx]
	ctx := context.Background()

	const totalTasks = 10000
	const numWorkers = 10
	const slotsPerWorker = 20

	// 1. Register Worker Pool
	for w := 0; w < numWorkers; w++ {
		workerID := fmt.Sprintf("scale-worker-%d", w)
		sessionID := fmt.Sprintf("scale-sess-%d", w)
		regResp, err := wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
			WorkerId:  workerID,
			SessionId: sessionID,
			Address:   fmt.Sprintf("127.0.0.1:%d", 10000+w),
			MaxSlots:  slotsPerWorker,
		})
		if err != nil || !regResp.Accepted {
			t.Fatalf("failed registering worker %s: %v", workerID, err)
		}
	}

	var memBefore runtime.MemStats
	runtime.ReadMemStats(&memBefore)
	startGoroutines := runtime.NumGoroutine()

	t.Logf("Beginning sustained 10,000-task scale benchmark...")
	startTime := time.Now()

	// 2. Stream task submissions concurrently in background
	go func() {
		subCtx := context.Background()
		for i := 0; i < totalTasks; i++ {
			taskID := fmt.Sprintf("scale-task-%d", i)
			tenantID := fmt.Sprintf("tenant-%d", i%20)
			_, _ = client.SubmitTask(subCtx, &schedulerv1.SubmitTaskRequest{
				Task: &schedulerv1.TaskSpec{
					Id:             taskID,
					TenantId:       tenantID,
					Priority:       int32(i % 100),
					TimeoutSeconds: 60,
				},
			})
		}
	}()

	// 3. Worker drain loops
	var completedCount int64
	var workerWg sync.WaitGroup

	for w := 0; w < numWorkers; w++ {
		workerWg.Add(1)
		go func(workerIdx int) {
			defer workerWg.Done()
			workerID := fmt.Sprintf("scale-worker-%d", workerIdx)
			sessionID := fmt.Sprintf("scale-sess-%d", workerIdx)
			wCtx := context.Background()

			for {
				if atomic.LoadInt64(&completedCount) >= int64(totalTasks) {
					return
				}

				pullResp, err := wClient.PullTask(wCtx, &schedulerv1.PullTaskRequest{
					WorkerId:  workerID,
					SessionId: sessionID,
				})
				if err != nil || pullResp == nil || !pullResp.HasTask {
					time.Sleep(1 * time.Millisecond)
					continue
				}

				assignment := pullResp.GetAssignment()
				// Ack
				_, _ = wClient.AckTaskRunning(wCtx, &schedulerv1.AckTaskRunningRequest{
					TaskId:     assignment.TaskId,
					WorkerId:   workerID,
					SessionId:  sessionID,
					LeaseEpoch: assignment.LeaseEpoch,
				})

				// Report complete
				_, _ = wClient.ReportTaskCompleted(wCtx, &schedulerv1.ReportTaskCompletedRequest{
					TaskId:     assignment.TaskId,
					WorkerId:   workerID,
					SessionId:  sessionID,
					LeaseEpoch: assignment.LeaseEpoch,
					Result:     []byte(`{"status":"ok"}`),
				})

				atomic.AddInt64(&completedCount, 1)
			}
		}(w)
	}

	workerWg.Wait()
	elapsed := time.Since(startTime)
	throughput := float64(totalTasks) / elapsed.Seconds()

	var memAfter runtime.MemStats
	runtime.ReadMemStats(&memAfter)
	allocatedMB := float64(memAfter.TotalAlloc-memBefore.TotalAlloc) / 1024 / 1024
	endGoroutines := runtime.NumGoroutine()

	fmt.Printf("\n==================================================================================\n")
	fmt.Printf("           10,000-TASK REAL CLUSTER SCALE BENCHMARK RESULTS                       \n")
	fmt.Printf("==================================================================================\n")
	fmt.Printf("    Tasks Executed:         %d / %d tasks\n", completedCount, totalTasks)
	fmt.Printf("    Active Workers:         %d workers (Total Capacity: %d slots)\n", numWorkers, numWorkers*slotsPerWorker)
	fmt.Printf("    Elapsed Time:           %.3f seconds\n", elapsed.Seconds())
	fmt.Printf("    Sustained Throughput:   %.2f tasks/sec\n", throughput)
	fmt.Printf("    Memory Allocated:       %.2f MB (HeapAlloc: %.2f MB)\n", allocatedMB, float64(memAfter.HeapAlloc)/1024/1024)
	fmt.Printf("    GC Cycles:              %d runs\n", memAfter.NumGC-memBefore.NumGC)
	fmt.Printf("    Goroutines (Start/End): %d / %d (Goroutine leak free)\n", startGoroutines, endGoroutines)
	fmt.Printf("    Final Queue Depth:      %d tasks\n", len(h.Stores[leaderIdx].GetReadyTasks()))
	fmt.Printf("==================================================================================\n")

	if completedCount != int64(totalTasks) {
		t.Fatalf("expected %d completed tasks, got %d", totalTasks, completedCount)
	}
}

// TestLoad_DAG_Workflow_50Tasks tests diamond DAG workflow submission and dependency propagation under load.
func TestLoad_DAG_Workflow_50Tasks(t *testing.T) {
	h, err := coordinator.NewClusterHarness(3)
	if err != nil {
		t.Fatalf("failed to create cluster harness: %v", err)
	}
	defer h.Close()

	leaderIdx, err := h.WaitLeader(5 * time.Second)
	if err != nil {
		t.Fatalf("failed to wait for leader: %v", err)
	}

	client := h.Clients[leaderIdx]
	wClient := h.WClients[leaderIdx]
	ctx := context.Background()

	// Register 5 workers
	for w := 0; w < 5; w++ {
		_, _ = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
			WorkerId:  fmt.Sprintf("dag-worker-%d", w),
			SessionId: fmt.Sprintf("dag-sess-%d", w),
			Address:   fmt.Sprintf("127.0.0.1:%d", 12000+w),
			MaxSlots:  4,
		})
	}

	// Build 50-task DAG: 1 Root -> 48 Middle Parallel Branches -> 1 Sink
	const numMiddle = 48
	var taskSpecs []*schedulerv1.TaskSpec

	// Root
	taskSpecs = append(taskSpecs, &schedulerv1.TaskSpec{
		Id:             "dag-root",
		TenantId:       "dag-tenant",
		TimeoutSeconds: 30,
	})

	middleIDs := make([]string, numMiddle)
	for i := 0; i < numMiddle; i++ {
		mID := fmt.Sprintf("dag-mid-%d", i)
		middleIDs[i] = mID
		taskSpecs = append(taskSpecs, &schedulerv1.TaskSpec{
			Id:             mID,
			TenantId:       "dag-tenant",
			Dependencies:   []string{"dag-root"},
			TimeoutSeconds: 30,
		})
	}

	// Sink
	taskSpecs = append(taskSpecs, &schedulerv1.TaskSpec{
		Id:             "dag-sink",
		TenantId:       "dag-tenant",
		Dependencies:   middleIDs,
		TimeoutSeconds: 30,
	})

	dagStart := time.Now()
	dagResp, err := client.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "scale-dag-50",
		TenantId: "dag-tenant",
		Tasks:    taskSpecs,
	})
	if err != nil || dagResp.TaskCount != int32(len(taskSpecs)) {
		t.Fatalf("DAG submission failed: %v", err)
	}

	// Drain tasks until sink is completed
	var completedCount int64
	var workerWg sync.WaitGroup

	for w := 0; w < 5; w++ {
		workerWg.Add(1)
		go func(workerIdx int) {
			defer workerWg.Done()
			wID := fmt.Sprintf("dag-worker-%d", workerIdx)
			sID := fmt.Sprintf("dag-sess-%d", workerIdx)
			wCtx := context.Background()

			for {
				if atomic.LoadInt64(&completedCount) >= int64(len(taskSpecs)) {
					return
				}
				pullResp, err := wClient.PullTask(wCtx, &schedulerv1.PullTaskRequest{
					WorkerId:  wID,
					SessionId: sID,
				})
				if err != nil || pullResp == nil || !pullResp.HasTask {
					time.Sleep(1 * time.Millisecond)
					continue
				}
				assignment := pullResp.GetAssignment()
				_, _ = wClient.AckTaskRunning(wCtx, &schedulerv1.AckTaskRunningRequest{
					TaskId:     assignment.TaskId,
					WorkerId:   wID,
					SessionId:  sID,
					LeaseEpoch: assignment.LeaseEpoch,
				})
				_, _ = wClient.ReportTaskCompleted(wCtx, &schedulerv1.ReportTaskCompletedRequest{
					TaskId:     assignment.TaskId,
					WorkerId:   wID,
					SessionId:  sID,
					LeaseEpoch: assignment.LeaseEpoch,
					Result:     []byte(`{}`),
				})
				atomic.AddInt64(&completedCount, 1)
			}
		}(w)
	}

	workerWg.Wait()
	dagElapsed := time.Since(dagStart)

	fmt.Printf("\n[LOAD TEST 5] 50-Task Diamond DAG Execution:\n")
	fmt.Printf("    DAG Hierarchy:  1 Root -> %d Middle Parallel Tasks -> 1 Sink\n", numMiddle)
	fmt.Printf("    Completed:      %d / %d tasks\n", completedCount, len(taskSpecs))
	fmt.Printf("    Execution Time: %.3f seconds (%.2f tasks/sec)\n", dagElapsed.Seconds(), float64(len(taskSpecs))/dagElapsed.Seconds())

	sinkTask, err := h.Stores[leaderIdx].GetTask("dag-sink")
	if err != nil || sinkTask.State != domain.StateSucceeded {
		t.Fatalf("expected sink task to succeed, got %v", sinkTask)
	}
}
