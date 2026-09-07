package benchmarks

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/coordinator"
)

// TestSystemProfiling captures CPU and Memory profiles during cluster operation
// to detect bottlenecks, heap allocation hot paths, and goroutine growth.
func TestSystemProfiling(t *testing.T) {
	// Create scratch dir for profile artifacts
	_ = os.MkdirAll("./profiles", 0755)

	cpuFile, err := os.Create("./profiles/cpu.pprof")
	if err != nil {
		t.Fatalf("failed to create cpu profile: %v", err)
	}
	defer cpuFile.Close()

	if err := pprof.StartCPUProfile(cpuFile); err != nil {
		t.Fatalf("failed to start cpu profile: %v", err)
	}
	defer pprof.StopCPUProfile()

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

	// Register 4 workers
	for w := 0; w < 4; w++ {
		_, _ = wClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
			WorkerId:  fmt.Sprintf("prof-worker-%d", w),
			SessionId: fmt.Sprintf("prof-sess-%d", w),
			Address:   fmt.Sprintf("127.0.0.1:%d", 13000+w),
			MaxSlots:  10,
		})
	}

	const taskCount = 1500
	var completedCount int64
	var wg sync.WaitGroup

	startTime := time.Now()

	// Submitter routine
	go func() {
		subCtx := context.Background()
		for i := 0; i < taskCount; i++ {
			_, _ = client.SubmitTask(subCtx, &schedulerv1.SubmitTaskRequest{
				Task: &schedulerv1.TaskSpec{
					Id:             fmt.Sprintf("prof-task-%d", i),
					TenantId:       fmt.Sprintf("tenant-%d", i%10),
					Priority:       int32(i % 50),
					TimeoutSeconds: 30,
				},
			})
		}
	}()

	// Worker drain routines
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			wID := fmt.Sprintf("prof-worker-%d", workerIdx)
			sID := fmt.Sprintf("prof-sess-%d", workerIdx)
			wCtx := context.Background()

			for {
				if atomic.LoadInt64(&completedCount) >= taskCount {
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

	// Stop CPU profiling
	pprof.StopCPUProfile()

	// Capture Heap Profile
	memFile, err := os.Create("./profiles/mem.pprof")
	if err == nil {
		runtime.GC() // Get up-to-date memory stats
		_ = pprof.WriteHeapProfile(memFile)
		_ = memFile.Close()
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	fmt.Printf("\n==================================================================================\n")
	fmt.Printf("           PROFILING RUN SUMMARY (1,500 TASKS)                                   \n")
	fmt.Printf("==================================================================================\n")
	fmt.Printf("    Processed:            %d tasks in %.3f seconds (%.2f tasks/sec)\n", completedCount, elapsed.Seconds(), float64(taskCount)/elapsed.Seconds())
	fmt.Printf("    HeapAlloc:            %.2f MB\n", float64(m.HeapAlloc)/1024/1024)
	fmt.Printf("    HeapSys:              %.2f MB\n", float64(m.HeapSys)/1024/1024)
	fmt.Printf("    NumGC Cycles:         %d\n", m.NumGC)
	fmt.Printf("    Active Goroutines:    %d\n", runtime.NumGoroutine())
	fmt.Printf("    Artifacts Written:    ./profiles/cpu.pprof, ./profiles/mem.pprof\n")
	fmt.Printf("==================================================================================\n")
}
