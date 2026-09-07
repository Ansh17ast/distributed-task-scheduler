package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestCoordinator_DAG_EndToEndDiamondExecution(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, &scheduler.PriorityPolicy{})
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// Track start and end timestamps for each task to verify strict dependency ordering
	var taskTiming sync.Map
	type taskSpan struct {
		start time.Time
		end   time.Time
	}

	handler := func(ctx context.Context, payload []byte) ([]byte, error) {
		taskID := string(payload)
		startTime := time.Now()
		time.Sleep(50 * time.Millisecond) // Simulated work
		endTime := time.Now()
		taskTiming.Store(taskID, taskSpan{start: startTime, end: endTime})
		return []byte("done-" + taskID), nil
	}

	logger := slog.Default()
	var daemons []*worker.Daemon
	for wIdx := 1; wIdx <= 2; wIdx++ {
		wCfg := config.DefaultWorkerConfig(fmt.Sprintf("worker-dag-%d", wIdx), addr)
		wCfg.MaxSlots = 2
		d := worker.NewDaemon(wCfg, handler, logger)
		if err := d.Start(ctx); err != nil {
			t.Fatalf("start worker failed: %v", err)
		}
		daemons = append(daemons, d)
	}
	defer func() {
		for _, d := range daemons {
			d.Stop()
		}
	}()

	// Submit Diamond DAG: A -> (B, C) -> D
	submitResp, err := coordClient.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-diamond-e2e",
		TenantId: "tenant-dag",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "A", Payload: []byte("A")},
			{Id: "B", Dependencies: []string{"A"}, Payload: []byte("B")},
			{Id: "C", Dependencies: []string{"A"}, Payload: []byte("C")},
			{Id: "D", Dependencies: []string{"B", "C"}, Payload: []byte("D")},
		},
	})
	if err != nil {
		t.Fatalf("submit dag failed: %v", err)
	}
	if submitResp.GetTaskCount() != 4 {
		t.Errorf("expected 4 tasks, got %d", submitResp.GetTaskCount())
	}

	// Wait for all 4 tasks to reach SUCCEEDED
	deadline := time.Now().Add(5 * time.Second)
	allSucceeded := false
	for time.Now().Before(deadline) {
		dagResp, err := coordClient.GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: "dag-diamond-e2e"})
		if err == nil && dagResp.GetSucceededTasks() == 4 {
			allSucceeded = true
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	if !allSucceeded {
		t.Fatalf("timeout waiting for diamond DAG to reach 4 succeeded tasks")
	}

	// Verify order invariants
	vA, _ := taskTiming.Load("A")
	vB, _ := taskTiming.Load("B")
	vC, _ := taskTiming.Load("C")
	vD, _ := taskTiming.Load("D")

	spanA := vA.(taskSpan)
	spanB := vB.(taskSpan)
	spanC := vC.(taskSpan)
	spanD := vD.(taskSpan)

	// A must complete before B and C start
	if spanA.end.After(spanB.start) {
		t.Errorf("order violation: task A ended at %v after task B started at %v", spanA.end, spanB.start)
	}
	if spanA.end.After(spanC.start) {
		t.Errorf("order violation: task A ended at %v after task C started at %v", spanA.end, spanC.start)
	}

	// B and C must complete before D starts
	if spanB.end.After(spanD.start) {
		t.Errorf("order violation: task B ended at %v after task D started at %v", spanB.end, spanD.start)
	}
	if spanC.end.After(spanD.start) {
		t.Errorf("order violation: task C ended at %v after task D started at %v", spanC.end, spanD.start)
	}

	// Check final DAG status through store
	dagObj, tasks, err := store.GetDAG("dag-diamond-e2e")
	if err != nil || len(tasks) != 4 {
		t.Errorf("failed retrieving DAG from store: %v", err)
	}
	if dagObj.ID != "dag-diamond-e2e" {
		t.Errorf("expected dag-diamond-e2e, got %s", dagObj.ID)
	}
}

func TestCoordinator_DAG_FailurePropagation(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	// Handler: task-B fails fatally
	handler := func(ctx context.Context, payload []byte) ([]byte, error) {
		name := string(payload)
		if name == "B" {
			return nil, errors.New("simulated fatal unrecoverable failure")
		}
		return []byte("ok"), nil
	}

	wCfg := config.DefaultWorkerConfig("worker-dag-fail", addr)
	wCfg.MaxSlots = 2
	d := worker.NewDaemon(wCfg, handler, slog.Default())
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start worker failed: %v", err)
	}
	defer d.Stop()

	// Pipeline: A -> B -> C (B has max_retries = 0)
	_, err = coordClient.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-failure-cascade",
		TenantId: "tenant-dag-fail",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "A", Payload: []byte("A")},
			{Id: "B", Dependencies: []string{"A"}, Payload: []byte("B"), MaxRetries: 0},
			{Id: "C", Dependencies: []string{"B"}, Payload: []byte("C")},
		},
	})
	if err != nil {
		t.Fatalf("submit dag failed: %v", err)
	}

	// Wait for execution to settle
	deadline := time.Now().Add(4 * time.Second)
	settled := false
	for time.Now().Before(deadline) {
		tA, _ := store.GetTask("A")
		tB, _ := store.GetTask("B")
		tC, _ := store.GetTask("C")
		if tA != nil && tB != nil && tC != nil {
			if tA.State == domain.StateSucceeded && tB.State == domain.StateFailed && tC.State == domain.StateCancelled {
				settled = true
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}

	if !settled {
		tA, _ := store.GetTask("A")
		tB, _ := store.GetTask("B")
		tC, _ := store.GetTask("C")
		t.Fatalf("DAG did not settle as expected: A=%v, B=%v, C=%v", tA.State, tB.State, tC.State)
	}
}

func TestCoordinator_DAG_RetryRecovery(t *testing.T) {
	_, addr, store, cleanup := setupTestCoordinator(t, nil)
	defer cleanup()

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial failed: %v", err)
	}
	defer conn.Close()

	coordClient := schedulerv1.NewCoordinatorServiceClient(conn)
	ctx := context.Background()

	var attemptsB atomic.Int32
	handler := func(ctx context.Context, payload []byte) ([]byte, error) {
		name := string(payload)
		if name == "B" {
			att := attemptsB.Add(1)
			if att == 1 {
				return nil, errors.New("transient database glitch")
			}
			return []byte("b-succeeded-on-retry"), nil
		}
		return []byte("ok"), nil
	}

	wCfg := config.DefaultWorkerConfig("worker-dag-retry", addr)
	wCfg.MaxSlots = 2
	d := worker.NewDaemon(wCfg, handler, slog.Default())
	if err := d.Start(ctx); err != nil {
		t.Fatalf("start worker failed: %v", err)
	}
	defer d.Stop()

	// A -> B -> C (B has max_retries = 2)
	_, err = coordClient.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-retry-recover",
		TenantId: "tenant-dag-retry",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "A", Payload: []byte("A")},
			{Id: "B", Dependencies: []string{"A"}, Payload: []byte("B"), MaxRetries: 2},
			{Id: "C", Dependencies: []string{"B"}, Payload: []byte("C")},
		},
	})
	if err != nil {
		t.Fatalf("submit dag failed: %v", err)
	}

	// Wait for all 3 tasks to reach SUCCEEDED
	deadline := time.Now().Add(5 * time.Second)
	allDone := false
	for time.Now().Before(deadline) {
		tA, _ := store.GetTask("A")
		tB, _ := store.GetTask("B")
		tC, _ := store.GetTask("C")
		if tA != nil && tB != nil && tC != nil {
			if tA.State == domain.StateSucceeded && tB.State == domain.StateSucceeded && tC.State == domain.StateSucceeded {
				allDone = true
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
	}

	if !allDone {
		tA, _ := store.GetTask("A")
		tB, _ := store.GetTask("B")
		tC, _ := store.GetTask("C")
		t.Fatalf("DAG tasks did not all succeed: A=%v, B=%v, C=%v", tA.State, tB.State, tC.State)
	}

	if attemptsB.Load() != 2 {
		t.Errorf("expected exactly 2 execution attempts for B, got %d", attemptsB.Load())
	}
}
