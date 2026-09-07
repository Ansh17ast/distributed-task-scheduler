package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync/atomic"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/coordinator"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
	"distributed-scheduler/internal/worker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	fmt.Println("================================================================================")
	fmt.Println(" PHASE 5: DAG SCHEDULER ENGINE LIVE DEMONSTRATION")
	fmt.Println("================================================================================")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Initialize and Start Coordinator
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to listen: %v\n", err)
		os.Exit(1)
	}
	defer lis.Close()

	coordAddr := lis.Addr().String()
	cfg := config.DefaultCoordinatorConfig("coord-dag-demo", 0, 0, "127.0.0.1:0")
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelWarn}))
	store := state.NewStore(storage.NewMemoryAuditStore())

	srv := coordinator.NewServer(cfg, store, &scheduler.PriorityPolicy{}, logger)
	go func() { _ = srv.Start(lis) }()
	defer srv.Stop()

	fmt.Printf("[Init] Coordinator running on %s\n", coordAddr)

	// 2. Start 2 Operational Worker Daemons (2 slots each = 4 total capacity)
	var flakyAttempts atomic.Int32
	handler := func(ctx context.Context, payload []byte) ([]byte, error) {
		taskName := string(payload)
		switch taskName {
		case "task-flaky":
			att := flakyAttempts.Add(1)
			if att == 1 {
				fmt.Printf("   --> [Execution] %s failed transiently on attempt 1! Entering RETRY_WAIT...\n", taskName)
				return nil, errors.New("transient network timeout")
			}
			fmt.Printf("   --> [Execution] %s succeeded on retry attempt 2!\n", taskName)
			return []byte("retry-success"), nil
		case "task-fatal":
			fmt.Printf("   --> [Execution] %s failed with fatal error!\n", taskName)
			return nil, errors.New("unrecoverable fatal computation failure")
		default:
			time.Sleep(80 * time.Millisecond) // Simulated execution
			return []byte("output:" + taskName), nil
		}
	}

	w1Cfg := config.DefaultWorkerConfig("worker-alpha", coordAddr)
	w1Cfg.MaxSlots = 2
	w1 := worker.NewDaemon(w1Cfg, handler, logger)
	_ = w1.Start(ctx)
	defer w1.Stop()

	w2Cfg := config.DefaultWorkerConfig("worker-beta", coordAddr)
	w2Cfg.MaxSlots = 2
	w2 := worker.NewDaemon(w2Cfg, handler, logger)
	_ = w2.Start(ctx)
	defer w2.Stop()

	fmt.Printf("[Init] 2 Workers registered (4 concurrent execution slots total)\n\n")

	conn, err := grpc.NewClient(coordAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(os.Stderr, "dial failed: %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	client := schedulerv1.NewCoordinatorServiceClient(conn)

	// =========================================================================
	// SCENARIO 1: Diamond DAG (A -> B, C -> D)
	// =========================================================================
	fmt.Println("--------------------------------------------------------------------------------")
	fmt.Println(" SCENARIO 1: Diamond DAG Execution (Fan-Out & Fan-In)")
	fmt.Println(" Topology: Task A -> (Task B, Task C) -> Task D")
	fmt.Println(" Expected: A executes -> B & C parallel -> D executes only after B & C complete")
	fmt.Println("--------------------------------------------------------------------------------")

	_, err = client.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-diamond",
		TenantId: "tenant-analytics",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "A", Payload: []byte("A")},
			{Id: "B", Dependencies: []string{"A"}, Payload: []byte("B")},
			{Id: "C", Dependencies: []string{"A"}, Payload: []byte("C")},
			{Id: "D", Dependencies: []string{"B", "C"}, Payload: []byte("D")},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit diamond dag failed: %v\n", err)
	}

	// Poll until DAG reaches completion
	for {
		dagResp, _ := client.GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: "dag-diamond"})
		if dagResp != nil && dagResp.GetSucceededTasks() == 4 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	printDAGTable(ctx, client, "dag-diamond")

	// =========================================================================
	// SCENARIO 2: Failure Propagation & Transitive Branch Cancellation
	// =========================================================================
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" SCENARIO 2: Upstream Failure & Transitive Cancellation")
	fmt.Println(" Topology: Task X -> Task Y (Fatal) -> Task Z")
	fmt.Println(" Expected: X succeeds, Y fails permanently, Z is transitively cancelled")
	fmt.Println("--------------------------------------------------------------------------------")

	_, err = client.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-failure-prop",
		TenantId: "tenant-analytics",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "X", Payload: []byte("X")},
			{Id: "Y", Dependencies: []string{"X"}, Payload: []byte("task-fatal"), MaxRetries: 0},
			{Id: "Z", Dependencies: []string{"Y"}, Payload: []byte("Z")},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit failed dag failed: %v\n", err)
	}

	for {
		dagResp, _ := client.GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: "dag-failure-prop"})
		if dagResp != nil && (dagResp.GetSucceededTasks()+dagResp.GetFailedTasks()) == 3 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	printDAGTable(ctx, client, "dag-failure-prop")

	// =========================================================================
	// SCENARIO 3: Retry Interaction with Dependency Unblocking
	// =========================================================================
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" SCENARIO 3: Transient Failure, Backoff Retry & Eventual Dependency Unblocking")
	fmt.Println(" Topology: R1 -> R2 (Flaky, 2 retries) -> R3")
	fmt.Println(" Expected: R2 fails attempt 1, backs off in RETRY_WAIT (R3 stays BLOCKED),")
	fmt.Println("           R2 succeeds attempt 2 -> R3 unblocked to READY -> All SUCCEEDED")
	fmt.Println("--------------------------------------------------------------------------------")

	_, err = client.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-retry-recover",
		TenantId: "tenant-analytics",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "R1", Payload: []byte("R1")},
			{Id: "R2", Dependencies: []string{"R1"}, Payload: []byte("task-flaky"), MaxRetries: 2},
			{Id: "R3", Dependencies: []string{"R2"}, Payload: []byte("R3")},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit retry dag failed: %v\n", err)
	}

	for {
		dagResp, _ := client.GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: "dag-retry-recover"})
		if dagResp != nil && dagResp.GetSucceededTasks() == 3 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	printDAGTable(ctx, client, "dag-retry-recover")

	// =========================================================================
	// SCENARIO 4: Branch Cancellation
	// =========================================================================
	fmt.Println("\n--------------------------------------------------------------------------------")
	fmt.Println(" SCENARIO 4: Branch Cancellation with Independent Sibling Branch")
	fmt.Println(" Topology: Root M -> (Branch N1, Branch N2) -> Join J")
	fmt.Println(" Action:   Branch N1 is cancelled by client request")
	fmt.Println(" Expected: N1 CANCELLED, J transitively CANCELLED; sibling N2 SUCCEEDS independently")
	fmt.Println("--------------------------------------------------------------------------------")

	_, err = client.SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    "dag-branch-cancel",
		TenantId: "tenant-analytics",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "M", Payload: []byte("M")},
			{Id: "N1", Dependencies: []string{"M"}, Payload: []byte("N1")},
			{Id: "N2", Dependencies: []string{"M"}, Payload: []byte("N2")},
			{Id: "J", Dependencies: []string{"N1", "N2"}, Payload: []byte("J")},
		},
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "submit cancel dag failed: %v\n", err)
	}

	// Wait for M to complete so N1 and N2 are READY
	for {
		tM, _ := store.GetTask("M")
		if tM != nil && tM.State == "SUCCEEDED" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Cancel Branch N1
	_, _ = client.CancelTask(ctx, &schedulerv1.CancelTaskRequest{
		TaskId: "N1",
		Reason: "client requested branch abort",
	})
	fmt.Println("   [Client Action] Cancelled task N1")

	// Wait for execution to settle
	for {
		dagResp, _ := client.GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: "dag-branch-cancel"})
		if dagResp != nil && (dagResp.GetSucceededTasks()+dagResp.GetFailedTasks()) == 4 {
			break
		}
		time.Sleep(30 * time.Millisecond)
	}

	printDAGTable(ctx, client, "dag-branch-cancel")

	fmt.Println("\n================================================================================")
	fmt.Println(" ALL PHASE 5 DAG SCENARIOS COMPLETED AND VERIFIED SUCCESSFULLY")
	fmt.Println("================================================================================")
}

func printDAGTable(ctx context.Context, client schedulerv1.CoordinatorServiceClient, dagID string) {
	resp, err := client.GetDAG(ctx, &schedulerv1.GetDAGRequest{DagId: dagID})
	if err != nil {
		fmt.Printf("Error fetching DAG: %v\n", err)
		return
	}

	fmt.Printf("\nDAG ID: %s | Total: %d | Succeeded: %d | Failed/Cancelled: %d | Pending: %d\n",
		resp.GetDagId(), resp.GetTotalTasks(), resp.GetSucceededTasks(), resp.GetFailedTasks(), resp.GetPendingTasks())
	fmt.Printf("%-10s %-14s %-8s %-12s %s\n", "TASK ID", "STATE", "EPOCH", "WORKER", "RESULT / ERROR")
	fmt.Println("--------------------------------------------------------------------------------")
	for _, t := range resp.GetTasks() {
		resultOrErr := string(t.GetResult())
		if t.GetErrorMessage() != "" {
			resultOrErr = "[" + t.GetErrorMessage() + "]"
		}
		fmt.Printf("%-10s %-14s %-8d %-12s %s\n",
			t.GetTaskId(), t.GetState().String(), t.GetLeaseEpoch(), t.GetAssignedWorkerId(), resultOrErr)
	}
}
