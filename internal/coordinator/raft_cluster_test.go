package coordinator

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
	"distributed-scheduler/internal/worker"
	"github.com/hashicorp/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// clusterHarness manages a 3-coordinator in-memory Raft cluster for tests.
type clusterHarness struct {
	t          *testing.T
	count      int
	transports []*raft.InmemTransport
	nodes      []*consensus.RaftNode
	servers    []*Server
	stores     []*state.Store
	listeners  []net.Listener
	grpcAddrs  []string
	clients    []schedulerv1.CoordinatorServiceClient
	wClients   []schedulerv1.WorkerServiceClient
	conns      []*grpc.ClientConn
}

func newClusterHarness(t *testing.T, count int) *clusterHarness {
	h := &clusterHarness{
		t:          t,
		count:      count,
		transports: make([]*raft.InmemTransport, count),
		nodes:      make([]*consensus.RaftNode, count),
		servers:    make([]*Server, count),
		stores:     make([]*state.Store, count),
		listeners:  make([]net.Listener, count),
		grpcAddrs:  make([]string, count),
		clients:    make([]schedulerv1.CoordinatorServiceClient, count),
		wClients:   make([]schedulerv1.WorkerServiceClient, count),
		conns:      make([]*grpc.ClientConn, count),
	}

	// 1. Create Inmem transports
	peers := make([]consensus.RaftPeer, count)
	for i := 0; i < count; i++ {
		addr, trans := raft.NewInmemTransport(raft.ServerAddress(fmt.Sprintf("127.0.0.1:2000%d", i+1)))
		h.transports[i] = trans
		peers[i] = consensus.RaftPeer{
			ID:      fmt.Sprintf("coord-%d", i+1),
			Address: string(addr),
		}
	}

	// Fully mesh the in-memory transports
	for i := 0; i < count; i++ {
		for j := 0; j < count; j++ {
			if i != j {
				h.transports[i].Connect(h.transports[j].LocalAddr(), h.transports[j])
			}
		}
	}

	silentLogger := slog.New(slog.NewTextHandler(io.Discard, nil))
	// 2. Initialize Stores, FSMs, RaftNodes, and Servers
	for i := 0; i < count; i++ {
		store := state.NewStore(storage.NewMemoryAuditStore())
		h.stores[i] = store
		fsm := consensus.NewFSM(store, silentLogger)

		cfg := &consensus.NodeConfig{
			NodeID:       fmt.Sprintf("coord-%d", i+1),
			BindAddr:     string(h.transports[i].LocalAddr()),
			Bootstrap:    i == 0, // Node 1 bootstraps cluster configuration
			InMemory:     true,
			Peers:        peers,
			HeartbeatDur: 25 * time.Millisecond,
			ElectionMin:  75 * time.Millisecond,
			ElectionMax:  150 * time.Millisecond,
		}

		node, err := consensus.NewRaftNode(cfg, fsm, h.transports[i], silentLogger)
		if err != nil {
			t.Fatalf("failed creating raft node %d: %v", i+1, err)
		}
		h.nodes[i] = node

		coordCfg := config.DefaultCoordinatorConfig(fmt.Sprintf("coord-%d", i+1), 0, 0, cfg.BindAddr)
		coordCfg.WorkerLeaseDur = 2 * time.Second
		coordCfg.LeaseGraceWindow = 1 * time.Second
		coordCfg.FailoverReconcile = 1 * time.Second
		coordCfg.ReaperInterval = 100 * time.Millisecond

		server := NewServer(coordCfg, store, &scheduler.FIFOPolicy{}, silentLogger)
		server.SetRaftNode(node)
		h.servers[i] = server

		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("failed listening gRPC for server %d: %v", i+1, err)
		}
		h.listeners[i] = lis
		h.grpcAddrs[i] = lis.Addr().String()

		go func(s *Server, l net.Listener) {
			_ = s.Start(l)
		}(server, lis)

		conn, err := grpc.NewClient(h.grpcAddrs[i], grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			t.Fatalf("failed dialing gRPC for server %d: %v", i+1, err)
		}
		h.conns[i] = conn
		h.clients[i] = schedulerv1.NewCoordinatorServiceClient(conn)
		h.wClients[i] = schedulerv1.NewWorkerServiceClient(conn)
	}

	return h
}

func (h *clusterHarness) close() {
	for i := 0; i < h.count; i++ {
		if h.conns[i] != nil {
			_ = h.conns[i].Close()
		}
		if h.servers[i] != nil {
			h.servers[i].Stop()
		}
		if h.listeners[i] != nil {
			_ = h.listeners[i].Close()
		}
	}
}

func (h *clusterHarness) findLeaderIndex() int {
	for i := 0; i < h.count; i++ {
		if h.nodes[i] != nil && h.nodes[i].IsLeader() {
			return i
		}
	}
	return -1
}

func (h *clusterHarness) waitLeader(timeout time.Duration) int {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		idx := h.findLeaderIndex()
		if idx >= 0 {
			return idx
		}
		time.Sleep(20 * time.Millisecond)
	}
	h.t.Fatalf("timeout waiting for leader election")
	return -1
}

// 1. Initial 3-node election
func TestRaft_InitialElection(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	if leaderIdx < 0 {
		t.Fatalf("expected 1 leader to be elected")
	}

	// Verify other 2 nodes are followers
	for i := 0; i < 3; i++ {
		if i != leaderIdx {
			if h.nodes[i].IsLeader() {
				t.Fatalf("node %d unexpectedly reports being leader", i+1)
			}
		}
	}
}

// 2. Exactly one leader per term
func TestRaft_ExactlyOneLeaderPerTerm(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	leaderTerm := h.nodes[leaderIdx].CurrentTerm()

	leaderCount := 0
	for i := 0; i < 3; i++ {
		if h.nodes[i].IsLeader() && h.nodes[i].CurrentTerm() == leaderTerm {
			leaderCount++
		}
	}
	if leaderCount != 1 {
		t.Fatalf("expected exactly 1 leader in term %d, got %d", leaderTerm, leaderCount)
	}
}

// 3. State replication across all 3 nodes' FSMs
func TestRaft_StateReplication(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Submit task through leader gRPC
	taskID := "task-rep-test"
	resp, err := h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:         taskID,
			TenantId:   "tenant-test",
			Priority:   50,
			MaxRetries: 2,
		},
	})
	if err != nil {
		t.Fatalf("submit task to leader failed: %v", err)
	}
	if resp.GetTaskId() != taskID {
		t.Fatalf("expected task id %s, got %s", taskID, resp.GetTaskId())
	}

	// Allow brief replication propagation
	time.Sleep(100 * time.Millisecond)

	// Verify all 3 stores have the replicated task with identical state and epoch
	for i := 0; i < 3; i++ {
		task, err := h.stores[i].GetTask(taskID)
		if err != nil {
			t.Fatalf("node %d failed to replicate task: %v", i+1, err)
		}
		if task.State != domain.StateReady {
			t.Fatalf("node %d task in state %s, expected READY", i+1, task.State)
		}
		if task.RaftAppliedIndex == 0 {
			t.Fatalf("node %d RaftAppliedIndex is 0", i+1)
		}
	}
}

// 4. Leader kill & 5. Leader replacement
func TestRaft_LeaderKillAndReplacement(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	oldLeaderIdx := h.waitLeader(3 * time.Second)
	t.Logf("Initial leader is coord-%d", oldLeaderIdx+1)

	// Kill leader by shutting down Raft and server
	_ = h.nodes[oldLeaderIdx].Shutdown()
	h.servers[oldLeaderIdx].Stop()
	h.transports[oldLeaderIdx].DisconnectAll()
	h.nodes[oldLeaderIdx] = nil

	// Wait for remaining 2 nodes to elect a replacement leader
	deadline := time.Now().Add(4 * time.Second)
	newLeaderIdx := -1
	for time.Now().Before(deadline) {
		for i := 0; i < 3; i++ {
			if i != oldLeaderIdx && h.nodes[i] != nil && h.nodes[i].IsLeader() {
				newLeaderIdx = i
				break
			}
		}
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if newLeaderIdx < 0 {
		t.Fatalf("failover failed: no new leader was elected")
	}
	t.Logf("New elected leader is coord-%d", newLeaderIdx+1)
	if newLeaderIdx == oldLeaderIdx {
		t.Fatalf("new leader cannot be old killed leader")
	}
}

// 6. Committed state preservation across leader failover
func TestRaft_CommittedStatePreservation(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Submit task to initial leader
	taskID := "task-preserve-failover"
	_, err := h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:         taskID,
			TenantId:   "tenant-core",
			Priority:   88,
			MaxRetries: 3,
		},
	})
	if err != nil {
		t.Fatalf("task submit failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Kill initial leader
	_ = h.nodes[leaderIdx].Shutdown()
	h.servers[leaderIdx].Stop()
	h.transports[leaderIdx].DisconnectAll()
	h.nodes[leaderIdx] = nil

	// Locate new leader
	newLeaderIdx := -1
	for attempt := 0; attempt < 80; attempt++ {
		for i := 0; i < 3; i++ {
			if h.nodes[i] != nil && h.nodes[i].IsLeader() {
				newLeaderIdx = i
				break
			}
		}
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if newLeaderIdx < 0 {
		t.Fatalf("failover did not produce new leader")
	}

	// Query task status from new leader's gRPC endpoint
	taskResp, err := h.clients[newLeaderIdx].GetTask(ctx, &schedulerv1.GetTaskRequest{TaskId: taskID})
	if err != nil {
		t.Fatalf("failed to query task from new leader: %v", err)
	}
	if taskResp.GetTaskId() != taskID {
		t.Fatalf("expected task id %s, got %s", taskID, taskResp.GetTaskId())
	}
	if taskResp.GetPriority() != 88 {
		t.Fatalf("expected priority 88, got %d", taskResp.GetPriority())
	}
	if taskResp.GetState() != schedulerv1.TaskState_TASK_STATE_READY {
		t.Fatalf("expected state READY, got %s", taskResp.GetState())
	}
}

// 7. Scheduling resumes after failover
func TestRaft_SchedulingResumesAfterFailover(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Submit task
	taskID := "task-resume-after-failover"
	_, _ = h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:       taskID,
			TenantId: "tenant-prod",
			Priority: 10,
		},
	})
	time.Sleep(50 * time.Millisecond)

	// Kill leader
	_ = h.nodes[leaderIdx].Shutdown()
	h.servers[leaderIdx].Stop()
	h.transports[leaderIdx].DisconnectAll()
	h.nodes[leaderIdx] = nil

	// Find new leader
	var newLeaderIdx int
	for attempt := 0; attempt < 80; attempt++ {
		newLeaderIdx = h.findLeaderIndex()
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Connect worker to new leader
	wCfg := config.DefaultWorkerConfig("w-failover", h.grpcAddrs[newLeaderIdx])
	wCfg.MaxSlots = 1
	wDaemon := worker.NewDaemon(wCfg, nil, slog.Default())
	if err := wDaemon.Start(ctx); err != nil {
		t.Fatalf("worker failed to start against new leader: %v", err)
	}
	defer wDaemon.Stop()

	// Verify task is pulled and reaches SUCCEEDED
	deadline := time.Now().Add(3 * time.Second)
	succeeded := false
	for time.Now().Before(deadline) {
		tCheck, err := h.stores[newLeaderIdx].GetTask(taskID)
		if err == nil && tCheck.State == domain.StateSucceeded {
			succeeded = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !succeeded {
		t.Fatalf("task failed to execute and reach SUCCEEDED under new leader")
	}
}

// 8. Worker lease reconciliation after leader failover
func TestRaft_WorkerLeaseReconciliation(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Register worker and pull task on initial leader
	taskID := "task-lease-reconcile"
	_, _ = h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-r", Priority: 50},
	})
	time.Sleep(50 * time.Millisecond)

	_, _ = h.wClients[leaderIdx].RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  "worker-reconcile",
		SessionId: "session-rec-1",
		MaxSlots:  1,
	})

	pullResp, _ := h.wClients[leaderIdx].PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       "worker-reconcile",
		SessionId:      "session-rec-1",
		AvailableSlots: 1,
	})
	epoch := pullResp.GetAssignment().GetLeaseEpoch()

	_, _ = h.wClients[leaderIdx].AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   "worker-reconcile",
		SessionId:  "session-rec-1",
		TaskId:     taskID,
		LeaseEpoch: epoch,
	})

	// Kill initial leader
	_ = h.nodes[leaderIdx].Shutdown()
	h.servers[leaderIdx].Stop()
	h.transports[leaderIdx].DisconnectAll()
	h.nodes[leaderIdx] = nil

	// Wait for new leader
	newLeaderIdx := -1
	for i := 0; i < 80; i++ {
		newLeaderIdx = h.findLeaderIndex()
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Worker sends heartbeat with (WorkerID, SessionID, LeaseEpoch, TaskID) to new leader
	hbResp, err := h.wClients[newLeaderIdx].Heartbeat(ctx, &schedulerv1.HeartbeatRequest{
		WorkerId:       "worker-reconcile",
		SessionId:      "session-rec-1",
		AvailableSlots: 0,
		ActiveLeases: []*schedulerv1.ActiveLeaseHeartbeat{
			{TaskId: taskID, LeaseEpoch: epoch},
		},
	})
	if err != nil {
		t.Fatalf("heartbeat to new leader failed: %v", err)
	}
	if len(hbResp.GetRevokedTaskIds()) > 0 {
		t.Fatalf("active lease was unexpectedly revoked: %v", hbResp.GetRevokedTaskIds())
	}

	// Worker completes task on new leader
	compResp, err := h.wClients[newLeaderIdx].ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "worker-reconcile",
		SessionId:  "session-rec-1",
		TaskId:     taskID,
		LeaseEpoch: epoch,
		Result:     []byte("reconciled-ok"),
	})
	if err != nil || !compResp.GetAcknowledged() {
		t.Fatalf("completion on new leader rejected: %v", err)
	}

	finalTask, _ := h.stores[newLeaderIdx].GetTask(taskID)
	if finalTask.State != domain.StateSucceeded {
		t.Fatalf("expected SUCCEEDED, got %s", finalTask.State)
	}
}

// 9. RETRY_WAIT reconstruction after leader change
func TestRaft_RetryWaitReconstruction(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Submit task with max_retries = 2
	taskID := "task-retry-reconstruct"
	_, _ = h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-retry", Priority: 50, MaxRetries: 2},
	})
	time.Sleep(50 * time.Millisecond)

	// Lease and fail with retryable = true
	_, _ = h.wClients[leaderIdx].RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "w-retry", SessionId: "s-retry", MaxSlots: 1,
	})
	pullResp, _ := h.wClients[leaderIdx].PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: "w-retry", SessionId: "s-retry", AvailableSlots: 1,
	})
	epoch := pullResp.GetAssignment().GetLeaseEpoch()
	_, _ = h.wClients[leaderIdx].AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: "w-retry", SessionId: "s-retry", TaskId: taskID, LeaseEpoch: epoch,
	})
	_, _ = h.wClients[leaderIdx].ReportTaskFailed(ctx, &schedulerv1.ReportTaskFailedRequest{
		WorkerId: "w-retry", SessionId: "s-retry", TaskId: taskID, LeaseEpoch: epoch,
		ErrorMessage: "transient lock", Retryable: true,
	})
	time.Sleep(100 * time.Millisecond) // Ensure Raft log is replicated and applied on followers

	// Verify task is in RETRY_WAIT on initial leader
	tWait, _ := h.stores[leaderIdx].GetTask(taskID)
	if tWait.State != domain.StateRetryWait {
		t.Fatalf("expected RETRY_WAIT, got %s", tWait.State)
	}

	// Kill initial leader while task is in RETRY_WAIT
	_ = h.nodes[leaderIdx].Shutdown()
	h.servers[leaderIdx].Stop()
	h.transports[leaderIdx].DisconnectAll()
	h.nodes[leaderIdx] = nil

	// New leader elected
	newLeaderIdx := -1
	for i := 0; i < 80; i++ {
		newLeaderIdx = h.findLeaderIndex()
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// New leader must reconstruct in-memory retry timer and promote task to READY
	deadline := time.Now().Add(3 * time.Second)
	promoted := false
	for time.Now().Before(deadline) {
		tCheck, err := h.stores[newLeaderIdx].GetTask(taskID)
		if err == nil && tCheck.State == domain.StateReady {
			promoted = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !promoted {
		tCheck, _ := h.stores[newLeaderIdx].GetTask(taskID)
		t.Fatalf("task failed to promote to READY on new leader, state is %s", tCheck.State)
	}
}

// 10. DAG state recovery after coordinator failover
func TestRaft_DAGStateRecovery(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Submit 2-task DAG: root -> child
	dagID := "dag-failover-rec"
	_, err := h.clients[leaderIdx].SubmitDAG(ctx, &schedulerv1.SubmitDAGRequest{
		DagId:    dagID,
		TenantId: "tenant-dag",
		Tasks: []*schedulerv1.TaskSpec{
			{Id: "dag-root", TenantId: "tenant-dag", Priority: 50},
			{Id: "dag-child", TenantId: "tenant-dag", Priority: 50, Dependencies: []string{"dag-root"}},
		},
	})
	if err != nil {
		t.Fatalf("submit DAG failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Execute root task on initial leader
	_, _ = h.wClients[leaderIdx].RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "w-dag", SessionId: "s-dag", MaxSlots: 1,
	})
	pull, _ := h.wClients[leaderIdx].PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: "w-dag", SessionId: "s-dag", AvailableSlots: 1,
	})
	_, _ = h.wClients[leaderIdx].AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: "w-dag", SessionId: "s-dag", TaskId: "dag-root", LeaseEpoch: pull.GetAssignment().GetLeaseEpoch(),
	})
	_, _ = h.wClients[leaderIdx].ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId: "w-dag", SessionId: "s-dag", TaskId: "dag-root", LeaseEpoch: pull.GetAssignment().GetLeaseEpoch(),
	})

	// Kill initial leader
	_ = h.nodes[leaderIdx].Shutdown()
	h.servers[leaderIdx].Stop()
	h.transports[leaderIdx].DisconnectAll()
	h.nodes[leaderIdx] = nil

	// New leader elected
	newLeaderIdx := -1
	for i := 0; i < 80; i++ {
		newLeaderIdx = h.findLeaderIndex()
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Verify dag-child is READY on new leader and can be pulled and completed
	childTask, err := h.stores[newLeaderIdx].GetTask("dag-child")
	if err != nil || childTask.State != domain.StateReady {
		t.Fatalf("expected dag-child to be READY on new leader, state=%v, err=%v", childTask.State, err)
	}

	_, _ = h.wClients[newLeaderIdx].RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "w-dag-2", SessionId: "s-dag-2", MaxSlots: 1,
	})
	pullChild, _ := h.wClients[newLeaderIdx].PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: "w-dag-2", SessionId: "s-dag-2", AvailableSlots: 1,
	})
	if !pullChild.GetHasTask() || pullChild.GetAssignment().GetTaskId() != "dag-child" {
		t.Fatalf("failed pulling dag-child on new leader")
	}
}

// 11. Network Partition: Minority cannot commit, 12. Majority partition continues, 13. Partition healing
func TestRaft_NetworkPartition_MinorityVsMajorityAndHealing(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Ensure leaderIdx is coord-1 for predictability (or identify which node is leader)
	isolatedIdx := leaderIdx
	majority1 := (isolatedIdx + 1) % 3
	majority2 := (isolatedIdx + 2) % 3

	// PARTITION: Disconnect isolatedIdx from majority nodes
	h.transports[isolatedIdx].Disconnect(h.transports[majority1].LocalAddr())
	h.transports[isolatedIdx].Disconnect(h.transports[majority2].LocalAddr())
	h.transports[majority1].Disconnect(h.transports[isolatedIdx].LocalAddr())
	h.transports[majority2].Disconnect(h.transports[isolatedIdx].LocalAddr())

	// 11. MINORITY: Isolated leader cannot commit new mutations
	_, err := h.clients[isolatedIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: "task-minority-lost", TenantId: "t-min", Priority: 1},
	})
	if err == nil {
		t.Fatalf("isolated minority leader unexpectedly succeeded in committing mutation")
	}

	// 12. MAJORITY: Majority partition elects leader and continues
	var majorityLeaderIdx int = -1
	for attempt := 0; attempt < 80; attempt++ {
		if h.nodes[majority1].IsLeader() {
			majorityLeaderIdx = majority1
			break
		}
		if h.nodes[majority2].IsLeader() {
			majorityLeaderIdx = majority2
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if majorityLeaderIdx < 0 {
		t.Fatalf("majority partition failed to elect a leader")
	}

	// Majority commits a task
	majTaskID := "task-majority-committed"
	majResp, err := h.clients[majorityLeaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: majTaskID, TenantId: "t-maj", Priority: 99},
	})
	if err != nil {
		t.Fatalf("majority leader failed to commit task: %v", err)
	}
	if majResp.GetTaskId() != majTaskID {
		t.Fatalf("unexpected task id: %s", majResp.GetTaskId())
	}

	// 13. HEALING: Reconnect isolated node
	h.transports[isolatedIdx].Connect(h.transports[majority1].LocalAddr(), h.transports[majority1])
	h.transports[isolatedIdx].Connect(h.transports[majority2].LocalAddr(), h.transports[majority2])
	h.transports[majority1].Connect(h.transports[isolatedIdx].LocalAddr(), h.transports[isolatedIdx])
	h.transports[majority2].Connect(h.transports[isolatedIdx].LocalAddr(), h.transports[isolatedIdx])

	// Wait for healed node to discover higher term, step down, and replicate state
	time.Sleep(300 * time.Millisecond)
	if h.nodes[isolatedIdx].IsLeader() {
		t.Fatalf("healed minority node failed to step down from leadership")
	}

	// Healed node catches up: majTaskID must now exist in healed node's store
	healedTask, err := h.stores[isolatedIdx].GetTask(majTaskID)
	if err != nil || healedTask.State != domain.StateReady {
		t.Fatalf("healed node failed to replicate committed state after partition healed")
	}
}

// 14. Restarted leader rejoins with persistent state (TCP & BoltDB)
func TestRaft_RestartedLeaderRejoinsPersistentState(t *testing.T) {
	tempDir := t.TempDir()

	// Use real TCP listeners for persistent cluster test
	l1, _ := net.Listen("tcp", "127.0.0.1:0")
	l2, _ := net.Listen("tcp", "127.0.0.1:0")
	l3, _ := net.Listen("tcp", "127.0.0.1:0")

	a1 := l1.Addr().String()
	a2 := l2.Addr().String()
	a3 := l3.Addr().String()

	_ = l1.Close()
	_ = l2.Close()
	_ = l3.Close()

	peers := []consensus.RaftPeer{
		{ID: "c-1", Address: a1},
		{ID: "c-2", Address: a2},
		{ID: "c-3", Address: a3},
	}

	store1 := state.NewStore(storage.NewMemoryAuditStore())
	fsm1 := consensus.NewFSM(store1, slog.Default())
	cfg1 := &consensus.NodeConfig{
		NodeID: "c-1", BindAddr: a1, DataDir: fmt.Sprintf("%s/c1", tempDir),
		Bootstrap: true, InMemory: false, Peers: peers,
		HeartbeatDur: 20 * time.Millisecond, ElectionMin: 60 * time.Millisecond, ElectionMax: 120 * time.Millisecond,
	}
	n1, err := consensus.NewRaftNode(cfg1, fsm1, nil, slog.Default())
	if err != nil {
		t.Fatalf("failed to create node 1: %v", err)
	}

	store2 := state.NewStore(storage.NewMemoryAuditStore())
	fsm2 := consensus.NewFSM(store2, slog.Default())
	cfg2 := &consensus.NodeConfig{
		NodeID: "c-2", BindAddr: a2, DataDir: fmt.Sprintf("%s/c2", tempDir),
		Bootstrap: false, InMemory: false, Peers: peers,
		HeartbeatDur: 20 * time.Millisecond, ElectionMin: 60 * time.Millisecond, ElectionMax: 120 * time.Millisecond,
	}
	n2, err := consensus.NewRaftNode(cfg2, fsm2, nil, slog.Default())
	if err != nil {
		t.Fatalf("failed to create node 2: %v", err)
	}

	store3 := state.NewStore(storage.NewMemoryAuditStore())
	fsm3 := consensus.NewFSM(store3, slog.Default())
	cfg3 := &consensus.NodeConfig{
		NodeID: "c-3", BindAddr: a3, DataDir: fmt.Sprintf("%s/c3", tempDir),
		Bootstrap: false, InMemory: false, Peers: peers,
		HeartbeatDur: 20 * time.Millisecond, ElectionMin: 60 * time.Millisecond, ElectionMax: 120 * time.Millisecond,
	}
	n3, err := consensus.NewRaftNode(cfg3, fsm3, nil, slog.Default())
	if err != nil {
		t.Fatalf("failed to create node 3: %v", err)
	}

	nodes := []*consensus.RaftNode{n1, n2, n3}
	defer func() {
		for _, n := range nodes {
			if n != nil {
				_ = n.Shutdown()
			}
		}
	}()

	// Wait for initial election
	for i := 0; i < 50; i++ {
		if n1.IsLeader() || n2.IsLeader() || n3.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Identify leader and apply task
	var leader *consensus.RaftNode
	for _, n := range nodes {
		if n.IsLeader() {
			leader = n
			break
		}
	}
	if leader == nil {
		t.Fatalf("no leader elected in 3-node cluster")
	}

	cmd := &consensus.Command{
		Op: consensus.OpSubmitTask,
		Task: &domain.Task{
			ID:       "task-persistent-cluster",
			TenantID: "tenant-tcp",
			Priority: 77,
		},
		Timestamp: time.Now(),
	}
	if _, err := leader.Apply(cmd, 3*time.Second); err != nil {
		t.Fatalf("failed to apply command to cluster leader: %v", err)
	}

	// Take snapshot on node 1 so FileSnapshotStore is written
	_ = n1.Raft().Snapshot().Error()

	// Terminate node 1
	_ = n1.Shutdown()
	nodes[0] = nil

	// Wait for new leader election between n2 and n3
	var newLeader *consensus.RaftNode
	for i := 0; i < 50; i++ {
		if n2.IsLeader() {
			newLeader = n2
			break
		}
		if n3.IsLeader() {
			newLeader = n3
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatalf("quorum failed to elect new leader after node 1 termination")
	}

	// Commit new task while node 1 is dead
	cmdDuringDowntime := &consensus.Command{
		Op: consensus.OpSubmitTask,
		Task: &domain.Task{
			ID:       "task-during-c1-downtime",
			TenantID: "tenant-tcp",
			Priority: 88,
		},
		Timestamp: time.Now(),
	}
	if _, err := newLeader.Apply(cmdDuringDowntime, 3*time.Second); err != nil {
		t.Fatalf("failed applying command to new leader during downtime: %v", err)
	}

	// Restart node 1 using the SAME DataDir with fresh store/FSM
	freshStore1 := state.NewStore(storage.NewMemoryAuditStore())
	freshFSM1 := consensus.NewFSM(freshStore1, slog.Default())
	restartCfg1 := &consensus.NodeConfig{
		NodeID:       "c-1",
		BindAddr:     a1,
		DataDir:      fmt.Sprintf("%s/c1", tempDir),
		Bootstrap:    false, // Rejoin existing cluster
		InMemory:     false, // Recover BoltDB & snapshots
		Peers:        peers,
		HeartbeatDur: 20 * time.Millisecond,
		ElectionMin:  60 * time.Millisecond,
		ElectionMax:  120 * time.Millisecond,
	}
	restartedN1, err := consensus.NewRaftNode(restartCfg1, freshFSM1, nil, slog.Default())
	if err != nil {
		t.Fatalf("failed to restart node 1 from persistent storage: %v", err)
	}
	nodes[0] = restartedN1

	// Wait for restarted node 1 to recover prior state, rejoin, and catch up to current leader
	var caughtUp bool
	for attempt := 0; attempt < 50; attempt++ {
		tPrev, err1 := freshStore1.GetTask("task-persistent-cluster")
		tNew, err2 := freshStore1.GetTask("task-during-c1-downtime")
		if err1 == nil && tPrev != nil && tPrev.Priority == 77 &&
			err2 == nil && tNew != nil && tNew.Priority == 88 {
			caughtUp = true
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !caughtUp {
		t.Fatalf("restarted node failed to recover persistent state or catch up to current leader")
	}
}

// 15. Restarted follower catches up
func TestRaft_RestartedFollowerCatchesUp(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	followerIdx := (leaderIdx + 1) % 3

	// Disconnect follower
	h.transports[followerIdx].DisconnectAll()

	// Leader commits task while follower is isolated
	taskID := "task-follower-catchup"
	_, err := h.clients[leaderIdx].SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "t-catchup", Priority: 50},
	})
	if err != nil {
		t.Fatalf("leader failed to commit while follower disconnected: %v", err)
	}

	// Reconnect follower
	for j := 0; j < 3; j++ {
		if j != followerIdx {
			h.transports[followerIdx].Connect(h.transports[j].LocalAddr(), h.transports[j])
			h.transports[j].Connect(h.transports[followerIdx].LocalAddr(), h.transports[followerIdx])
		}
	}

	// Wait for follower to catch up
	time.Sleep(250 * time.Millisecond)
	tCheck, err := h.stores[followerIdx].GetTask(taskID)
	if err != nil || tCheck.State != domain.StateReady {
		t.Fatalf("reconnected follower failed to catch up to committed task")
	}
}

// 16. Fencing tuple (WorkerID, SessionID, LeaseEpoch) survives failover
func TestRaft_FencingTupleSurvivesFailover(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	taskID := "task-fencing-survives"
	_, _ = h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "t-fence", Priority: 10},
	})
	time.Sleep(50 * time.Millisecond)

	_, _ = h.wClients[leaderIdx].RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId: "w-fence", SessionId: "sess-valid-1", MaxSlots: 1,
	})
	pull, _ := h.wClients[leaderIdx].PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId: "w-fence", SessionId: "sess-valid-1", AvailableSlots: 1,
	})
	epoch := pull.GetAssignment().GetLeaseEpoch()

	_, _ = h.wClients[leaderIdx].AckTaskRunning(ctx, &schedulerv1.AckTaskRunningRequest{
		WorkerId: "w-fence", SessionId: "sess-valid-1", TaskId: taskID, LeaseEpoch: epoch,
	})

	// Kill initial leader
	_ = h.nodes[leaderIdx].Shutdown()
	h.servers[leaderIdx].Stop()
	h.transports[leaderIdx].DisconnectAll()
	h.nodes[leaderIdx] = nil

	newLeaderIdx := -1
	for i := 0; i < 80; i++ {
		newLeaderIdx = h.findLeaderIndex()
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Attempt completion with stale session on new leader
	_, staleErr := h.wClients[newLeaderIdx].ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:   "w-fence",
		SessionId:  "sess-stale-imposter", // STALE SESSION
		TaskId:     taskID,
		LeaseEpoch: epoch,
		Result:     []byte("bad"),
	})
	if staleErr == nil {
		t.Fatalf("expected stale session to be rejected on new leader")
	}
	if st, _ := status.FromError(staleErr); st.Code() != codes.FailedPrecondition {
		t.Fatalf("expected FailedPrecondition for stale session, got %s", st.Code())
	}
}

// 17. Duplicate command / idempotency safety
func TestRaft_DuplicateCommandIdempotency(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Submit task with idempotency key
	resp1, err1 := h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:             "task-orig-1",
			TenantId:       "tenant-idem",
			Priority:       10,
			IdempotencyKey: "unique-client-token-99",
		},
	})
	if err1 != nil {
		t.Fatalf("first submit failed: %v", err1)
	}

	// Re-submit with same idempotency key but different task ID
	resp2, err2 := h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{
			Id:             "task-dup-2",
			TenantId:       "tenant-idem",
			Priority:       10,
			IdempotencyKey: "unique-client-token-99",
		},
	})
	if err2 != nil {
		t.Fatalf("second submit failed: %v", err2)
	}

	if resp1.GetTaskId() != resp2.GetTaskId() {
		t.Fatalf("idempotency violated: expected task ID %s, got %s", resp1.GetTaskId(), resp2.GetTaskId())
	}
}

// 18. Snapshot & restore correctness across fresh FSM
func TestRaft_SnapshotRestoreCorrectness(t *testing.T) {
	store1 := state.NewStore(storage.NewMemoryAuditStore())
	fsm1 := consensus.NewFSM(store1, slog.Default())

	// Apply several mutations
	_, _ = store1.ApplySubmitTask(&domain.Task{ID: "t-1", TenantID: "t-a"}, 1, 1)
	_, _ = store1.ApplyMarkTaskReady("t-1", 2, 1)
	_, _ = store1.ApplySubmitTask(&domain.Task{ID: "t-2", TenantID: "t-a"}, 3, 1)
	_, _ = store1.ApplyRegisterWorker(&domain.Worker{ID: "w-1", MaxSlots: 5}, 4, 1)

	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("failed to take snapshot: %v", err)
	}

	buf := new(mockBufferSink)
	if err := snap.Persist(buf); err != nil {
		t.Fatalf("failed to persist snapshot: %v", err)
	}

	store2 := state.NewStore(storage.NewMemoryAuditStore())
	fsm2 := consensus.NewFSM(store2, slog.Default())

	if err := fsm2.Restore(buf); err != nil {
		t.Fatalf("failed to restore snapshot: %v", err)
	}

	t1, err := store2.GetTask("t-1")
	if err != nil || t1.State != domain.StateReady {
		t.Fatalf("t-1 mismatch after restore")
	}
	t2, err := store2.GetTask("t-2")
	if err != nil || t2.State != domain.StatePending {
		t.Fatalf("t-2 mismatch after restore: %v", t2)
	}
	w1, err := store2.GetWorker("w-1")
	if err != nil || w1.MaxSlots != 5 {
		t.Fatalf("w-1 mismatch after restore")
	}
}

// 19. Concurrent worker pulls never double-assign a committed lease
func TestRaft_ConcurrentWorkerPullsNoDoubleAssign(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Submit exactly ONE task
	taskID := "task-single-lease-race"
	_, _ = h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "t-race", Priority: 100},
	})
	time.Sleep(50 * time.Millisecond)

	// Register 4 workers
	for i := 1; i <= 4; i++ {
		_, _ = h.wClients[leaderIdx].RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
			WorkerId:  fmt.Sprintf("worker-%d", i),
			SessionId: fmt.Sprintf("session-%d", i),
			MaxSlots:  2,
		})
	}

	// Concurrently attempt to pull the single task across all 4 workers
	var wg sync.WaitGroup
	var pullSuccessCount atomic.Int32
	var assignedWorker string
	var assignedWorkerMu sync.Mutex

	for i := 1; i <= 4; i++ {
		wg.Add(1)
		go func(workerNum int) {
			defer wg.Done()
			wID := fmt.Sprintf("worker-%d", workerNum)
			sID := fmt.Sprintf("session-%d", workerNum)

			resp, err := h.wClients[leaderIdx].PullTask(ctx, &schedulerv1.PullTaskRequest{
				WorkerId:       wID,
				SessionId:      sID,
				AvailableSlots: 2,
			})
			if err == nil && resp.GetHasTask() {
				pullSuccessCount.Add(1)
				assignedWorkerMu.Lock()
				assignedWorker = wID
				assignedWorkerMu.Unlock()
			}
		}(i)
	}

	wg.Wait()

	// Invariant verification: EXACTLY ONE worker received the lease!
	if pullSuccessCount.Load() != 1 {
		t.Fatalf("INVARIANT VIOLATION: expected exactly 1 worker to get lease, got %d", pullSuccessCount.Load())
	}

	tCheck, _ := h.stores[leaderIdx].GetTask(taskID)
	if tCheck.State != domain.StateLeased {
		t.Fatalf("expected state LEASED, got %s", tCheck.State)
	}
	if tCheck.AssignedWorkerID != assignedWorker {
		t.Fatalf("task assigned worker %s != pull winner %s", tCheck.AssignedWorkerID, assignedWorker)
	}
}

// 20. Stale leader cannot mutate after losing quorum
func TestRaft_StaleLeaderCannotMutateAfterQuorumLoss(t *testing.T) {
	h := newClusterHarness(t, 3)
	defer h.close()

	leaderIdx := h.waitLeader(3 * time.Second)
	ctx := context.Background()

	// Isolate the leader completely from both followers
	h.transports[leaderIdx].DisconnectAll()
	for i := 0; i < 3; i++ {
		if i != leaderIdx {
			h.transports[i].Disconnect(h.transports[leaderIdx].LocalAddr())
		}
	}

	// 1. Mutation attempt on isolated leader must fail
	_, err := h.clients[leaderIdx].SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: "task-stale-leader", TenantId: "t-stale", Priority: 50},
	})
	if err == nil {
		t.Fatalf("expected mutation to fail on quorum-less leader")
	}

	// 2. Linearizable read attempt on isolated leader must fail
	err = h.servers[leaderIdx].verifyLinearizableRead()
	if err == nil {
		t.Fatalf("expected linearizable read verification to fail on quorum-less leader")
	}
}

type mockBufferSink struct {
	data []byte
	pos  int
}

func (m *mockBufferSink) Write(p []byte) (n int, err error) {
	m.data = append(m.data, p...)
	return len(p), nil
}

func (m *mockBufferSink) Close() error {
	return nil
}

func (m *mockBufferSink) ID() string {
	return "mock-id"
}

func (m *mockBufferSink) Cancel() error {
	return nil
}

func (m *mockBufferSink) Read(p []byte) (n int, err error) {
	if m.pos >= len(m.data) {
		return 0, io.EOF
	}
	n = copy(p, m.data[m.pos:])
	m.pos += n
	return n, nil
}
