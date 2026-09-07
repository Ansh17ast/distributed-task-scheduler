package consensus

import (
	"bytes"
	"io"
	"log/slog"
	"testing"
	"time"

	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/storage"
	"github.com/hashicorp/raft"
)

func TestCommand_EncodeDecode(t *testing.T) {
	cmd := &Command{
		Op:         OpSubmitTask,
		TaskID:     "task-123",
		WorkerID:   "worker-alpha",
		SessionID:  "session-uuid-1",
		LeaseEpoch: 42,
		Task: &domain.Task{
			ID:       "task-123",
			TenantID: "tenant-analytics",
			Priority: 50,
		},
		Timestamp: time.Unix(1700000000, 0),
	}

	data, err := cmd.Encode()
	if err != nil {
		t.Fatalf("failed to encode command: %v", err)
	}

	decoded, err := DecodeCommand(data)
	if err != nil {
		t.Fatalf("failed to decode command: %v", err)
	}

	if decoded.Op != cmd.Op {
		t.Fatalf("expected op %s, got %s", cmd.Op, decoded.Op)
	}
	if decoded.TaskID != cmd.TaskID {
		t.Fatalf("expected task id %s, got %s", cmd.TaskID, decoded.TaskID)
	}
	if decoded.LeaseEpoch != cmd.LeaseEpoch {
		t.Fatalf("expected epoch %d, got %d", cmd.LeaseEpoch, decoded.LeaseEpoch)
	}
	if decoded.Task.TenantID != "tenant-analytics" {
		t.Fatalf("expected tenant-analytics, got %s", decoded.Task.TenantID)
	}
}

func TestFSM_SnapshotAndRestore(t *testing.T) {
	store1 := state.NewStore(storage.NewMemoryAuditStore())
	fsm1 := NewFSM(store1, slog.Default())

	// 1. Populate store1 with tasks, DAGs, workers, tenant quotas
	store1.SetTenantQuota(&domain.TenantQuota{TenantID: "tenant-finance", MaxConcurrency: 5, MaxQueueDepth: 100})

	_, err := store1.ApplyRegisterWorker(&domain.Worker{
		ID:        "worker-alpha",
		SessionID: "sess-1",
		MaxSlots:  4,
		Status:    domain.WorkerStatusHealthy,
	}, 1, 1)
	if err != nil {
		t.Fatalf("failed to register worker: %v", err)
	}

	_, err = store1.ApplySubmitTask(&domain.Task{
		ID:             "task-snap-1",
		TenantID:       "tenant-finance",
		Priority:       90,
		State:          domain.StatePending,
		IdempotencyKey: "idem-key-1",
	}, 2, 1)
	if err != nil {
		t.Fatalf("failed to submit task: %v", err)
	}

	_, err = store1.ApplyMarkTaskReady("task-snap-1", 3, 1)
	if err != nil {
		t.Fatalf("failed to mark ready: %v", err)
	}

	_, err = store1.ApplyGrantTaskLease("task-snap-1", "worker-alpha", "sess-1", 5*time.Second, 4, 1)
	if err != nil {
		t.Fatalf("failed to grant lease: %v", err)
	}

	// 2. Take snapshot from fsm1
	snap, err := fsm1.Snapshot()
	if err != nil {
		t.Fatalf("failed to take snapshot: %v", err)
	}

	buf := new(bytes.Buffer)
	mockSink := &mockSnapshotSink{buf: buf}
	if err := snap.Persist(mockSink); err != nil {
		t.Fatalf("failed to persist snapshot: %v", err)
	}

	// 3. Create fresh store2 + fsm2 and restore from snapshot
	store2 := state.NewStore(storage.NewMemoryAuditStore())
	fsm2 := NewFSM(store2, slog.Default())

	readCloser := io.NopCloser(bytes.NewReader(buf.Bytes()))
	if err := fsm2.Restore(readCloser); err != nil {
		t.Fatalf("failed to restore snapshot into fsm2: %v", err)
	}

	// 4. Verify authoritative state in store2 matches store1
	tRestored, err := store2.GetTask("task-snap-1")
	if err != nil {
		t.Fatalf("expected task-snap-1 in store2: %v", err)
	}
	if tRestored.State != domain.StateLeased {
		t.Fatalf("expected state LEASED, got %s", tRestored.State)
	}
	if tRestored.AssignedWorkerID != "worker-alpha" {
		t.Fatalf("expected worker-alpha, got %s", tRestored.AssignedWorkerID)
	}
	if tRestored.AssignedSessionID != "sess-1" {
		t.Fatalf("expected session sess-1, got %s", tRestored.AssignedSessionID)
	}
	if tRestored.LeaseEpoch != 1 {
		t.Fatalf("expected lease epoch 1, got %d", tRestored.LeaseEpoch)
	}

	wRestored, err := store2.GetWorker("worker-alpha")
	if err != nil {
		t.Fatalf("expected worker-alpha in store2: %v", err)
	}
	if wRestored.AvailableSlots != 3 { // 4 - 1 leased = 3
		t.Fatalf("expected 3 available slots, got %d", wRestored.AvailableSlots)
	}

	// 5. Continue processing commands on restored fsm2
	ackCmd := &Command{
		Op:         OpAckTaskRunning,
		TaskID:     "task-snap-1",
		WorkerID:   "worker-alpha",
		SessionID:  "sess-1",
		LeaseEpoch: 1,
		Timestamp:  time.Now(),
	}
	ackData, _ := ackCmd.Encode()
	res := fsm2.Apply(&raft.Log{Index: 5, Term: 1, Data: ackData})
	if applyErr, ok := res.(error); ok {
		t.Fatalf("failed to apply command on restored FSM: %v", applyErr)
	}

	tAfterAck, _ := store2.GetTask("task-snap-1")
	if tAfterAck.State != domain.StateRunning {
		t.Fatalf("expected RUNNING after ack, got %s", tAfterAck.State)
	}
}

func TestRaftNode_SingleNodeInMem(t *testing.T) {
	store := state.NewStore(storage.NewMemoryAuditStore())
	fsm := NewFSM(store, slog.Default())

	addr, trans := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:0"))
	cfg := &NodeConfig{
		NodeID:       "node-1",
		BindAddr:     string(addr),
		Bootstrap:    true,
		InMemory:     true,
		Peers:        []RaftPeer{{ID: "node-1", Address: string(addr)}},
		HeartbeatDur: 20 * time.Millisecond,
		ElectionMin:  50 * time.Millisecond,
		ElectionMax:  100 * time.Millisecond,
	}

	node, err := NewRaftNode(cfg, fsm, trans, slog.Default())
	if err != nil {
		t.Fatalf("failed to create RaftNode: %v", err)
	}
	defer node.Shutdown()

	// Wait for election
	leaderElected := false
	for i := 0; i < 40; i++ {
		if node.IsLeader() {
			leaderElected = true
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !leaderElected {
		t.Fatalf("node failed to become leader")
	}

	// Test linearizable read verification
	if err := node.VerifyLeader(); err != nil {
		t.Fatalf("VerifyLeader failed on elected leader: %v", err)
	}

	// Propose command via RaftNode.Apply
	cmd := &Command{
		Op: OpSubmitTask,
		Task: &domain.Task{
			ID:       "task-replicated-1",
			TenantID: "tenant-core",
			Priority: 100,
		},
		Timestamp: time.Now(),
	}

	resp, err := node.Apply(cmd, 2*time.Second)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	task, ok := resp.(*domain.Task)
	if !ok {
		t.Fatalf("expected *domain.Task response, got %T", resp)
	}
	if task.ID != "task-replicated-1" {
		t.Fatalf("expected task-replicated-1, got %s", task.ID)
	}

	// Verify committed state inside Store
	committed, err := store.GetTask("task-replicated-1")
	if err != nil {
		t.Fatalf("task not found in store: %v", err)
	}
	if committed.State != domain.StatePending {
		t.Fatalf("expected PENDING, got %s", committed.State)
	}
	if committed.RaftAppliedIndex == 0 {
		t.Fatalf("expected RaftAppliedIndex > 0, got %d", committed.RaftAppliedIndex)
	}
}

func TestRaftNode_PersistentBoltStoreRecovery(t *testing.T) {
	tempDir := t.TempDir()

	// Phase 1: Boot Node with BoltDB persistence
	store1 := state.NewStore(storage.NewMemoryAuditStore())
	fsm1 := NewFSM(store1, slog.Default())

	addr1, trans1 := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:10001"))
	cfg1 := &NodeConfig{
		NodeID:       "node-persist",
		BindAddr:     string(addr1),
		DataDir:      tempDir,
		Bootstrap:    true,
		InMemory:     false, // Use persistent BoltDB & FileSnapshotStore!
		Peers:        []RaftPeer{{ID: "node-persist", Address: string(addr1)}},
		HeartbeatDur: 20 * time.Millisecond,
		ElectionMin:  50 * time.Millisecond,
		ElectionMax:  100 * time.Millisecond,
	}

	node1, err := NewRaftNode(cfg1, fsm1, trans1, slog.Default())
	if err != nil {
		t.Fatalf("failed to start node1 with persistent storage: %v", err)
	}

	// Wait for election
	for i := 0; i < 40; i++ {
		if node1.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !node1.IsLeader() {
		t.Fatalf("node1 failed to become leader")
	}

	// Submit task
	cmd := &Command{
		Op: OpSubmitTask,
		Task: &domain.Task{
			ID:       "task-persistent-saved",
			TenantID: "tenant-db",
			Priority: 42,
		},
		Timestamp: time.Now(),
	}
	if _, err := node1.Apply(cmd, 2*time.Second); err != nil {
		t.Fatalf("failed to apply command to persistent node: %v", err)
	}

	// Take Raft snapshot so persistent state is stored to FileSnapshotStore
	snapFuture := node1.Raft().Snapshot()
	if err := snapFuture.Error(); err != nil {
		t.Fatalf("failed to take raft snapshot: %v", err)
	}

	// Apply a second task AFTER the snapshot to verify log replay from BoltDB
	cmd2 := &Command{
		Op: OpSubmitTask,
		Task: &domain.Task{
			ID:       "task-after-snapshot",
			TenantID: "tenant-db",
			Priority: 99,
		},
		Timestamp: time.Now(),
	}
	if _, err := node1.Apply(cmd2, 2*time.Second); err != nil {
		t.Fatalf("failed to apply command after snapshot: %v", err)
	}

	// Gracefully shutdown node 1 and close BoltDB
	if err := node1.Shutdown(); err != nil {
		t.Fatalf("failed to shutdown node1: %v", err)
	}

	// Phase 2: Restart node using the SAME data directory with a FRESH Store
	store2 := state.NewStore(storage.NewMemoryAuditStore())
	fsm2 := NewFSM(store2, slog.Default())

	addr2, trans2 := raft.NewInmemTransport(raft.ServerAddress("127.0.0.1:10001"))
	cfg2 := &NodeConfig{
		NodeID:       "node-persist",
		BindAddr:     string(addr2),
		DataDir:      tempDir, // SAME PATH
		Bootstrap:    false,   // Recovers existing state
		InMemory:     false,
		Peers:        []RaftPeer{{ID: "node-persist", Address: string(addr2)}},
		HeartbeatDur: 20 * time.Millisecond,
		ElectionMin:  50 * time.Millisecond,
		ElectionMax:  100 * time.Millisecond,
	}

	node2, err := NewRaftNode(cfg2, fsm2, trans2, slog.Default())
	if err != nil {
		t.Fatalf("failed to restart node2 on same data dir: %v", err)
	}
	defer node2.Shutdown()

	// Wait for node 2 to replay log from BoltDB and re-elect
	for i := 0; i < 40; i++ {
		if node2.IsLeader() {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if !node2.IsLeader() {
		t.Fatalf("restarted node failed to re-acquire leadership")
	}

	// Apply a barrier to ensure all prior log entries from BoltDB are applied
	_ = node2.Raft().Barrier(2 * time.Second).Error()

	// Verify task-persistent-saved was replayed from snapshot into the fresh Store!
	recovered1, err := store2.GetTask("task-persistent-saved")
	if err != nil {
		t.Fatalf("task-persistent-saved was NOT recovered from snapshot: %v", err)
	}
	if recovered1.Priority != 42 {
		t.Fatalf("expected priority 42, got %d", recovered1.Priority)
	}

	// Verify task-after-snapshot was replayed from log entries in BoltDB!
	var recovered2 *domain.Task
	for attempt := 0; attempt < 20; attempt++ {
		recovered2, err = store2.GetTask("task-after-snapshot")
		if err == nil && recovered2 != nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if recovered2 == nil {
		t.Fatalf("task-after-snapshot was NOT recovered from BoltDB log replay: %v", err)
	}
	if recovered2.Priority != 99 {
		t.Fatalf("expected priority 99, got %d", recovered2.Priority)
	}
}

type mockSnapshotSink struct {
	buf *bytes.Buffer
}

func (m *mockSnapshotSink) Write(p []byte) (n int, err error) {
	return m.buf.Write(p)
}

func (m *mockSnapshotSink) Close() error {
	return nil
}

func (m *mockSnapshotSink) ID() string {
	return "mock-snap-id"
}

func (m *mockSnapshotSink) Cancel() error {
	return nil
}

// TestFSM_NoConsensusRecursion verifies that FSM.Apply directly mutates the store
// via Apply... methods and never attempts to recursively call consensus or re-enter Raft.
func TestFSM_NoConsensusRecursion(t *testing.T) {
	st := state.NewStore(storage.NewMemoryAuditStore())
	fsm := NewFSM(st, slog.Default())

	// Test each opcode via FSM.Apply to ensure direct local mutation
	ops := []*Command{
		{
			Op: OpRegisterWorker,
			Worker: &domain.Worker{
				ID:        "w-rec-1",
				SessionID: "s-1",
				MaxSlots:  2,
			},
			Timestamp: time.Now(),
		},
		{
			Op: OpSubmitTask,
			Task: &domain.Task{
				ID:       "t-rec-1",
				TenantID: "tenant-rec",
				Priority: 10,
			},
			Timestamp: time.Now(),
		},
		{
			Op:        OpMarkTaskReady,
			TaskID:    "t-rec-1",
			Timestamp: time.Now(),
		},
		{
			Op:            OpGrantTaskLease,
			TaskID:        "t-rec-1",
			WorkerID:      "w-rec-1",
			SessionID:     "s-1",
			LeaseDuration: 5 * time.Second,
			Timestamp:     time.Now(),
		},
		{
			Op:         OpAckTaskRunning,
			TaskID:     "t-rec-1",
			WorkerID:   "w-rec-1",
			SessionID:  "s-1",
			LeaseEpoch: 1,
			Timestamp:  time.Now(),
		},
		{
			Op:         OpCompleteTask,
			TaskID:     "t-rec-1",
			WorkerID:   "w-rec-1",
			SessionID:  "s-1",
			LeaseEpoch: 1,
			Result:     []byte("ok"),
			Timestamp:  time.Now(),
		},
	}

	for i, cmd := range ops {
		data, err := cmd.Encode()
		if err != nil {
			t.Fatalf("failed encoding cmd %d: %v", i, err)
		}
		res := fsm.Apply(&raft.Log{Index: uint64(i + 1), Term: 1, Data: data})
		if applyErr, ok := res.(error); ok {
			t.Fatalf("FSM.Apply failed on op %s: %v", cmd.Op, applyErr)
		}
	}

	// Verify local state reached StateSucceeded without any consensus recursion
	finalTask, err := st.GetTask("t-rec-1")
	if err != nil {
		t.Fatalf("expected task in store: %v", err)
	}
	if finalTask.State != domain.StateSucceeded {
		t.Fatalf("expected state SUCCEEDED, got %s", finalTask.State)
	}
}
