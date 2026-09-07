package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
)

// TestChaos_SnapshotAndHeavyLogChurnRecovery tests generating heavy Raft state, forcing snapshot, killing follower, applying more commits, and restarting follower.
func TestChaos_SnapshotAndHeavyLogChurnRecovery(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-SNAP-CHURN", 5001)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	leaderIdx := h.WaitLeader(3 * time.Second)
	if leaderIdx < 0 {
		t.Fatalf("leader election failed")
	}

	followerIdx := (leaderIdx + 1) % 3

	// 1. Generate heavy state (100 tasks + tenant quotas)
	allTaskIDs := make([]string, 0, 150)
	for i := 0; i < 100; i++ {
		tID := fmt.Sprintf("task-churn-%03d", i)
		allTaskIDs = append(allTaskIDs, tID)
		_, err := h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{Id: tID, TenantId: "tenant-churn", Priority: int32(i % 100)},
		})
		if err != nil {
			t.Fatalf("failed submitting task %d: %v", i, err)
		}
	}

	// 2. Take Raft Snapshot on all nodes
	for i := 0; i < 3; i++ {
		if h.Node(i) != nil {
			_ = h.Node(i).Raft().Snapshot().Error()
		}
	}

	// 3. Kill follower
	h.KillNode(followerIdx)

	// 4. Submit 50 more tasks to leader while follower is offline
	for i := 100; i < 150; i++ {
		tID := fmt.Sprintf("task-churn-%03d", i)
		allTaskIDs = append(allTaskIDs, tID)
		_, err := h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{Id: tID, TenantId: "tenant-churn", Priority: int32(i % 100)},
		})
		if err != nil {
			t.Fatalf("failed submitting task %d during follower downtime: %v", i, err)
		}
	}

	// 5. Restart follower
	err = h.RestartNode(followerIdx)
	if err != nil {
		t.Fatalf("failed restarting follower: %v", err)
	}

	// Wait for follower to catch up from snapshot + bolt log stream
	time.Sleep(500 * time.Millisecond)

	// Verify all 150 tasks exist in follower store with identical state
	for _, id := range allTaskIDs {
		ft, err := h.Store(followerIdx).GetTask(id)
		if err != nil || ft == nil {
			t.Fatalf("follower failed to recover task %s after snapshot/log catchup: %v", id, err)
		}
	}

	h.Invariants.CheckStateConvergenceLiveness(h.Store(leaderIdx), []*state.Store{h.Store(followerIdx)}, allTaskIDs)
	if !h.Invariants.AllPassed() {
		t.Fatalf("invariants failed after snapshot/log churn: %+v", h.Invariants.Results())
	}
}

// TestChaos_SplitBrainAdversarialIsolation tests that an isolated node attempting old-term writes is rejected and discarded upon healing.
func TestChaos_SplitBrainAdversarialIsolation(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-SPLIT-BRAIN", 5002)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	leaderIdx := h.WaitLeader(3 * time.Second)
	oldLeaderID := h.Node(leaderIdx).Config().NodeID
	oldTerm := h.Node(leaderIdx).CurrentTerm()

	// Form partition: Node isolated
	h.Injector.IsolateNode(leaderIdx)

	// Isolated node attempts 5 unauthorized writes
	for i := 0; i < 5; i++ {
		badCmd := &consensus.Command{
			Op: consensus.OpSubmitTask,
			Task: &domain.Task{
				ID:       fmt.Sprintf("task-unauthorized-%d", i),
				TenantID: "tenant-evil",
				Priority: 10,
			},
			Timestamp: time.Now(),
		}
		_, err := h.Node(leaderIdx).Apply(badCmd, 200*time.Millisecond)
		if err == nil {
			t.Fatalf("isolated node succeeded in committing unauthorized mutation!")
		}
	}

	// Remaining 2 nodes elect new leader
	newLeaderIdx := -1
	for attempt := 0; attempt < 50; attempt++ {
		for i := 0; i < 3; i++ {
			if i != leaderIdx && h.Node(i) != nil && h.Node(i).IsLeader() {
				newLeaderIdx = i
				break
			}
		}
		if newLeaderIdx >= 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeaderIdx < 0 {
		t.Fatalf("majority failed to elect new leader")
	}

	// Commit valid task on new leader
	validTaskID := "task-legitimate-majority"
	_, err = h.Client(newLeaderIdx).SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: validTaskID, TenantId: "tenant-good", Priority: 90},
	})
	if err != nil {
		t.Fatalf("new leader failed to commit legitimate task: %v", err)
	}

	// Heal partition
	h.Injector.Heal()
	time.Sleep(400 * time.Millisecond)

	// Verify old leader stepped down
	if h.Node(leaderIdx).IsLeader() && h.Node(leaderIdx).CurrentTerm() == oldTerm {
		t.Fatalf("old leader %s did not step down after partition healed", oldLeaderID)
	}

	// Verify none of the unauthorized tasks exist anywhere
	for i := 0; i < 5; i++ {
		badID := fmt.Sprintf("task-unauthorized-%d", i)
		for nodeI := 0; nodeI < 3; nodeI++ {
			_, errBad := h.Store(nodeI).GetTask(badID)
			if errBad == nil {
				t.Fatalf("unauthorized task %s was found committed in node %d store!", badID, nodeI+1)
			}
		}
	}

	// Legitimate task exists on all nodes
	for nodeI := 0; nodeI < 3; nodeI++ {
		goodTask, errGood := h.Store(nodeI).GetTask(validTaskID)
		if errGood != nil || goodTask == nil {
			t.Fatalf("node %d missing legitimate task after healing: %v", nodeI+1, errGood)
		}
	}
}
