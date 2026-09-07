package chaos

import (
	"context"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
)

// TestChaos_Partition_Case1_C1Isolated tests isolating C1 from C2+C3.
func TestChaos_Partition_Case1_C1Isolated(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-PART-CASE1", 2001)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	leaderIdx := h.WaitLeader(3 * time.Second)
	if leaderIdx < 0 {
		t.Fatalf("initial election failed")
	}

	// Partition C1 (idx 0) away from C2 (idx 1) and C3 (idx 2)
	h.Injector.Partition([]int{0}, []int{1, 2})

	// Wait for leader in majority partition {1, 2}
	time.Sleep(300 * time.Millisecond)
	majorityLeader := -1
	for i := 1; i <= 2; i++ {
		if h.Node(i).IsLeader() {
			majorityLeader = i
			break
		}
	}
	if majorityLeader < 0 {
		t.Fatalf("majority partition {C2, C3} failed to maintain/elect leader")
	}

	// Submit task on majority partition
	taskID := "task-part-case1"
	_, err = h.Client(majorityLeader).SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-part", Priority: 50},
	})
	if err != nil {
		t.Fatalf("majority partition failed to commit: %v", err)
	}

	// Heal partition
	h.Injector.Heal()
	time.Sleep(300 * time.Millisecond)

	// Verify C1 converges
	c1Task, err := h.Store(0).GetTask(taskID)
	if err != nil || c1Task == nil {
		t.Fatalf("C1 did not converge after healing: %v", err)
	}
	h.Invariants.CheckStateConvergenceLiveness(h.Store(majorityLeader), []*state.Store{h.Store(0)}, []string{taskID})
}

// TestChaos_Partition_Case2_C3Isolated tests isolating C3 from C1+C2.
func TestChaos_Partition_Case2_C3Isolated(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-PART-CASE2", 2002)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	leaderIdx := h.WaitLeader(3 * time.Second)
	if leaderIdx < 0 {
		t.Fatalf("initial election failed")
	}

	// Isolate C3 (idx 2)
	h.Injector.IsolateNode(2)
	time.Sleep(200 * time.Millisecond)

	// Majority partition {0, 1} should have or elect leader
	majLeader := -1
	for i := 0; i <= 1; i++ {
		if h.Node(i).IsLeader() {
			majLeader = i
			break
		}
	}
	if majLeader < 0 {
		t.Fatalf("majority partition {C1, C2} failed to have leader")
	}

	taskID := "task-part-case2"
	_, err = h.Client(majLeader).SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-part", Priority: 70},
	})
	if err != nil {
		t.Fatalf("majority failed to commit task: %v", err)
	}

	// Heal and check convergence
	h.Injector.Heal()
	time.Sleep(300 * time.Millisecond)

	c3Task, err := h.Store(2).GetTask(taskID)
	if err != nil || c3Task == nil {
		t.Fatalf("C3 failed to catch up after heal: %v", err)
	}
}

// TestChaos_Partition_Case3_LeaderIsolatedMinority tests minority leader inability to commit.
func TestChaos_Partition_Case3_LeaderIsolatedMinority(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-PART-CASE3", 2003)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	leaderIdx := h.WaitLeader(3 * time.Second)
	if leaderIdx < 0 {
		t.Fatalf("initial election failed")
	}

	oldLeaderTerm := h.Node(leaderIdx).CurrentTerm()
	oldLeaderID := h.Node(leaderIdx).Config().NodeID

	// Form group: minority = {leaderIdx}, majority = {remaining 2 nodes}
	majority := make([]int, 0, 2)
	for i := 0; i < 3; i++ {
		if i != leaderIdx {
			majority = append(majority, i)
		}
	}

	h.Injector.Partition([]int{leaderIdx}, majority)
	time.Sleep(200 * time.Millisecond)

	// 1. Attempt mutation on isolated minority leader -> must FAIL / timeout
	staleCmd := &consensus.Command{
		Op: consensus.OpSubmitTask,
		Task: &domain.Task{
			ID:       "task-stale-leader-attempt",
			TenantID: "tenant-part",
			Priority: 10,
		},
		Timestamp: time.Now(),
	}
	_, applyErr := h.Node(leaderIdx).Apply(staleCmd, 300*time.Millisecond)
	h.Invariants.CheckMinorityQuarantine(applyErr)

	// 2. Majority partition elects new leader
	newLeaderIdx := -1
	for attempt := 0; attempt < 50; attempt++ {
		for _, m := range majority {
			if h.Node(m).IsLeader() {
				newLeaderIdx = m
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

	// 3. Commit mutation on majority leader
	validTaskID := "task-committed-by-new-leader"
	_, err = h.Client(newLeaderIdx).SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: validTaskID, TenantId: "tenant-part", Priority: 99},
	})
	if err != nil {
		t.Fatalf("new leader failed to commit: %v", err)
	}

	// 4. Heal partition
	h.Injector.Heal()
	time.Sleep(400 * time.Millisecond)

	// 5. Old leader must step down and catch up
	if h.Node(leaderIdx).IsLeader() && h.Node(leaderIdx).CurrentTerm() == oldLeaderTerm {
		t.Fatalf("stale leader %s failed to step down after healing", oldLeaderID)
	}

	// Invariant: task-stale-leader-attempt must NOT exist in store; validTaskID MUST exist
	_, errStale := h.Store(leaderIdx).GetTask("task-stale-leader-attempt")
	if errStale == nil {
		t.Fatalf("stale uncommitted task erroneously committed into store!")
	}

	validTask, errValid := h.Store(leaderIdx).GetTask(validTaskID)
	if errValid != nil || validTask == nil {
		t.Fatalf("old leader failed to catch up to validTask after healing: %v", errValid)
	}

	h.Invariants.CheckZeroCommittedLoss(h.Store(leaderIdx), []string{validTaskID})
}

// TestChaos_Partition_Case4_AsymmetricAndCase5_Healing tests asymmetric links and recovery.
func TestChaos_Partition_Case4_AsymmetricAndCase5_Healing(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-PART-ASYMMETRIC", 2004)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	leaderIdx := h.WaitLeader(3 * time.Second)
	f1 := (leaderIdx + 1) % 3
	f2 := (leaderIdx + 2) % 3

	// Cut directed link leader -> f2
	h.Injector.CutDirectedLink(leaderIdx, f2)

	// Leader can still communicate with f1 -> quorum 2/3 intact
	taskID := "task-asymmetric-commit"
	_, err = h.Client(leaderIdx).SubmitTask(context.Background(), &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-part", Priority: 50},
	})
	if err != nil {
		t.Fatalf("leader failed to commit with quorum across leader+f1: %v", err)
	}

	// Heal full mesh
	h.Injector.Heal()
	time.Sleep(300 * time.Millisecond)

	h.Invariants.CheckStateConvergenceLiveness(h.Store(leaderIdx), []*state.Store{h.Store(f1), h.Store(f2)}, []string{taskID})
	if !h.Invariants.AllPassed() {
		t.Fatalf("invariants failed: %+v", h.Invariants.Results())
	}
}
