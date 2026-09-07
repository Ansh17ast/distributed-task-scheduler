package chaos

import (
	"context"
	"fmt"
	"testing"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
)

// TestChaos_LeaderKillAndReplacement tests single and repeated leader kills.
func TestChaos_LeaderKillAndReplacement(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-COORD-KILL", 1001)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	leaderIdx := h.WaitLeader(3 * time.Second)
	if leaderIdx < 0 {
		t.Fatalf("initial leader election failed")
	}

	// Submit initial task
	task1 := "task-coord-1"
	_, err = h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: task1, TenantId: "tenant-chaos", Priority: 50},
	})
	if err != nil {
		t.Fatalf("failed submitting initial task: %v", err)
	}

	// Kill initial leader
	oldLeaderID := h.Node(leaderIdx).Config().NodeID
	h.KillNode(leaderIdx)

	// Wait for new leader election on remaining 2 nodes
	newLeaderIdx := -1
	start := time.Now()
	for time.Since(start) < 4*time.Second {
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
		t.Fatalf("new leader was not elected after killing %s", oldLeaderID)
	}

	// Invariant checks
	h.Invariants.CheckLeaderElectionLiveness(true, time.Since(start).String())
	h.Invariants.CheckZeroCommittedLoss(h.Store(newLeaderIdx), []string{task1})

	// Submit task on new leader
	task2 := "task-coord-2"
	_, err = h.Client(newLeaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: task2, TenantId: "tenant-chaos", Priority: 80},
	})
	if err != nil {
		t.Fatalf("failed submitting task to new leader: %v", err)
	}

	if !h.Invariants.AllPassed() {
		t.Fatalf("invariants failed: %+v", h.Invariants.Results())
	}
}

// TestChaos_CyclicLeaderKills (10x iteration) tests stability under repeated leader terminations.
func TestChaos_CyclicLeaderKills(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-CYCLIC-KILL", 1002)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	submittedTasks := make([]string, 0, 10)

	for iter := 1; iter <= 5; iter++ {
		leaderIdx := h.WaitLeader(4 * time.Second)
		if leaderIdx < 0 {
			t.Fatalf("iter %d: leader election failed", iter)
		}

		taskID := fmt.Sprintf("task-cyclic-%d", iter)
		_, err := h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
			Task: &schedulerv1.TaskSpec{Id: taskID, TenantId: "tenant-cyclic", Priority: int32(iter * 10)},
		})
		if err != nil {
			t.Fatalf("iter %d: failed submitting task: %v", iter, err)
		}
		submittedTasks = append(submittedTasks, taskID)

		// Kill current leader
		h.KillNode(leaderIdx)

		// Restart a previously killed node to maintain quorum
		for other := 0; other < 3; other++ {
			if other != leaderIdx && h.Node(other) == nil {
				_ = h.RestartNode(other)
				break
			}
		}

		time.Sleep(100 * time.Millisecond)

		// Restart the killed leader as well
		_ = h.RestartNode(leaderIdx)
		time.Sleep(100 * time.Millisecond)
	}

	// Final verification
	finalLeaderIdx := h.WaitLeader(4 * time.Second)
	if finalLeaderIdx < 0 {
		t.Fatalf("final leader election failed")
	}

	h.Invariants.CheckZeroCommittedLoss(h.Store(finalLeaderIdx), submittedTasks)
	if !h.Invariants.AllPassed() {
		t.Fatalf("invariants failed after cyclic kills: %+v", h.Invariants.Results())
	}
}

// TestChaos_FollowerKillAndRejoin tests killing follower without affecting leader progress.
func TestChaos_FollowerKillAndRejoin(t *testing.T) {
	cfg := DefaultHarnessConfig("EXP-FOLLOWER-KILL", 1003)
	h, err := NewChaosHarness(cfg)
	if err != nil {
		t.Fatalf("failed creating harness: %v", err)
	}
	defer h.Close()

	ctx := context.Background()
	leaderIdx := h.WaitLeader(3 * time.Second)
	followerIdx := (leaderIdx + 1) % 3

	// Kill follower
	h.KillNode(followerIdx)

	// Leader must continue committing
	task1 := "task-during-follower-death"
	_, err = h.Client(leaderIdx).SubmitTask(ctx, &schedulerv1.SubmitTaskRequest{
		Task: &schedulerv1.TaskSpec{Id: task1, TenantId: "tenant-fol", Priority: 60},
	})
	if err != nil {
		t.Fatalf("leader failed to commit while follower was dead: %v", err)
	}

	// Restart follower
	err = h.RestartNode(followerIdx)
	if err != nil {
		t.Fatalf("failed restarting follower: %v", err)
	}

	// Wait for follower to catch up
	var followerTask *domain.Task
	for attempt := 0; attempt < 40; attempt++ {
		followerTask, err = h.Store(followerIdx).GetTask(task1)
		if err == nil && followerTask != nil && followerTask.State == domain.StateReady {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}

	if followerTask == nil || followerTask.State != domain.StateReady {
		t.Fatalf("restarted follower did not catch up: task=%+v, err=%v", followerTask, err)
	}

	h.Invariants.CheckStateConvergenceLiveness(h.Store(leaderIdx), []*state.Store{h.Store(followerIdx)}, []string{task1})
}
