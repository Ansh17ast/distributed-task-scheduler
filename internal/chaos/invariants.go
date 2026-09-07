package chaos

import (
	"fmt"

	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
)

// InvariantType classifies invariant guarantees into Safety vs Liveness.
type InvariantType string

const (
	InvariantSafety   InvariantType = "SAFETY"
	InvariantLiveness InvariantType = "LIVENESS"
)

// InvariantResult holds validation details for a single invariant check.
type InvariantResult struct {
	ID          string        `json:"id"`
	Type        InvariantType `json:"type"`
	Description string        `json:"description"`
	Passed      bool          `json:"passed"`
	ErrorDetail string        `json:"error_detail,omitempty"`
}

// InvariantChecker verifies formal distributed invariants across cluster state.
type InvariantChecker struct {
	results []InvariantResult
}

// NewInvariantChecker initializes a new invariant validator.
func NewInvariantChecker() *InvariantChecker {
	return &InvariantChecker{
		results: make([]InvariantResult, 0, 16),
	}
}

// Record records an invariant verification outcome.
func (ic *InvariantChecker) Record(id string, invType InvariantType, desc string, passed bool, errDetail string) InvariantResult {
	res := InvariantResult{
		ID:          id,
		Type:        invType,
		Description: desc,
		Passed:      passed,
		ErrorDetail: errDetail,
	}
	ic.results = append(ic.results, res)
	return res
}

// Results returns all verified invariant records.
func (ic *InvariantChecker) Results() []InvariantResult {
	return ic.results
}

// AllPassed returns true if all checked invariants passed.
func (ic *InvariantChecker) AllPassed() bool {
	for _, r := range ic.results {
		if !r.Passed {
			return false
		}
	}
	return true
}

// CheckSingleLeaderPerTerm (INV-CHAOS-01, Safety) verifies at most one leader exists for a given term.
func (ic *InvariantChecker) CheckSingleLeaderPerTerm(leadersByTerm map[uint64][]string) bool {
	for term, leaders := range leadersByTerm {
		if len(leaders) > 1 {
			ic.Record("INV-CHAOS-01", InvariantSafety,
				"At most one committed leader per term",
				false,
				fmt.Sprintf("multiple leaders observed in term %d: %v", term, leaders))
			return false
		}
	}
	ic.Record("INV-CHAOS-01", InvariantSafety, "At most one committed leader per term", true, "")
	return true
}

// CheckZeroCommittedLoss (INV-CHAOS-02, Safety) verifies that all previously committed tasks exist in current store.
func (ic *InvariantChecker) CheckZeroCommittedLoss(st *state.Store, expectedTaskIDs []string) bool {
	for _, id := range expectedTaskIDs {
		t, err := st.GetTask(id)
		if err != nil || t == nil {
			ic.Record("INV-CHAOS-02", InvariantSafety,
				"Committed task state is never silently lost",
				false,
				fmt.Sprintf("committed task %s missing from store: %v", id, err))
			return false
		}
	}
	ic.Record("INV-CHAOS-02", InvariantSafety, "Committed task state is never silently lost", true, "")
	return true
}

// CheckMinorityQuarantine (INV-CHAOS-03, Safety) verifies minority partition failed to commit a mutation.
func (ic *InvariantChecker) CheckMinorityQuarantine(applyErr error) bool {
	if applyErr == nil {
		ic.Record("INV-CHAOS-03", InvariantSafety,
			"Minority partition cannot commit scheduler mutations",
			false,
			"minority node successfully committed a mutation without quorum")
		return false
	}
	ic.Record("INV-CHAOS-03", InvariantSafety,
		"Minority partition cannot commit scheduler mutations",
		true, "")
	return true
}

// CheckZombieFencing (INV-CHAOS-04, Safety) verifies a stale epoch/session was rejected and authoritative state preserved.
func (ic *InvariantChecker) CheckZombieFencing(st *state.Store, taskID string, expectedWinnerWorkerID string, expectedLeaseEpoch uint64, rejectionErr error) bool {
	if rejectionErr == nil {
		ic.Record("INV-CHAOS-04", InvariantSafety,
			"Stale lease reports never mutate current authoritative state",
			false,
			"stale worker report was accepted by coordinator")
		return false
	}
	t, err := st.GetTask(taskID)
	if err != nil || t == nil {
		ic.Record("INV-CHAOS-04", InvariantSafety,
			"Stale lease reports never mutate current authoritative state",
			false,
			fmt.Sprintf("task %s not found: %v", taskID, err))
		return false
	}
	if t.AssignedWorkerID != expectedWinnerWorkerID || t.LeaseEpoch != expectedLeaseEpoch {
		ic.Record("INV-CHAOS-04", InvariantSafety,
			"Stale lease reports never mutate current authoritative state",
			false,
			fmt.Sprintf("authoritative state corrupted: worker=%s (expected %s), epoch=%d (expected %d)",
				t.AssignedWorkerID, expectedWinnerWorkerID, t.LeaseEpoch, expectedLeaseEpoch))
		return false
	}
	ic.Record("INV-CHAOS-04", InvariantSafety,
		"Stale lease reports never mutate current authoritative state",
		true, "")
	return true
}

// CheckSlotAccounting (INV-CHAOS-07, Safety) verifies slot invariants for all active workers: 0 <= AvailableSlots <= MaxSlots.
func (ic *InvariantChecker) CheckSlotAccounting(st *state.Store) bool {
	workers := st.GetActiveWorkers()
	for _, w := range workers {
		if w.AvailableSlots < 0 || w.AvailableSlots > w.MaxSlots {
			ic.Record("INV-CHAOS-07", InvariantSafety,
				"Worker slot capacity accounting remains valid (0 <= Available <= Max)",
				false,
				fmt.Sprintf("worker %s invalid slots: available=%d, max=%d", w.ID, w.AvailableSlots, w.MaxSlots))
			return false
		}
	}
	ic.Record("INV-CHAOS-07", InvariantSafety,
		"Worker slot capacity accounting remains valid (0 <= Available <= Max)",
		true, "")
	return true
}

// CheckDAGTopologyIntegrity (INV-CHAOS-08, Safety) verifies child tasks did not start before parent finished.
func (ic *InvariantChecker) CheckDAGTopologyIntegrity(st *state.Store, parentTaskID, childTaskID string) bool {
	parent, pErr := st.GetTask(parentTaskID)
	child, cErr := st.GetTask(childTaskID)
	if pErr != nil || cErr != nil {
		ic.Record("INV-CHAOS-08", InvariantSafety,
			"DAG dependency ordering remains valid after failover",
			false,
			fmt.Sprintf("error retrieving DAG tasks: parentErr=%v, childErr=%v", pErr, cErr))
		return false
	}
	if parent.State != domain.StateSucceeded &&
		(child.State == domain.StateRunning || child.State == domain.StateSucceeded) {
		ic.Record("INV-CHAOS-08", InvariantSafety,
			"DAG dependency ordering remains valid after failover",
			false,
			fmt.Sprintf("child %s is %s while parent %s is %s", child.ID, child.State, parent.ID, parent.State))
		return false
	}
	ic.Record("INV-CHAOS-08", InvariantSafety,
		"DAG dependency ordering remains valid after failover",
		true, "")
	return true
}

// CheckLeaderElectionLiveness (INV-CHAOS-L01, Liveness) verifies leader was elected within duration.
func (ic *InvariantChecker) CheckLeaderElectionLiveness(elected bool, elapsedStr string) bool {
	if !elected {
		ic.Record("INV-CHAOS-L01", InvariantLiveness,
			"New leader eventually elected after recoverable failure",
			false, "no leader elected within timeout")
		return false
	}
	ic.Record("INV-CHAOS-L01", InvariantLiveness,
		fmt.Sprintf("New leader eventually elected after recoverable failure (took %s)", elapsedStr),
		true, "")
	return true
}

// CheckStateConvergenceLiveness (INV-CHAOS-L02, Liveness) verifies all store replicas match leader state.
func (ic *InvariantChecker) CheckStateConvergenceLiveness(leaderStore *state.Store, followerStores []*state.Store, taskIDs []string) bool {
	for _, id := range taskIDs {
		lt, err := leaderStore.GetTask(id)
		if err != nil {
			ic.Record("INV-CHAOS-L02", InvariantLiveness,
				"Recovered nodes eventually converge to leader FSM state",
				false, fmt.Sprintf("task %s missing on leader: %v", id, err))
			return false
		}
		for i, fs := range followerStores {
			ft, fErr := fs.GetTask(id)
			if fErr != nil || ft == nil {
				ic.Record("INV-CHAOS-L02", InvariantLiveness,
					"Recovered nodes eventually converge to leader FSM state",
					false, fmt.Sprintf("follower %d missing task %s: %v", i+1, id, fErr))
				return false
			}
			if ft.State != lt.State || ft.LeaseEpoch != lt.LeaseEpoch {
				ic.Record("INV-CHAOS-L02", InvariantLiveness,
					"Recovered nodes eventually converge to leader FSM state",
					false, fmt.Sprintf("follower %d task %s state mismatch: got (%s, epoch %d), expected (%s, epoch %d)",
						i+1, id, ft.State, ft.LeaseEpoch, lt.State, lt.LeaseEpoch))
				return false
			}
		}
	}
	ic.Record("INV-CHAOS-L02", InvariantLiveness,
		"Recovered nodes eventually converge to leader FSM state",
		true, "")
	return true
}
