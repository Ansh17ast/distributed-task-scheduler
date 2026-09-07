package consensus

import (
	"fmt"
	"io"
	"log/slog"

	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/state"
	"github.com/hashicorp/raft"
)

// FSM implements hashicorp/raft.FSM backed by state.Store.
//
// HARD ARCHITECTURAL INVARIANTS:
// 1. DETERMINISM: FSM.Apply() is 100% deterministic across all nodes.
//   - No wall-clock timestamps are created locally; timestamps arrive in Command.
//   - No random jitter or numbers are generated locally.
//   - No goroutines, timers, or network calls are triggered during Apply().
//     2. NO CONSENSUS RE-ENTRY: FSM.Apply() mutates state.Store directly via Apply...
//     methods. It NEVER calls Raft.Apply() or consensus methods recursively.
type FSM struct {
	store  *state.Store
	logger *slog.Logger
}

// NewFSM creates a new deterministic Raft FSM.
func NewFSM(store *state.Store, logger *slog.Logger) *FSM {
	if logger == nil {
		logger = slog.Default()
	}
	return &FSM{
		store:  store,
		logger: logger,
	}
}

// Apply applies a committed Raft log entry to the deterministic state machine.
func (f *FSM) Apply(l *raft.Log) interface{} {
	cmd, err := DecodeCommand(l.Data)
	if err != nil {
		f.logger.Error("failed to decode command in FSM.Apply",
			"index", l.Index, "term", l.Term, "err", err)
		return err
	}

	raftIndex := l.Index
	raftTerm := l.Term

	switch cmd.Op {
	case OpSubmitTask:
		return f.applySubmitTask(cmd, raftIndex, raftTerm)

	case OpSubmitDAG:
		return f.applySubmitDAG(cmd, raftIndex, raftTerm)

	case OpMarkTaskReady:
		return f.applyMarkTaskReady(cmd, raftIndex, raftTerm)

	case OpGrantTaskLease:
		return f.applyGrantTaskLease(cmd, raftIndex, raftTerm)

	case OpAckTaskRunning:
		return f.applyAckTaskRunning(cmd, raftIndex, raftTerm)

	case OpCompleteTask:
		return f.applyCompleteTask(cmd, raftIndex, raftTerm)

	case OpFailTask:
		return f.applyFailTask(cmd, raftIndex, raftTerm)

	case OpReclaimTask:
		return f.applyReclaimTask(cmd, raftIndex, raftTerm)

	case OpCancelTask:
		return f.applyCancelTask(cmd, raftIndex, raftTerm)

	case OpRegisterWorker:
		return f.applyRegisterWorker(cmd, raftIndex, raftTerm)

	case OpExpireWorker:
		return f.applyExpireWorker(cmd, raftIndex, raftTerm)

	case OpSetTenantQuota:
		return f.applySetTenantQuota(cmd, raftIndex, raftTerm)

	default:
		err := fmt.Errorf("unknown command opcode: %s", cmd.Op)
		f.logger.Error("FSM.Apply error", "op", cmd.Op, "err", err)
		return err
	}
}

func (f *FSM) applySubmitTask(cmd *Command, index, term uint64) interface{} {
	if cmd.Task == nil {
		return fmt.Errorf("missing task in OpSubmitTask")
	}
	t, err := f.store.ApplySubmitTask(cmd.Task, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applySubmitDAG(cmd *Command, index, term uint64) interface{} {
	if cmd.DAG == nil {
		return fmt.Errorf("missing dag in OpSubmitDAG")
	}
	tasks := cmd.Tasks
	if len(tasks) == 0 && cmd.Task != nil {
		tasks = []*domain.Task{cmd.Task}
	}
	return f.store.ApplySubmitDAG(cmd.DAG, tasks, index, term)
}

func (f *FSM) applyMarkTaskReady(cmd *Command, index, term uint64) interface{} {
	t, err := f.store.ApplyMarkTaskReady(cmd.TaskID, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applyGrantTaskLease(cmd *Command, index, term uint64) interface{} {
	t, err := f.store.ApplyGrantTaskLease(cmd.TaskID, cmd.WorkerID, cmd.SessionID, cmd.LeaseDuration, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applyAckTaskRunning(cmd *Command, index, term uint64) interface{} {
	t, err := f.store.ApplyAckTaskRunning(cmd.TaskID, cmd.WorkerID, cmd.SessionID, cmd.LeaseEpoch, cmd.Timestamp, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applyCompleteTask(cmd *Command, index, term uint64) interface{} {
	t, err := f.store.ApplyCompleteTask(cmd.TaskID, cmd.WorkerID, cmd.SessionID, cmd.LeaseEpoch, cmd.Result, cmd.Timestamp, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applyFailTask(cmd *Command, index, term uint64) interface{} {
	t, err := f.store.ApplyFailTask(cmd.TaskID, cmd.WorkerID, cmd.SessionID, cmd.LeaseEpoch, cmd.ErrorMessage, cmd.NextState, cmd.Timestamp, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applyReclaimTask(cmd *Command, index, term uint64) interface{} {
	t, err := f.store.ApplyReclaimTask(cmd.TaskID, cmd.NextState, cmd.Reason, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applyCancelTask(cmd *Command, index, term uint64) interface{} {
	t, err := f.store.ApplyCancelTask(cmd.TaskID, cmd.Reason, index, term)
	if err != nil {
		return err
	}
	return t
}

func (f *FSM) applyRegisterWorker(cmd *Command, index, term uint64) interface{} {
	if cmd.Worker == nil {
		return fmt.Errorf("missing worker in OpRegisterWorker")
	}
	w, err := f.store.ApplyRegisterWorker(cmd.Worker, index, term)
	if err != nil {
		return err
	}
	return w
}

func (f *FSM) applyExpireWorker(cmd *Command, index, term uint64) interface{} {
	return f.store.ApplyExpireWorker(cmd.WorkerID, index, term)
}

func (f *FSM) applySetTenantQuota(cmd *Command, index, term uint64) interface{} {
	if cmd.TenantQuota == nil {
		return fmt.Errorf("missing tenant quota in OpSetTenantQuota")
	}
	return f.store.ApplySetTenantQuota(cmd.TenantQuota, index, term)
}

// Snapshot returns a point-in-time snapshot of the state machine.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	data, err := f.store.ExportSnapshot()
	if err != nil {
		return nil, fmt.Errorf("failed to export store snapshot: %w", err)
	}
	return &fsmSnapshot{data: data}, nil
}

// Restore restores the state machine from a snapshot stream.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("failed to read snapshot payload: %w", err)
	}
	if err := f.store.ImportSnapshot(data); err != nil {
		return fmt.Errorf("failed to import store snapshot: %w", err)
	}
	f.logger.Info("FSM restored successfully from snapshot", "bytes", len(data))
	return nil
}

// fsmSnapshot handles persisting snapshot bytes to Raft's sink.
type fsmSnapshot struct {
	data []byte
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := sink.Write(s.data); err != nil {
		_ = sink.Cancel()
		return fmt.Errorf("failed to write snapshot data to sink: %w", err)
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {
	s.data = nil
}
