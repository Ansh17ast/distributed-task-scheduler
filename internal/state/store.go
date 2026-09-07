package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/statemachine"
	"distributed-scheduler/internal/storage"
)

// Store defines the authoritative state store for the single coordinator.
// All state mutations pass through the statemachine.TransitionEngine.
type Store struct {
	mu                  sync.RWMutex
	tasks               map[string]*domain.Task
	dags                map[string]*domain.DAG
	tasksByDAG          map[string][]string
	workers             map[string]*domain.Worker
	workerRuntime       map[string]*domain.WorkerRuntimeState
	tenants             map[string]*domain.TenantQuota
	activeTenantRunning map[string]int32
	activeTenantQueued  map[string]int32
	idempotencyIndex    map[string]string // IdempotencyKey -> TaskID
	engine              *statemachine.TransitionEngine
	auditStore          storage.AuditStore
}

// NewStore creates an initialized Store.
func NewStore(auditStore storage.AuditStore) *Store {
	if auditStore == nil {
		auditStore = storage.NewMemoryAuditStore()
	}
	return &Store{
		tasks:               make(map[string]*domain.Task),
		dags:                make(map[string]*domain.DAG),
		tasksByDAG:          make(map[string][]string),
		workers:             make(map[string]*domain.Worker),
		workerRuntime:       make(map[string]*domain.WorkerRuntimeState),
		tenants:             make(map[string]*domain.TenantQuota),
		activeTenantRunning: make(map[string]int32),
		activeTenantQueued:  make(map[string]int32),
		idempotencyIndex:    make(map[string]string),
		engine:              statemachine.NewTransitionEngine(),
		auditStore:          auditStore,
	}
}

// SetTenantQuota sets or updates concurrency and queue limits for a tenant.
func (s *Store) SetTenantQuota(quota *domain.TenantQuota) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants[quota.TenantID] = quota
}

// GetTenantQuota returns tenant quota, or defaults if not configured.
func (s *Store) GetTenantQuota(tenantID string) *domain.TenantQuota {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if q, exists := s.tenants[tenantID]; exists {
		return q
	}
	return &domain.TenantQuota{
		TenantID:       tenantID,
		MaxConcurrency: 10,
		MaxQueueDepth:  1000,
	}
}

// SubmitTask admits a new task into PENDING, checking idempotency and tenant queue limits.
func (s *Store) SubmitTask(task *domain.Task) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Idempotency Key check
	if task.IdempotencyKey != "" {
		if existingID, exists := s.idempotencyIndex[task.IdempotencyKey]; exists {
			return s.tasks[existingID], nil
		}
	}

	// 2. Tenant Queue Backpressure check
	quota, exists := s.tenants[task.TenantID]
	if !exists {
		quota = &domain.TenantQuota{
			TenantID:       task.TenantID,
			MaxConcurrency: 10,
			MaxQueueDepth:  1000,
		}
		s.tenants[task.TenantID] = quota
	}

	queuedCount := s.activeTenantQueued[task.TenantID]
	if queuedCount >= quota.MaxQueueDepth {
		return nil, domain.ErrTenantQuotaExceeded
	}

	// 3. Initialize task
	task.State = domain.StatePending
	if task.CreatedAt.IsZero() {
		task.CreatedAt = time.Now()
	}
	s.tasks[task.ID] = task
	s.activeTenantQueued[task.TenantID]++
	if task.IdempotencyKey != "" {
		s.idempotencyIndex[task.IdempotencyKey] = task.ID
	}

	return task, nil
}

// MarkTaskReady transitions a task to READY.
func (s *Store) MarkTaskReady(taskID string) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	prevState := task.State
	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "MARK_READY",
		TargetState: domain.StateReady,
	})
	if err != nil {
		return nil, err
	}
	if prevState == domain.StateRetryWait || prevState == domain.StateRecovering || prevState == domain.StateBlocked {
		s.activeTenantQueued[task.TenantID]++
	}
	return task, nil
}

// RegisterWorker records or updates a worker with declared slot capacity.
func (s *Store) RegisterWorker(w *domain.Worker) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if w.MaxSlots <= 0 {
		return errors.New("worker max slots must be > 0")
	}

	existing, exists := s.workers[w.ID]
	if exists {
		// Worker reconnect
		existing.Address = w.Address
		existing.MaxSlots = w.MaxSlots
		existing.Status = domain.WorkerStatusHealthy
		existing.Metadata = w.Metadata

		// If this is a new process incarnation (SessionID changed)
		if w.SessionID != "" && existing.SessionID != w.SessionID {
			existing.SessionID = w.SessionID
			// Reset runtime capacity specifically associated with the new process incarnation
			existing.AvailableSlots = w.MaxSlots
		}

		s.workerRuntime[w.ID] = &domain.WorkerRuntimeState{
			WorkerID:                    w.ID,
			SessionID:                   existing.SessionID,
			LastLocalMonotonicHeartbeat: time.Now(),
		}
		return nil
	}

	w.AvailableSlots = w.MaxSlots
	w.Status = domain.WorkerStatusHealthy
	w.RegisteredAt = time.Now()
	s.workers[w.ID] = w
	s.workerRuntime[w.ID] = &domain.WorkerRuntimeState{
		WorkerID:                    w.ID,
		SessionID:                   w.SessionID,
		LastLocalMonotonicHeartbeat: time.Now(),
	}
	return nil
}

// UpdateWorkerHeartbeat updates a worker's local liveness timestamp.
// Enforces that heartbeats from a stale process incarnation cannot touch or revive worker state.
func (s *Store) UpdateWorkerHeartbeat(workerID string, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[workerID]
	if !exists {
		return domain.ErrWorkerNotFound
	}

	if sessionID != "" && w.SessionID != "" && w.SessionID != sessionID {
		return fmt.Errorf("%w: heartbeat session %s does not match active worker session %s",
			domain.ErrStaleWorkerSession, sessionID, w.SessionID)
	}

	w.Status = domain.WorkerStatusHealthy
	if rt, ok := s.workerRuntime[workerID]; ok {
		rt.LastLocalMonotonicHeartbeat = time.Now()
		if sessionID != "" {
			rt.SessionID = sessionID
		}
	} else {
		s.workerRuntime[workerID] = &domain.WorkerRuntimeState{
			WorkerID:                    workerID,
			SessionID:                   w.SessionID,
			LastLocalMonotonicHeartbeat: time.Now(),
		}
	}
	return nil
}

// GetWorkerLastHeartbeat returns the process-local monotonic heartbeat instant for a worker.
func (s *Store) GetWorkerLastHeartbeat(workerID string) (time.Time, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	rt, exists := s.workerRuntime[workerID]
	if !exists {
		return time.Time{}, false
	}
	return rt.LastLocalMonotonicHeartbeat, true
}

// GetWorker returns a copy of worker state.
func (s *Store) GetWorker(workerID string) (*domain.Worker, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	w, exists := s.workers[workerID]
	if !exists {
		return nil, domain.ErrWorkerNotFound
	}
	copied := *w
	return &copied, nil
}

// GetActiveWorkers returns all healthy workers.
func (s *Store) GetActiveWorkers() []*domain.Worker {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []*domain.Worker
	for _, w := range s.workers {
		if w.Status == domain.WorkerStatusHealthy {
			copied := *w
			list = append(list, &copied)
		}
	}
	return list
}

// GrantTaskLease assigns a READY task to a worker session, incrementing epoch and consuming a worker slot.
func (s *Store) GrantTaskLease(taskID, workerID, sessionID string, leaseDur time.Duration) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	worker, exists := s.workers[workerID]
	if !exists {
		return nil, domain.ErrWorkerNotFound
	}

	if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
		return nil, fmt.Errorf("%w: pull session %s does not match active worker session %s",
			domain.ErrStaleWorkerSession, sessionID, worker.SessionID)
	}

	if worker.AvailableSlots <= 0 {
		return nil, errors.New("worker has zero available slots")
	}

	// Verify tenant concurrency limit
	quota, exists := s.tenants[task.TenantID]
	if exists && quota.MaxConcurrency > 0 {
		running := s.activeTenantRunning[task.TenantID]
		if running >= quota.MaxConcurrency {
			return nil, domain.ErrTenantQuotaExceeded
		}
	}

	// Transition state machine: READY -> LEASED
	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "GRANT_LEASE",
		TargetState: domain.StateLeased,
		WorkerID:    workerID,
		SessionID:   sessionID,
	})
	if err != nil {
		return nil, err
	}

	task.LeaseDuration = leaseDur

	// Slot accounting: decrement available slots
	worker.AvailableSlots--
	if s.activeTenantQueued[task.TenantID] > 0 {
		s.activeTenantQueued[task.TenantID]--
	}
	s.activeTenantRunning[task.TenantID]++

	return task, nil
}

// MarkTaskRunning records worker acknowledgement that execution has started.
func (s *Store) MarkTaskRunning(taskID, workerID, sessionID string, epoch uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
			return fmt.Errorf("%w: worker %s has active session %s; ack from stale session %s rejected",
				domain.ErrStaleWorkerSession, workerID, worker.SessionID, sessionID)
		}
	}

	task, exists := s.tasks[taskID]
	if !exists {
		return domain.ErrTaskNotFound
	}

	return s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "ACK_RUNNING",
		TargetState: domain.StateRunning,
		WorkerID:    workerID,
		SessionID:   sessionID,
		LeaseEpoch:  epoch,
	})
}

// CompleteTask records task success and releases the worker slot and tenant concurrency.
func (s *Store) CompleteTask(taskID, workerID, sessionID string, epoch uint64, result []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
			return fmt.Errorf("%w: worker %s has active session %s; completion from stale session %s rejected",
				domain.ErrStaleWorkerSession, workerID, worker.SessionID, sessionID)
		}
	}

	task, exists := s.tasks[taskID]
	if !exists {
		return domain.ErrTaskNotFound
	}

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    workerID,
		SessionID:   sessionID,
		LeaseEpoch:  epoch,
		Result:      result,
	})
	if err != nil {
		return err
	}

	// Release worker slot if not already released
	if worker, exists := s.workers[workerID]; exists {
		if worker.AvailableSlots < worker.MaxSlots {
			worker.AvailableSlots++
		}
	}
	if s.activeTenantRunning[task.TenantID] > 0 {
		s.activeTenantRunning[task.TenantID]--
	}

	return nil
}

// FailTask records task failure.
// If retryable && attempt < maxRetries, transitions to RETRY_WAIT.
// Otherwise transitions to terminal FAILED. Releases worker slot and tenant concurrency.
func (s *Store) FailTask(taskID, workerID, sessionID string, epoch uint64, errMsg string, retryable bool) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
			return false, fmt.Errorf("%w: worker %s has active session %s; failure report from stale session %s rejected",
				domain.ErrStaleWorkerSession, workerID, worker.SessionID, sessionID)
		}
	}

	task, exists := s.tasks[taskID]
	if !exists {
		return false, domain.ErrTaskNotFound
	}

	willRetry := retryable && (task.AttemptCount < task.MaxRetries)
	targetState := domain.StateFailed
	cmdType := "FAIL_TASK"
	if willRetry {
		targetState = domain.StateRetryWait
		cmdType = "FAIL_RETRY"
	}

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType:  cmdType,
		TargetState:  targetState,
		WorkerID:     workerID,
		SessionID:    sessionID,
		LeaseEpoch:   epoch,
		ErrorMessage: errMsg,
	})
	if err != nil {
		return false, err
	}

	// Release worker slot
	if worker, exists := s.workers[workerID]; exists {
		if worker.AvailableSlots < worker.MaxSlots {
			worker.AvailableSlots++
		}
	}
	if s.activeTenantRunning[task.TenantID] > 0 {
		s.activeTenantRunning[task.TenantID]--
	}

	return willRetry, nil
}

// ReclaimTask reclaims an expired task from LEASED/RUNNING, releases worker slot, and resets state.
// Returns the resulting domain.State (domain.StateReady or domain.StateFailed) and any error.
func (s *Store) ReclaimTask(taskID string) (domain.State, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reclaimTaskLocked(taskID)
}

func (s *Store) reclaimTaskLocked(taskID string) (domain.State, error) {
	task, exists := s.tasks[taskID]
	if !exists {
		return "", domain.ErrTaskNotFound
	}

	oldWorkerID := task.AssignedWorkerID

	// Transition: LEASED/RUNNING -> LOST
	if err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "RECLAIM_TASK",
		TargetState: domain.StateLost,
	}); err != nil {
		return task.State, err
	}

	// LOST -> RECOVERING
	if err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		TargetState: domain.StateRecovering,
	}); err != nil {
		return task.State, err
	}

	// Release slot from old worker if alive
	if oldWorker, exists := s.workers[oldWorkerID]; exists {
		if oldWorker.AvailableSlots < oldWorker.MaxSlots {
			oldWorker.AvailableSlots++
		}
	}
	if s.activeTenantRunning[task.TenantID] > 0 {
		s.activeTenantRunning[task.TenantID]--
	}

	// Decide whether to re-enqueue as READY or mark FAILED
	if task.AttemptCount < task.MaxRetries {
		task.AttemptCount++
		s.activeTenantQueued[task.TenantID]++
		err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
			CommandType: "MARK_READY",
			TargetState: domain.StateReady,
		})
		return domain.StateReady, err
	}

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType:  "FAIL_TASK",
		TargetState:  domain.StateFailed,
		ErrorMessage: "worker lease expired; retries exhausted",
	})
	return domain.StateFailed, err
}

// CancelTask cancels an active or queued task.
func (s *Store) CancelTask(taskID, reason string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return domain.ErrTaskNotFound
	}

	wasActive := (task.State == domain.StateLeased || task.State == domain.StateRunning)
	wasQueued := (task.State == domain.StatePending || task.State == domain.StateReady)
	assignedWorker := task.AssignedWorkerID

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType:  "CANCEL_TASK",
		TargetState:  domain.StateCancelled,
		ErrorMessage: reason,
	})
	if err != nil {
		return err
	}

	if wasQueued && s.activeTenantQueued[task.TenantID] > 0 {
		s.activeTenantQueued[task.TenantID]--
	}

	if wasActive {
		if worker, exists := s.workers[assignedWorker]; exists {
			if worker.AvailableSlots < worker.MaxSlots {
				worker.AvailableSlots++
			}
		}
		if s.activeTenantRunning[task.TenantID] > 0 {
			s.activeTenantRunning[task.TenantID]--
		}
	}

	return nil
}

// GetTask returns a copy of the task state.
func (s *Store) GetTask(taskID string) (*domain.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}
	copied := *task
	return &copied, nil
}

// GetReadyTasks returns all tasks currently in READY state.
func (s *Store) GetReadyTasks() []*domain.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var ready []*domain.Task
	for _, t := range s.tasks {
		if t.State == domain.StateReady {
			copied := *t
			ready = append(ready, &copied)
		}
	}
	return ready
}

// GetTasksByState returns copies of all tasks matching the specified State.
func (s *Store) GetTasksByState(targetState domain.State) []*domain.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var matched []*domain.Task
	for _, t := range s.tasks {
		if t.State == targetState {
			copied := *t
			matched = append(matched, &copied)
		}
	}
	return matched
}

// GetTenantActiveRunning returns currently active running tasks for a tenant.
func (s *Store) GetTenantActiveRunning(tenantID string) int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.activeTenantRunning[tenantID]
}

// GetActiveTasksByWorker returns all tasks assigned to the worker in LEASED or RUNNING states.
func (s *Store) GetActiveTasksByWorker(workerID string) []*domain.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var list []*domain.Task
	for _, t := range s.tasks {
		if t.AssignedWorkerID == workerID && (t.State == domain.StateLeased || t.State == domain.StateRunning) {
			copied := *t
			list = append(list, &copied)
		}
	}
	return list
}

// MarkWorkerDead marks a worker as DEAD.
func (s *Store) MarkWorkerDead(workerID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[workerID]
	if !exists {
		return domain.ErrWorkerNotFound
	}
	w.Status = domain.WorkerStatusDead
	return nil
}

// SubmitDAG admits a complete validated DAG workflow into the state store.
// Tasks with 0 dependencies transition: PENDING -> READY.
// Tasks with dependencies transition: PENDING -> BLOCKED.
func (s *Store) SubmitDAG(dag *domain.DAG, tasks []*domain.Task) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.dags[dag.ID]; exists {
		return errors.New("dag already exists")
	}

	// Check tenant queue depth backpressure
	quota, exists := s.tenants[dag.TenantID]
	if !exists {
		quota = &domain.TenantQuota{
			TenantID:       dag.TenantID,
			MaxConcurrency: 10,
			MaxQueueDepth:  1000,
		}
		s.tenants[dag.TenantID] = quota
	}

	// Count how many tasks will be immediately queued as READY
	readyCount := 0
	for _, t := range tasks {
		if len(t.Dependencies) == 0 {
			readyCount++
		}
	}
	if s.activeTenantQueued[dag.TenantID]+int32(readyCount) > quota.MaxQueueDepth {
		return domain.ErrTenantQuotaExceeded
	}

	now := time.Now()
	if dag.CreatedAt.IsZero() {
		dag.CreatedAt = now
	}

	var taskIDs []string
	for _, t := range tasks {
		t.DAGID = dag.ID
		t.TenantID = dag.TenantID
		if t.CreatedAt.IsZero() {
			t.CreatedAt = now
		}
		t.State = domain.StatePending

		if len(t.Dependencies) == 0 {
			// Root task transitions: PENDING -> READY
			if err := s.engine.ApplyTransition(t, statemachine.TransitionRequest{
				CommandType: "MARK_READY",
				TargetState: domain.StateReady,
			}); err != nil {
				return err
			}
			s.activeTenantQueued[t.TenantID]++
		} else {
			// Dependent task transitions: PENDING -> BLOCKED
			if err := s.engine.ApplyTransition(t, statemachine.TransitionRequest{
				CommandType: "BLOCK_TASK",
				TargetState: domain.StateBlocked,
			}); err != nil {
				return err
			}
		}

		s.tasks[t.ID] = t
		taskIDs = append(taskIDs, t.ID)
		if t.IdempotencyKey != "" {
			s.idempotencyIndex[t.IdempotencyKey] = t.ID
		}
	}

	dag.TaskIDs = taskIDs
	s.dags[dag.ID] = dag
	s.tasksByDAG[dag.ID] = taskIDs

	return nil
}

// GetDAG returns a copy of the DAG definition along with copies of all its child tasks.
func (s *Store) GetDAG(dagID string) (*domain.DAG, []*domain.Task, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	dag, exists := s.dags[dagID]
	if !exists {
		return nil, nil, domain.ErrDAGNotFound
	}

	dagCopy := *dag
	var tasks []*domain.Task
	for _, tID := range s.tasksByDAG[dagID] {
		if t, ok := s.tasks[tID]; ok {
			tCopy := *t
			tasks = append(tasks, &tCopy)
		}
	}

	return &dagCopy, tasks, nil
}

// GetTasksByDAG returns copies of all tasks in a given DAG.
func (s *Store) GetTasksByDAG(dagID string) []*domain.Task {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var tasks []*domain.Task
	for _, tID := range s.tasksByDAG[dagID] {
		if t, ok := s.tasks[tID]; ok {
			tCopy := *t
			tasks = append(tasks, &tCopy)
		}
	}
	return tasks
}

// ============================================================================
// DETERMINISTIC REPLICATED FSM MUTATION METHODS
//
// All methods below are called EXCLUSIVELY by consensus.FSM.Apply() or when
// replicating state across the 3-node cluster.
// They are 100% DETERMINISTIC:
// - No wall-clock timestamps are created (use arguments from committed Command)
// - No random numbers or jitter calculations
// - No goroutines or timers scheduled
// - RaftAppliedIndex and RaftAppliedTerm are stamped onto mutated domain entities
// ============================================================================

// ApplySubmitTask deterministically admits a new task from a committed Raft command.
func (s *Store) ApplySubmitTask(task *domain.Task, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// 1. Idempotency Key check
	if task.IdempotencyKey != "" {
		if existingID, exists := s.idempotencyIndex[task.IdempotencyKey]; exists {
			return s.tasks[existingID], nil
		}
	}

	// 2. Tenant Queue Backpressure check
	quota, exists := s.tenants[task.TenantID]
	if !exists {
		quota = &domain.TenantQuota{
			TenantID:       task.TenantID,
			MaxConcurrency: 10,
			MaxQueueDepth:  1000,
		}
		s.tenants[task.TenantID] = quota
	}

	queuedCount := s.activeTenantQueued[task.TenantID]
	if queuedCount >= quota.MaxQueueDepth {
		return nil, domain.ErrTenantQuotaExceeded
	}

	// 3. Initialize task
	taskCopy := *task
	taskCopy.State = domain.StatePending
	taskCopy.RaftAppliedIndex = raftIndex
	taskCopy.RaftAppliedTerm = raftTerm
	s.tasks[taskCopy.ID] = &taskCopy
	s.activeTenantQueued[taskCopy.TenantID]++
	if taskCopy.IdempotencyKey != "" {
		s.idempotencyIndex[taskCopy.IdempotencyKey] = taskCopy.ID
	}

	return &taskCopy, nil
}

// ApplySubmitDAG deterministically admits a DAG and its tasks from a committed Raft command.
func (s *Store) ApplySubmitDAG(dag *domain.DAG, tasks []*domain.Task, raftIndex, raftTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.dags[dag.ID]; exists {
		return fmt.Errorf("dag %s already exists", dag.ID)
	}

	var taskIDs []string
	for _, t := range tasks {
		tCopy := *t
		tCopy.DAGID = dag.ID
		tCopy.RaftAppliedIndex = raftIndex
		tCopy.RaftAppliedTerm = raftTerm

		quota, exists := s.tenants[tCopy.TenantID]
		if !exists {
			quota = &domain.TenantQuota{
				TenantID:       tCopy.TenantID,
				MaxConcurrency: 10,
				MaxQueueDepth:  1000,
			}
			s.tenants[tCopy.TenantID] = quota
		}

		if len(tCopy.Dependencies) == 0 {
			tCopy.State = domain.StateReady
			s.activeTenantQueued[tCopy.TenantID]++
		} else {
			tCopy.State = domain.StateBlocked
		}

		s.tasks[tCopy.ID] = &tCopy
		taskIDs = append(taskIDs, tCopy.ID)
		if tCopy.IdempotencyKey != "" {
			s.idempotencyIndex[tCopy.IdempotencyKey] = tCopy.ID
		}
	}

	dagCopy := *dag
	dagCopy.TaskIDs = taskIDs
	s.dags[dag.ID] = &dagCopy
	s.tasksByDAG[dag.ID] = taskIDs

	return nil
}

// ApplyMarkTaskReady deterministically promotes a task to READY.
func (s *Store) ApplyMarkTaskReady(taskID string, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	prevState := task.State
	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "MARK_READY",
		TargetState: domain.StateReady,
	})
	if err != nil {
		return nil, err
	}

	task.RaftAppliedIndex = raftIndex
	task.RaftAppliedTerm = raftTerm

	if prevState == domain.StateBlocked {
		s.activeTenantQueued[task.TenantID]++
	}

	return task, nil
}

// ApplyGrantTaskLease deterministically leases a READY task to a worker session.
func (s *Store) ApplyGrantTaskLease(taskID, workerID, sessionID string, leaseDur time.Duration, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	worker, exists := s.workers[workerID]
	if !exists {
		return nil, domain.ErrWorkerNotFound
	}

	if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
		return nil, fmt.Errorf("%w: pull session %s does not match active worker session %s",
			domain.ErrStaleWorkerSession, sessionID, worker.SessionID)
	}

	if worker.AvailableSlots <= 0 {
		return nil, errors.New("worker has zero available slots")
	}

	// Verify tenant concurrency limit
	quota, exists := s.tenants[task.TenantID]
	if exists && quota.MaxConcurrency > 0 {
		running := s.activeTenantRunning[task.TenantID]
		if running >= quota.MaxConcurrency {
			return nil, domain.ErrTenantQuotaExceeded
		}
	}

	// Transition state machine: READY -> LEASED
	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "GRANT_LEASE",
		TargetState: domain.StateLeased,
		WorkerID:    workerID,
		SessionID:   sessionID,
	})
	if err != nil {
		return nil, err
	}

	task.LeaseDuration = leaseDur
	task.RaftAppliedIndex = raftIndex
	task.RaftAppliedTerm = raftTerm

	// Slot accounting: decrement available slots
	worker.AvailableSlots--
	if s.activeTenantQueued[task.TenantID] > 0 {
		s.activeTenantQueued[task.TenantID]--
	}
	s.activeTenantRunning[task.TenantID]++

	return task, nil
}

// ApplyAckTaskRunning deterministically transitions LEASED -> RUNNING.
func (s *Store) ApplyAckTaskRunning(taskID, workerID, sessionID string, epoch uint64, startedAt time.Time, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
			return nil, fmt.Errorf("%w: worker %s has active session %s; ack from stale session %s rejected",
				domain.ErrStaleWorkerSession, workerID, worker.SessionID, sessionID)
		}
	}

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "ACK_RUNNING",
		TargetState: domain.StateRunning,
		WorkerID:    workerID,
		SessionID:   sessionID,
		LeaseEpoch:  epoch,
	})
	if err != nil {
		return nil, err
	}

	task.StartedAt = &startedAt
	task.RaftAppliedIndex = raftIndex
	task.RaftAppliedTerm = raftTerm

	return task, nil
}

// ApplyCompleteTask deterministically records task completion and restores worker slots.
func (s *Store) ApplyCompleteTask(taskID, workerID, sessionID string, epoch uint64, result []byte, completedAt time.Time, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
			return nil, fmt.Errorf("%w: worker %s has active session %s; completion from stale session %s rejected",
				domain.ErrStaleWorkerSession, workerID, worker.SessionID, sessionID)
		}
	}

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "COMPLETE_TASK",
		TargetState: domain.StateSucceeded,
		WorkerID:    workerID,
		SessionID:   sessionID,
		LeaseEpoch:  epoch,
		Result:      result,
	})
	if err != nil {
		return nil, err
	}

	task.CompletedAt = &completedAt
	task.RaftAppliedIndex = raftIndex
	task.RaftAppliedTerm = raftTerm

	// Release worker slot if not already released
	if worker, exists := s.workers[workerID]; exists {
		if worker.AvailableSlots < worker.MaxSlots {
			worker.AvailableSlots++
		}
	}
	if s.activeTenantRunning[task.TenantID] > 0 {
		s.activeTenantRunning[task.TenantID]--
	}

	return task, nil
}

// ApplyFailTask deterministically fails a task and sets next state (FAILED or RETRY_WAIT).
func (s *Store) ApplyFailTask(taskID, workerID, sessionID string, epoch uint64, errMsg string, nextState domain.State, completedAt time.Time, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if worker, exists := s.workers[workerID]; exists {
		if sessionID != "" && worker.SessionID != "" && worker.SessionID != sessionID {
			return nil, fmt.Errorf("%w: worker %s has active session %s; failure report from stale session %s rejected",
				domain.ErrStaleWorkerSession, workerID, worker.SessionID, sessionID)
		}
	}

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	cmdType := "FAIL_TASK"
	if nextState == domain.StateRetryWait {
		cmdType = "FAIL_RETRY"
	}

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType:  cmdType,
		TargetState:  nextState,
		WorkerID:     workerID,
		SessionID:    sessionID,
		LeaseEpoch:   epoch,
		ErrorMessage: errMsg,
	})
	if err != nil {
		return nil, err
	}

	task.CompletedAt = &completedAt
	task.RaftAppliedIndex = raftIndex
	task.RaftAppliedTerm = raftTerm

	// Release worker slot
	if worker, exists := s.workers[workerID]; exists {
		if worker.AvailableSlots < worker.MaxSlots {
			worker.AvailableSlots++
		}
	}
	if s.activeTenantRunning[task.TenantID] > 0 {
		s.activeTenantRunning[task.TenantID]--
	}
	if nextState == domain.StateRetryWait {
		s.activeTenantQueued[task.TenantID]++
	}

	return task, nil
}

// ApplyReclaimTask deterministically reclaims an expired task and clears worker bindings.
func (s *Store) ApplyReclaimTask(taskID string, nextState domain.State, reason string, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	oldWorkerID := task.AssignedWorkerID
	wasRunning := (task.State == domain.StateRunning)

	// Step 1: LEASED/RUNNING -> LOST
	if err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType: "RECLAIM_TASK",
		TargetState: domain.StateLost,
	}); err != nil {
		return nil, err
	}

	// Step 2: LOST -> RECOVERING
	if err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		TargetState: domain.StateRecovering,
	}); err != nil {
		return nil, err
	}

	// Step 3: RECOVERING -> READY / FAILED / RETRY_WAIT
	recoverCmd := "MARK_READY"
	if nextState == domain.StateFailed {
		recoverCmd = "RECOVER_FAILED"
	} else if nextState == domain.StateRetryWait {
		recoverCmd = "RECOVER_RETRY"
	}

	if err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType:  recoverCmd,
		TargetState:  nextState,
		ErrorMessage: reason,
	}); err != nil {
		return nil, err
	}

	task.RaftAppliedIndex = raftIndex
	task.RaftAppliedTerm = raftTerm

	// Restore slot to previously assigned worker if valid
	if oldWorkerID != "" {
		if w, ok := s.workers[oldWorkerID]; ok {
			if w.AvailableSlots < w.MaxSlots {
				w.AvailableSlots++
			}
		}
	}

	if wasRunning && s.activeTenantRunning[task.TenantID] > 0 {
		s.activeTenantRunning[task.TenantID]--
	}
	if nextState == domain.StateReady || nextState == domain.StateRetryWait {
		task.AttemptCount++
		s.activeTenantQueued[task.TenantID]++
	}

	return task, nil
}

// ApplyCancelTask deterministically cancels a task.
func (s *Store) ApplyCancelTask(taskID, reason string, raftIndex, raftTerm uint64) (*domain.Task, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	task, exists := s.tasks[taskID]
	if !exists {
		return nil, domain.ErrTaskNotFound
	}

	prevState := task.State
	oldWorkerID := task.AssignedWorkerID

	err := s.engine.ApplyTransition(task, statemachine.TransitionRequest{
		CommandType:  "CANCEL_TASK",
		TargetState:  domain.StateCancelled,
		ErrorMessage: reason,
	})
	if err != nil {
		return nil, err
	}

	task.RaftAppliedIndex = raftIndex
	task.RaftAppliedTerm = raftTerm

	if prevState == domain.StateLeased || prevState == domain.StateRunning {
		if oldWorkerID != "" {
			if w, ok := s.workers[oldWorkerID]; ok {
				if w.AvailableSlots < w.MaxSlots {
					w.AvailableSlots++
				}
			}
		}
		if s.activeTenantRunning[task.TenantID] > 0 {
			s.activeTenantRunning[task.TenantID]--
		}
	} else if prevState == domain.StateReady || prevState == domain.StatePending || prevState == domain.StateRetryWait {
		if s.activeTenantQueued[task.TenantID] > 0 {
			s.activeTenantQueued[task.TenantID]--
		}
	}

	return task, nil
}

// ApplyRegisterWorker deterministically registers a worker or updates process incarnation.
func (s *Store) ApplyRegisterWorker(w *domain.Worker, raftIndex, raftTerm uint64) (*domain.Worker, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	existing, exists := s.workers[w.ID]
	if exists {
		if w.SessionID != "" && existing.SessionID != w.SessionID {
			existing.SessionID = w.SessionID
			existing.AvailableSlots = w.MaxSlots
		}
		existing.MaxSlots = w.MaxSlots
		existing.Address = w.Address
		existing.Metadata = w.Metadata
		existing.Status = domain.WorkerStatusHealthy
		copied := *existing
		return &copied, nil
	}

	workerCopy := *w
	workerCopy.AvailableSlots = w.MaxSlots
	workerCopy.Status = domain.WorkerStatusHealthy
	s.workers[w.ID] = &workerCopy
	copied := workerCopy
	return &copied, nil
}

// ApplyExpireWorker marks an expired worker dead.
func (s *Store) ApplyExpireWorker(workerID string, raftIndex, raftTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	w, exists := s.workers[workerID]
	if !exists {
		return domain.ErrWorkerNotFound
	}
	w.Status = domain.WorkerStatusDead
	w.AvailableSlots = 0
	return nil
}

// ApplySetTenantQuota sets tenant quota limits.
func (s *Store) ApplySetTenantQuota(quota *domain.TenantQuota, raftIndex, raftTerm uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	qCopy := *quota
	s.tenants[quota.TenantID] = &qCopy
	return nil
}

// ============================================================================
// SNAPSHOT & RESTORE METHODS FOR RAFT FSM
// ============================================================================

// StoreSnapshot is the serializable state of the store for Raft snapshots.
type StoreSnapshot struct {
	Tasks               map[string]*domain.Task        `json:"tasks"`
	DAGs                map[string]*domain.DAG         `json:"dags"`
	TasksByDAG          map[string][]string            `json:"tasks_by_dag"`
	Workers             map[string]*domain.Worker      `json:"workers"`
	Tenants             map[string]*domain.TenantQuota `json:"tenants"`
	ActiveTenantRunning map[string]int32               `json:"active_tenant_running"`
	ActiveTenantQueued  map[string]int32               `json:"active_tenant_queued"`
	IdempotencyIndex    map[string]string              `json:"idempotency_index"`
}

// ExportSnapshot creates a serialized point-in-time representation of all authoritative state.
func (s *Store) ExportSnapshot() ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	snap := StoreSnapshot{
		Tasks:               make(map[string]*domain.Task, len(s.tasks)),
		DAGs:                make(map[string]*domain.DAG, len(s.dags)),
		TasksByDAG:          make(map[string][]string, len(s.tasksByDAG)),
		Workers:             make(map[string]*domain.Worker, len(s.workers)),
		Tenants:             make(map[string]*domain.TenantQuota, len(s.tenants)),
		ActiveTenantRunning: make(map[string]int32, len(s.activeTenantRunning)),
		ActiveTenantQueued:  make(map[string]int32, len(s.activeTenantQueued)),
		IdempotencyIndex:    make(map[string]string, len(s.idempotencyIndex)),
	}

	for k, v := range s.tasks {
		c := *v
		snap.Tasks[k] = &c
	}
	for k, v := range s.dags {
		c := *v
		snap.DAGs[k] = &c
	}
	for k, v := range s.tasksByDAG {
		c := make([]string, len(v))
		copy(c, v)
		snap.TasksByDAG[k] = c
	}
	for k, v := range s.workers {
		c := *v
		snap.Workers[k] = &c
	}
	for k, v := range s.tenants {
		c := *v
		snap.Tenants[k] = &c
	}
	for k, v := range s.activeTenantRunning {
		snap.ActiveTenantRunning[k] = v
	}
	for k, v := range s.activeTenantQueued {
		snap.ActiveTenantQueued[k] = v
	}
	for k, v := range s.idempotencyIndex {
		snap.IdempotencyIndex[k] = v
	}

	return json.Marshal(snap)
}

// ImportSnapshot restores all authoritative state from a serialized snapshot.
func (s *Store) ImportSnapshot(data []byte) error {
	var snap StoreSnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return fmt.Errorf("failed to decode store snapshot: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	s.tasks = snap.Tasks
	s.dags = snap.DAGs
	s.tasksByDAG = snap.TasksByDAG
	s.workers = snap.Workers
	s.tenants = snap.Tenants
	s.activeTenantRunning = snap.ActiveTenantRunning
	s.activeTenantQueued = snap.ActiveTenantQueued
	s.idempotencyIndex = snap.IdempotencyIndex

	// Re-initialize volatile process-local runtime state
	s.workerRuntime = make(map[string]*domain.WorkerRuntimeState)

	return nil
}
