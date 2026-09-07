package worker

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TaskHandler represents the execution logic for a task payload.
type TaskHandler func(ctx context.Context, payload []byte) ([]byte, error)

// Daemon implements the operational worker node running the worker-pull protocol.
type Daemon struct {
	cfg       *config.WorkerConfig
	sessionID string
	logger    *slog.Logger
	handler   TaskHandler

	conn   *grpc.ClientConn
	client schedulerv1.WorkerServiceClient

	maxSlots       int32
	availableSlots atomic.Int32

	mu           sync.Mutex
	activeLeases map[string]*activeTaskInfo
	stopCh       chan struct{}

	pausedHeartbeats atomic.Bool
	killed           atomic.Bool
}

type activeTaskInfo struct {
	taskID     string
	leaseEpoch uint64
	cancelFunc context.CancelFunc
}

// generateSessionUUID produces a compliant RFC 4122 v4 UUID string.
func generateSessionUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // RFC 4122 v4
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// NewDaemon creates a new worker daemon with a unique process incarnation SessionID.
func NewDaemon(cfg *config.WorkerConfig, handler TaskHandler, logger *slog.Logger) *Daemon {
	if handler == nil {
		// Default mock handler: returns success after brief delay
		handler = func(ctx context.Context, payload []byte) ([]byte, error) {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(50 * time.Millisecond):
				return []byte("task-completed-ok"), nil
			}
		}
	}

	if logger == nil {
		logger = slog.Default()
	}

	sessionID := cfg.SessionID
	if sessionID == "" {
		sessionID = generateSessionUUID()
	}

	d := &Daemon{
		cfg:          cfg,
		sessionID:    sessionID,
		logger:       logger,
		handler:      handler,
		maxSlots:     cfg.MaxSlots,
		activeLeases: make(map[string]*activeTaskInfo),
		stopCh:       make(chan struct{}),
	}
	d.availableSlots.Store(cfg.MaxSlots)
	return d
}

// WorkerID returns the logical worker identifier for this daemon.
func (d *Daemon) WorkerID() string {
	return d.cfg.WorkerID
}

// SessionID returns the unique process incarnation identifier for this daemon.
func (d *Daemon) SessionID() string {
	return d.sessionID
}

// SetSessionID overrides the session ID for testing incarnation transitions.
func (d *Daemon) SetSessionID(id string) {
	d.sessionID = id
}

// PauseHeartbeats temporarily disables heartbeats to simulate network disconnect or freeze.
func (d *Daemon) PauseHeartbeats() {
	d.pausedHeartbeats.Store(true)
}

// ResumeHeartbeats resumes heartbeat transmission.
func (d *Daemon) ResumeHeartbeats() {
	d.pausedHeartbeats.Store(false)
}

// Kill simulates an abrupt process termination (SIGKILL) without clean context teardown.
func (d *Daemon) Kill() {
	d.killed.Store(true)
	d.pausedHeartbeats.Store(true)
	d.Stop()
}

// AvailableSlots returns the current free execution slot count.
func (d *Daemon) AvailableSlots() int32 {
	return d.availableSlots.Load()
}

// Start connects to the coordinator, registers, and starts worker-pull and heartbeat loops.
func (d *Daemon) Start(ctx context.Context) error {
	conn, err := grpc.NewClient(d.cfg.CoordinatorEndpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial coordinator %s: %w", d.cfg.CoordinatorEndpoint, err)
	}
	d.conn = conn
	d.client = schedulerv1.NewWorkerServiceClient(conn)

	// 1. Register with coordinator
	regResp, err := d.client.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  d.cfg.WorkerID,
		SessionId: d.sessionID,
		Address:   d.cfg.Address,
		MaxSlots:  d.cfg.MaxSlots,
	})
	if err != nil {
		return fmt.Errorf("failed to register worker: %w", err)
	}
	if !regResp.GetAccepted() {
		return errors.New("worker registration was rejected by coordinator")
	}

	d.logger.Info("worker registered successfully",
		"worker_id", d.cfg.WorkerID,
		"session_id", d.sessionID,
		"max_slots", d.maxSlots,
		"coordinator", d.cfg.CoordinatorEndpoint,
	)

	// 2. Launch background heartbeat loop
	go d.runHeartbeatLoop()

	// 3. Launch background worker-pull loop
	go d.runPullLoop()

	return nil
}

// Stop terminates the worker daemon and closes connections.
func (d *Daemon) Stop() {
	d.mu.Lock()
	select {
	case <-d.stopCh:
		d.mu.Unlock()
		return
	default:
		close(d.stopCh)
	}

	// Cancel any active tasks
	for _, task := range d.activeLeases {
		if task.cancelFunc != nil {
			task.cancelFunc()
		}
	}
	d.mu.Unlock()

	if d.conn != nil {
		_ = d.conn.Close()
	}
	d.logger.Info("worker daemon stopped", "worker_id", d.cfg.WorkerID)
}

// runPullLoop continuously pulls tasks whenever available slots > 0.
func (d *Daemon) runPullLoop() {
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			if d.killed.Load() {
				return
			}
			avail := d.availableSlots.Load()
			if avail > 0 {
				d.tryPullTask()
			}
		}
	}
}

func (d *Daemon) tryPullTask() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := d.client.PullTask(ctx, &schedulerv1.PullTaskRequest{
		WorkerId:       d.cfg.WorkerID,
		SessionId:      d.sessionID,
		AvailableSlots: d.availableSlots.Load(),
	})
	if err != nil || resp == nil || !resp.GetHasTask() {
		return
	}

	assignment := resp.GetAssignment()
	if assignment == nil {
		return
	}

	// Explicit slot accounting: decrement available slots
	newAvail := d.availableSlots.Add(-1)
	if newAvail < 0 {
		// Invariant violation guard
		d.availableSlots.Store(0)
		return
	}

	d.logger.Info("worker pulled task",
		"worker_id", d.cfg.WorkerID,
		"session_id", d.sessionID,
		"task_id", assignment.GetTaskId(),
		"lease_epoch", assignment.GetLeaseEpoch(),
		"available_slots", newAvail,
	)

	// Execute task in goroutine
	go d.executeTask(assignment)
}

func (d *Daemon) executeTask(task *schedulerv1.TaskAssignment) {
	taskID := task.GetTaskId()
	leaseEpoch := task.GetLeaseEpoch()

	// Monotonic local preemption deadline: LeaseDuration - PreemptBuffer
	leaseDur := time.Duration(task.GetLeaseDurationMs()) * time.Millisecond
	preemptDeadline := time.Now().Add(leaseDur - d.cfg.PreemptBuffer)

	ctx, cancel := context.WithDeadline(context.Background(), preemptDeadline)
	defer cancel()

	d.mu.Lock()
	d.activeLeases[taskID] = &activeTaskInfo{
		taskID:     taskID,
		leaseEpoch: leaseEpoch,
		cancelFunc: cancel,
	}
	d.mu.Unlock()

	defer func() {
		d.mu.Lock()
		delete(d.activeLeases, taskID)
		d.mu.Unlock()

		// Slot release guard: AvailableSlots must never exceed MaxSlots
		curr := d.availableSlots.Add(1)
		if curr > d.maxSlots {
			d.availableSlots.Store(d.maxSlots)
		}
	}()

	// 1. Send AckTaskRunning
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 2*time.Second)
	_, _ = d.client.AckTaskRunning(ackCtx, &schedulerv1.AckTaskRunningRequest{
		WorkerId:   d.cfg.WorkerID,
		SessionId:  d.sessionID,
		TaskId:     taskID,
		LeaseEpoch: leaseEpoch,
		StartedAt:  timestamppb.Now(),
	})
	ackCancel()

	// 2. Execute task via handler
	result, execErr := d.handler(ctx, task.GetPayload())

	// 3. Report completion or failure
	reportCtx, reportCancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer reportCancel()

	if execErr != nil {
		d.logger.Warn("task execution failed on worker",
			"task_id", taskID,
			"error", execErr,
		)
		_, _ = d.client.ReportTaskFailed(reportCtx, &schedulerv1.ReportTaskFailedRequest{
			WorkerId:     d.cfg.WorkerID,
			SessionId:    d.sessionID,
			TaskId:       taskID,
			LeaseEpoch:   leaseEpoch,
			ErrorMessage: execErr.Error(),
			Retryable:    true,
			FailedAt:     timestamppb.Now(),
		})
	} else {
		d.logger.Info("task finished successfully on worker",
			"task_id", taskID,
		)
		_, _ = d.client.ReportTaskCompleted(reportCtx, &schedulerv1.ReportTaskCompletedRequest{
			WorkerId:    d.cfg.WorkerID,
			SessionId:   d.sessionID,
			TaskId:      taskID,
			LeaseEpoch:  leaseEpoch,
			Result:      result,
			CompletedAt: timestamppb.Now(),
		})
	}
}

// runHeartbeatLoop periodically reports liveness and receives revoked task notifications.
func (d *Daemon) runHeartbeatLoop() {
	ticker := time.NewTicker(d.cfg.HeartbeatInterval)
	defer ticker.Stop()

	for {
		select {
		case <-d.stopCh:
			return
		case <-ticker.C:
			if d.killed.Load() {
				return
			}
			if !d.pausedHeartbeats.Load() {
				d.sendHeartbeat()
			}
		}
	}
}

func (d *Daemon) sendHeartbeat() {
	d.mu.Lock()
	var active []*schedulerv1.ActiveLeaseHeartbeat
	for _, info := range d.activeLeases {
		active = append(active, &schedulerv1.ActiveLeaseHeartbeat{
			TaskId:     info.taskID,
			LeaseEpoch: info.leaseEpoch,
		})
	}
	d.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	resp, err := d.client.Heartbeat(ctx, &schedulerv1.HeartbeatRequest{
		WorkerId:        d.cfg.WorkerID,
		SessionId:       d.sessionID,
		AvailableSlots:  d.availableSlots.Load(),
		ActiveLeases:    active,
		ClientTimestamp: timestamppb.Now(),
	})
	if err != nil || resp == nil {
		return
	}

	// Handle revoked tasks: cancel local execution immediately
	if len(resp.GetRevokedTaskIds()) > 0 {
		d.mu.Lock()
		for _, revokedID := range resp.GetRevokedTaskIds() {
			if info, exists := d.activeLeases[revokedID]; exists {
				d.logger.Warn("lease revoked by coordinator; preempting local execution", "task_id", revokedID)
				if info.cancelFunc != nil {
					info.cancelFunc()
				}
			}
		}
		d.mu.Unlock()
	}
}

// SimulateStaleCompletion transmits an explicit ReportTaskCompleted RPC with specified session and epoch.
func (d *Daemon) SimulateStaleCompletion(ctx context.Context, taskID, sessionID string, epoch uint64, result []byte) error {
	if d.client == nil {
		return errors.New("worker client not connected")
	}
	_, err := d.client.ReportTaskCompleted(ctx, &schedulerv1.ReportTaskCompletedRequest{
		WorkerId:    d.cfg.WorkerID,
		SessionId:   sessionID,
		TaskId:      taskID,
		LeaseEpoch:  epoch,
		Result:      result,
		CompletedAt: timestamppb.Now(),
	})
	return err
}

// ReconnectTo switches the daemon to a new coordinator endpoint (e.g. after cluster failover).
func (d *Daemon) ReconnectTo(ctx context.Context, endpoint string) error {
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial new coordinator %s: %w", endpoint, err)
	}

	newClient := schedulerv1.NewWorkerServiceClient(conn)
	regResp, err := newClient.RegisterWorker(ctx, &schedulerv1.RegisterWorkerRequest{
		WorkerId:  d.cfg.WorkerID,
		SessionId: d.sessionID,
		Address:   d.cfg.Address,
		MaxSlots:  d.maxSlots,
	})
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("failed to register with new coordinator %s: %w", endpoint, err)
	}
	if !regResp.GetAccepted() {
		_ = conn.Close()
		return fmt.Errorf("registration rejected by new coordinator %s", endpoint)
	}

	d.mu.Lock()
	if d.conn != nil {
		_ = d.conn.Close()
	}
	d.conn = conn
	d.client = newClient
	d.mu.Unlock()

	d.logger.Info("worker successfully reconnected to coordinator",
		"worker_id", d.cfg.WorkerID,
		"session_id", d.sessionID,
		"new_endpoint", endpoint,
	)
	return nil
}

func (d *Daemon) tryFailover(ctx context.Context, triggerErr error) {
	if len(d.cfg.CoordinatorAddrs) == 0 {
		return
	}
	st, ok := status.FromError(triggerErr)
	if !ok || (st.Code() != codes.Unavailable && st.Code() != codes.FailedPrecondition) {
		return
	}

	for _, addr := range d.cfg.CoordinatorAddrs {
		if err := d.ReconnectTo(ctx, addr); err == nil {
			return
		}
	}
}
