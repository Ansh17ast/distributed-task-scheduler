package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"sync"
	"time"

	schedulerv1 "distributed-scheduler/api/proto/v1"
	"distributed-scheduler/internal/config"
	"distributed-scheduler/internal/consensus"
	"distributed-scheduler/internal/dag"
	"distributed-scheduler/internal/domain"
	"distributed-scheduler/internal/scheduler"
	"distributed-scheduler/internal/state"
	"distributed-scheduler/internal/telemetry"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Server encapsulates the coordinator gRPC server, Raft consensus node, and scheduling loop.
type Server struct {
	schedulerv1.UnimplementedCoordinatorServiceServer
	schedulerv1.UnimplementedWorkerServiceServer

	cfg        *config.CoordinatorConfig
	logger     *slog.Logger
	store      *state.Store
	readyQueue *scheduler.ReadyQueue
	dagEngine  *dag.Engine
	grpcServer *grpc.Server
	raftNode   *consensus.RaftNode

	mu          sync.Mutex
	stopCh      chan struct{}
	retryTimers map[string]*time.Timer

	metrics    *telemetry.Metrics
	tracer     *telemetry.TracerProvider
	httpServer *telemetry.HTTPServer
	startTime  time.Time
}

// NewServer creates and initializes a coordinator Server.
func NewServer(cfg *config.CoordinatorConfig, store *state.Store, policy scheduler.Policy, logger *slog.Logger) *Server {
	if policy == nil {
		policy = &scheduler.PriorityPolicy{}
	}
	if logger == nil {
		logger = slog.Default()
	}
	m := telemetry.NewMetrics()
	tp, _ := telemetry.InitTracer("coordinator", cfg.NodeID, false)
	return &Server{
		cfg:         cfg,
		logger:      logger,
		store:       store,
		readyQueue:  scheduler.NewReadyQueue(policy, store),
		dagEngine:   dag.NewEngine(store, logger),
		stopCh:      make(chan struct{}),
		retryTimers: make(map[string]*time.Timer),
		metrics:     m,
		tracer:      tp,
		startTime:   time.Now(),
	}
}

// SetMetrics configures custom Prometheus metrics.
func (s *Server) SetMetrics(m *telemetry.Metrics) {
	s.metrics = m
}

// Metrics returns the Prometheus metrics collector.
func (s *Server) Metrics() *telemetry.Metrics {
	return s.metrics
}

// HTTPServer returns the active telemetry HTTP server, if running.
func (s *Server) HTTPServer() *telemetry.HTTPServer {
	return s.httpServer
}

// IsReady implements telemetry.StatusProvider.
func (s *Server) IsReady() bool {
	if s.raftNode == nil {
		return true
	}
	lAddr, _ := s.raftNode.LeaderWithID()
	return s.raftNode.IsLeader() || lAddr != ""
}

// GetClusterStatus implements telemetry.StatusProvider.
func (s *Server) GetClusterStatus() telemetry.ClusterStatus {
	nodeID := s.cfg.NodeID
	role := "Standalone"
	var term, commitIdx, appliedIdx uint64
	leaderAddr := ""
	leaderID := ""

	if s.raftNode != nil {
		term = s.raftNode.CurrentTerm()
		commitIdx = s.raftNode.LastIndex()
		appliedIdx = s.raftNode.LastIndex()
		lAddr, lID := s.raftNode.LeaderWithID()
		leaderAddr = string(lAddr)
		leaderID = string(lID)
		if s.raftNode.IsLeader() {
			role = "Leader"
		} else {
			role = "Follower"
		}
	}

	workers := s.store.GetActiveWorkers()
	queueDepth := s.readyQueue.Len()
	uptime := time.Since(s.startTime).Seconds()

	return telemetry.ClusterStatus{
		NodeID:        nodeID,
		Role:          role,
		LeaderID:      leaderID,
		LeaderAddr:    leaderAddr,
		Term:          term,
		CommitIndex:   commitIdx,
		AppliedIndex:  appliedIdx,
		ActiveWorkers: len(workers),
		QueueDepth:    queueDepth,
		StartTime:     s.startTime,
		UptimeSeconds: uptime,
	}
}

// SetHTTPServer allows setting an explicit HTTP telemetry server (useful in tests).
func (s *Server) SetHTTPServer(hs *telemetry.HTTPServer) {
	s.httpServer = hs
}

// SetTracerProvider sets the OpenTelemetry tracer provider.
func (s *Server) SetTracerProvider(tp *telemetry.TracerProvider) {
	s.tracer = tp
}

// SetRaftNode attaches an active RaftNode to the server for distributed consensus.
func (s *Server) SetRaftNode(rn *consensus.RaftNode) {
	s.raftNode = rn
}

// RaftNode returns the attached RaftNode, if configured.
func (s *Server) RaftNode() *consensus.RaftNode {
	return s.raftNode
}

// Store returns the underlying state store.
func (s *Server) Store() *state.Store {
	return s.store
}

// Start begins listening on gRPC and starts background routines.
func (s *Server) Start(lis net.Listener) error {
	s.grpcServer = grpc.NewServer()
	schedulerv1.RegisterCoordinatorServiceServer(s.grpcServer, s)
	schedulerv1.RegisterWorkerServiceServer(s.grpcServer, s)

	if s.raftNode != nil {
		// Replicated cluster mode: monitor leadership transitions
		go s.monitorLeadership()
	} else {
		// Standalone mode: always authoritative leader
		go s.runLeaseReaper()
	}

	if s.cfg.HTTPPort > 0 && s.httpServer == nil {
		httpAddr := fmt.Sprintf("127.0.0.1:%d", s.cfg.HTTPPort)
		s.httpServer = telemetry.NewHTTPServer(httpAddr, s.metrics, s)
		if err := s.httpServer.Start(); err != nil {
			s.logger.Warn("failed to start telemetry HTTP server", "error", err)
		} else {
			s.logger.Info("telemetry HTTP server started", "addr", s.httpServer.Addr())
		}
	}

	s.logger.Info("coordinator gRPC server started",
		"addr", lis.Addr().String(),
		"node_id", s.cfg.NodeID,
		"replicated", s.raftNode != nil,
	)
	return s.grpcServer.Serve(lis)
}

// Stop gracefully terminates the coordinator.
func (s *Server) Stop() {
	s.mu.Lock()
	select {
	case <-s.stopCh:
		s.mu.Unlock()
		return
	default:
		close(s.stopCh)
	}
	for _, t := range s.retryTimers {
		t.Stop()
	}
	s.retryTimers = make(map[string]*time.Timer)
	s.mu.Unlock()

	if s.httpServer != nil {
		_ = s.httpServer.Stop(context.Background())
	}
	if s.tracer != nil {
		_ = s.tracer.Shutdown(context.Background())
	}
	if s.raftNode != nil {
		_ = s.raftNode.Shutdown()
	}
	if s.grpcServer != nil {
		s.grpcServer.GracefulStop()
	}
}

// ============================================================================
// RAFT LEADERSHIP & FAILOVER RECONCILIATION ROUTINES
// ============================================================================

func (s *Server) monitorLeadership() {
	leaderCh := s.raftNode.LeaderCh()
	for {
		select {
		case <-s.stopCh:
			return
		case isLeader, ok := <-leaderCh:
			if !ok {
				return
			}
			if isLeader {
				s.onElectedLeader()
			} else {
				s.onStepDown()
			}
		}
	}
}

func (s *Server) onElectedLeader() {
	s.logger.Info("coordinator acquired cluster leadership", "node_id", s.cfg.NodeID)
	if s.metrics != nil {
		s.metrics.RaftLeaderChanges.WithLabelValues(s.cfg.NodeID).Inc()
		s.metrics.LeaderFailoversTotal.WithLabelValues(s.cfg.NodeID).Inc()
		if s.raftNode != nil {
			s.metrics.RaftCurrentTerm.WithLabelValues(s.cfg.NodeID).Set(float64(s.raftNode.CurrentTerm()))
		}
	}

	// 1. Recover in-memory retry timers for any replicated RETRY_WAIT tasks
	s.recoverRetryTimers()

	// 2. Start failover lease reconciliation window
	reconcileDur := s.cfg.FailoverReconcile
	if reconcileDur == 0 {
		reconcileDur = 2 * time.Second
	}
	s.logger.Info("starting failover lease reconciliation window", "duration", reconcileDur)

	time.AfterFunc(reconcileDur, func() {
		s.mu.Lock()
		select {
		case <-s.stopCh:
			s.mu.Unlock()
			return
		default:
		}
		s.mu.Unlock()

		if s.raftNode != nil && !s.raftNode.IsLeader() {
			return // Stepped down before reconciliation concluded
		}
		s.logger.Info("failover reconciliation window concluded; sweeping unadopted leases")
		s.SweepExpiredLeases()
	})

	// 3. Start background lease reaper
	go s.runLeaseReaper()
}

func (s *Server) onStepDown() {
	s.logger.Info("coordinator stepped down from leadership to follower", "node_id", s.cfg.NodeID)
	s.mu.Lock()
	for _, t := range s.retryTimers {
		t.Stop()
	}
	s.retryTimers = make(map[string]*time.Timer)
	s.mu.Unlock()
}

func (s *Server) recoverRetryTimers() {
	s.mu.Lock()
	defer s.mu.Unlock()

	retryTasks := s.store.GetTasksByState(domain.StateRetryWait)
	if len(retryTasks) > 0 {
		s.logger.Info("recovering in-memory retry timers for RETRY_WAIT tasks", "count", len(retryTasks))
	}

	for _, task := range retryTasks {
		taskID := task.ID
		if _, exists := s.retryTimers[taskID]; exists {
			continue
		}
		delay := s.calculateBackoff(task.AttemptCount)
		timer := time.AfterFunc(delay, func() {
			s.mu.Lock()
			delete(s.retryTimers, taskID)
			s.mu.Unlock()

			if s.raftNode != nil && !s.raftNode.IsLeader() {
				return
			}

			s.logger.Info("recovered retry timer fired; promoting task to READY", "task_id", taskID)
			if s.raftNode != nil {
				cmd := &consensus.Command{
					Op:        consensus.OpMarkTaskReady,
					TaskID:    taskID,
					Timestamp: time.Now(),
				}
				_, _ = s.raftNode.Apply(cmd, 3*time.Second)
			} else {
				_, _ = s.store.MarkTaskReady(taskID)
			}
		})
		s.retryTimers[taskID] = timer
	}
}

func (s *Server) ensureLeader() error {
	if s.raftNode == nil {
		return nil
	}
	if !s.raftNode.IsLeader() {
		leaderAddr, leaderID := s.raftNode.LeaderWithID()
		return status.Errorf(codes.FailedPrecondition, "not cluster leader: current leader is %s at %s", leaderID, leaderAddr)
	}
	return nil
}

func (s *Server) verifyLinearizableRead() error {
	if s.raftNode == nil {
		return nil
	}
	if err := s.raftNode.VerifyLeader(); err != nil {
		return status.Errorf(codes.Unavailable, "linearizable read failed: node lost quorum: %v", err)
	}
	return nil
}

// ============================================================================
// CoordinatorServiceServer RPC Implementations
// ============================================================================

func (s *Server) SubmitTask(ctx context.Context, req *schedulerv1.SubmitTaskRequest) (*schedulerv1.SubmitTaskResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	spec := req.GetTask()
	if spec == nil {
		return nil, status.Error(codes.InvalidArgument, "task specification is required")
	}
	if spec.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "task id is required")
	}
	if spec.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant id is required")
	}
	if len(spec.GetPayload()) > 1024*1024 {
		return nil, status.Error(codes.InvalidArgument, "task payload exceeds maximum permitted limit of 1MB")
	}
	if len(spec.GetDependencies()) > 50 {
		return nil, status.Error(codes.InvalidArgument, "task dependencies count exceeds maximum limit of 50")
	}

	ctx, span := telemetry.StartSpan(ctx, "coordinator.SubmitTask",
		trace.WithAttributes(
			telemetry.AttrTaskID.String(spec.GetId()),
			telemetry.AttrTenantID.String(spec.GetTenantId()),
			telemetry.AttrNodeID.String(s.cfg.NodeID),
		),
	)
	defer span.End()

	submitStart := time.Now()
	task := &domain.Task{
		ID:             spec.GetId(),
		TenantID:       spec.GetTenantId(),
		Priority:       spec.GetPriority(),
		MaxRetries:     spec.GetMaxRetries(),
		Timeout:        time.Duration(spec.GetTimeoutSeconds()) * time.Second,
		PayloadType:    spec.GetPayloadType(),
		Payload:        spec.GetPayload(),
		IdempotencyKey: spec.GetIdempotencyKey(),
		Dependencies:   spec.GetDependencies(),
		CreatedAt:      submitStart,
	}

	var admittedTask *domain.Task
	var err error

	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:        consensus.OpSubmitTask,
			Task:      task,
			Timestamp: submitStart,
		}
		res, applyErr := s.raftNode.Apply(cmd, 3*time.Second)
		if applyErr != nil {
			if s.metrics != nil {
				s.metrics.RaftCommandsTotal.WithLabelValues(s.cfg.NodeID, "submit_task", "failure").Inc()
			}
			if errors.Is(applyErr, domain.ErrTenantQuotaExceeded) {
				if s.metrics != nil {
					s.metrics.SchedulerRejectedTasks.WithLabelValues(spec.GetTenantId(), "quota_exceeded").Inc()
				}
				return nil, status.Error(codes.ResourceExhausted, "tenant queue limit exceeded")
			}
			return nil, status.Errorf(codes.Internal, "raft replication failed: %v", applyErr)
		}
		if s.metrics != nil {
			s.metrics.RaftCommitLatency.WithLabelValues(s.cfg.NodeID, "submit_task").Observe(time.Since(submitStart).Seconds())
			s.metrics.RaftCommandsTotal.WithLabelValues(s.cfg.NodeID, "submit_task", "success").Inc()
		}
		admittedTask = res.(*domain.Task)
	} else {
		admittedTask, err = s.store.SubmitTask(task)
		if err != nil {
			if errors.Is(err, domain.ErrTenantQuotaExceeded) {
				if s.metrics != nil {
					s.metrics.SchedulerRejectedTasks.WithLabelValues(spec.GetTenantId(), "quota_exceeded").Inc()
				}
				return nil, status.Error(codes.ResourceExhausted, "tenant queue limit exceeded")
			}
			return nil, status.Errorf(codes.Internal, "failed to submit task: %v", err)
		}
	}

	// If root task (0 dependencies), promote to READY
	if len(spec.GetDependencies()) == 0 {
		if s.raftNode != nil {
			cmdReady := &consensus.Command{
				Op:        consensus.OpMarkTaskReady,
				TaskID:    admittedTask.ID,
				Timestamp: time.Now(),
			}
			resReady, applyErr := s.raftNode.Apply(cmdReady, 3*time.Second)
			if applyErr == nil {
				admittedTask = resReady.(*domain.Task)
			}
		} else {
			readyTask, readyErr := s.store.MarkTaskReady(admittedTask.ID)
			if readyErr == nil {
				admittedTask = readyTask
			}
		}
	}

	if s.metrics != nil {
		s.metrics.TasksSubmittedTotal.WithLabelValues(admittedTask.TenantID).Inc()
		s.metrics.SchedulerQueueDepth.WithLabelValues(admittedTask.TenantID).Set(float64(s.readyQueue.Len()))
	}

	s.logger.Info("task admitted",
		"task_id", admittedTask.ID,
		"state", admittedTask.State,
		"tenant", admittedTask.TenantID,
		"replicated", s.raftNode != nil,
	)

	return &schedulerv1.SubmitTaskResponse{
		TaskId:      admittedTask.ID,
		State:       mapDomainStateToProto(admittedTask.State),
		SubmittedAt: timestamppb.New(admittedTask.CreatedAt),
	}, nil
}

func (s *Server) SubmitDAG(ctx context.Context, req *schedulerv1.SubmitDAGRequest) (*schedulerv1.SubmitDAGResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	if req.GetDagId() == "" {
		return nil, status.Error(codes.InvalidArgument, "dag_id is required")
	}
	if req.GetTenantId() == "" {
		return nil, status.Error(codes.InvalidArgument, "tenant_id is required")
	}
	if len(req.GetTasks()) == 0 {
		return nil, status.Error(codes.InvalidArgument, "dag must contain at least one task")
	}
	if len(req.GetTasks()) > 500 {
		return nil, status.Error(codes.InvalidArgument, "dag task count exceeds maximum limit of 500 tasks")
	}

	ctx, span := telemetry.StartSpan(ctx, "coordinator.SubmitDAG",
		trace.WithAttributes(
			telemetry.AttrDAGID.String(req.GetDagId()),
			telemetry.AttrTenantID.String(req.GetTenantId()),
			telemetry.AttrNodeID.String(s.cfg.NodeID),
		),
	)
	defer span.End()

	validationStart := time.Now()
	domainDAG := &domain.DAG{
		ID:        req.GetDagId(),
		TenantID:  req.GetTenantId(),
		CreatedAt: validationStart,
	}

	domainTasks := make([]*domain.Task, 0, len(req.GetTasks()))
	for _, spec := range req.GetTasks() {
		domainTasks = append(domainTasks, &domain.Task{
			ID:             spec.GetId(),
			DAGID:          req.GetDagId(),
			TenantID:       spec.GetTenantId(),
			Priority:       spec.GetPriority(),
			MaxRetries:     spec.GetMaxRetries(),
			Timeout:        time.Duration(spec.GetTimeoutSeconds()) * time.Second,
			PayloadType:    spec.GetPayloadType(),
			Payload:        spec.GetPayload(),
			IdempotencyKey: spec.GetIdempotencyKey(),
			Dependencies:   spec.GetDependencies(),
			CreatedAt:      validationStart,
		})
	}

	if err := dag.ValidateDAG(domainDAG.ID, domainDAG.TenantID, domainTasks); err != nil {
		if s.metrics != nil {
			s.metrics.DAGCycleRejectionsTotal.WithLabelValues(domainDAG.TenantID).Inc()
		}
		return nil, status.Errorf(codes.InvalidArgument, "invalid dag: %v", err)
	}

	if s.metrics != nil {
		s.metrics.DAGValidationLatency.WithLabelValues(domainDAG.TenantID).Observe(time.Since(validationStart).Seconds())
	}

	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:        consensus.OpSubmitDAG,
			DAG:       domainDAG,
			Tasks:     domainTasks,
			Timestamp: validationStart,
		}
		if _, err := s.raftNode.Apply(cmd, 3*time.Second); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to replicate dag: %v", err)
		}
	} else {
		if err := s.store.SubmitDAG(domainDAG, domainTasks); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to submit dag: %v", err)
		}
	}

	if s.metrics != nil {
		s.metrics.DAGSubmissionsTotal.WithLabelValues(domainDAG.TenantID).Inc()
		s.metrics.SchedulerQueueDepth.WithLabelValues(domainDAG.TenantID).Set(float64(s.readyQueue.Len()))
	}

	s.logger.Info("dag admitted successfully",
		"dag_id", domainDAG.ID,
		"task_count", len(domainTasks),
		"replicated", s.raftNode != nil,
	)

	return &schedulerv1.SubmitDAGResponse{
		DagId:       domainDAG.ID,
		TaskCount:   int32(len(domainTasks)),
		SubmittedAt: timestamppb.New(validationStart),
	}, nil
}

func (s *Server) GetTask(ctx context.Context, req *schedulerv1.GetTaskRequest) (*schedulerv1.GetTaskResponse, error) {
	if err := s.verifyLinearizableRead(); err != nil {
		return nil, err
	}

	task, err := s.store.GetTask(req.GetTaskId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	resp := &schedulerv1.GetTaskResponse{
		TaskId:           task.ID,
		DagId:            task.DAGID,
		TenantId:         task.TenantID,
		State:            mapDomainStateToProto(task.State),
		Priority:         task.Priority,
		AttemptCount:     task.AttemptCount,
		MaxRetries:       task.MaxRetries,
		AssignedWorkerId: task.AssignedWorkerID,
		LeaseEpoch:       task.LeaseEpoch,
		Result:           task.Result,
		ErrorMessage:     task.ErrorMessage,
		CreatedAt:        timestamppb.New(task.CreatedAt),
	}
	if task.StartedAt != nil {
		resp.StartedAt = timestamppb.New(*task.StartedAt)
	}
	if task.CompletedAt != nil {
		resp.CompletedAt = timestamppb.New(*task.CompletedAt)
	}
	return resp, nil
}

func (s *Server) GetDAG(ctx context.Context, req *schedulerv1.GetDAGRequest) (*schedulerv1.GetDAGResponse, error) {
	if err := s.verifyLinearizableRead(); err != nil {
		return nil, err
	}

	dag, tasks, err := s.store.GetDAG(req.GetDagId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "dag not found")
	}

	var pending, running, succeeded, failed int32
	var taskResponses []*schedulerv1.GetTaskResponse

	for _, t := range tasks {
		switch t.State {
		case domain.StatePending, domain.StateBlocked, domain.StateReady, domain.StateRetryWait:
			pending++
		case domain.StateRunning, domain.StateLeased:
			running++
		case domain.StateSucceeded:
			succeeded++
		case domain.StateFailed, domain.StateCancelled, domain.StateLost:
			failed++
		}

		tr := &schedulerv1.GetTaskResponse{
			TaskId:           t.ID,
			DagId:            t.DAGID,
			TenantId:         t.TenantID,
			State:            mapDomainStateToProto(t.State),
			Priority:         t.Priority,
			AttemptCount:     t.AttemptCount,
			MaxRetries:       t.MaxRetries,
			AssignedWorkerId: t.AssignedWorkerID,
			LeaseEpoch:       t.LeaseEpoch,
			Result:           t.Result,
			ErrorMessage:     t.ErrorMessage,
			CreatedAt:        timestamppb.New(t.CreatedAt),
		}
		if t.StartedAt != nil {
			tr.StartedAt = timestamppb.New(*t.StartedAt)
		}
		if t.CompletedAt != nil {
			tr.CompletedAt = timestamppb.New(*t.CompletedAt)
		}
		taskResponses = append(taskResponses, tr)
	}

	return &schedulerv1.GetDAGResponse{
		DagId:          dag.ID,
		TenantId:       dag.TenantID,
		TotalTasks:     int32(len(tasks)),
		PendingTasks:   pending,
		RunningTasks:   running,
		SucceededTasks: succeeded,
		FailedTasks:    failed,
		Tasks:          taskResponses,
	}, nil
}

func (s *Server) CancelTask(ctx context.Context, req *schedulerv1.CancelTaskRequest) (*schedulerv1.CancelTaskResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	taskBefore, err := s.store.GetTask(req.GetTaskId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:        consensus.OpCancelTask,
			TaskID:    req.GetTaskId(),
			Reason:    req.GetReason(),
			Timestamp: time.Now(),
		}
		if _, err := s.raftNode.Apply(cmd, 3*time.Second); err != nil {
			if errors.Is(err, domain.ErrTerminalState) {
				return nil, status.Error(codes.FailedPrecondition, "task is already in terminal state and cannot be cancelled")
			}
			return nil, status.Errorf(codes.Internal, "failed to replicate cancel: %v", err)
		}
	} else {
		err = s.store.CancelTask(req.GetTaskId(), req.GetReason())
		if err != nil {
			if errors.Is(err, domain.ErrTerminalState) {
				return nil, status.Error(codes.FailedPrecondition, "task is already in terminal state and cannot be cancelled")
			}
			return nil, status.Errorf(codes.Internal, "failed to cancel task: %v", err)
		}
	}

	// Trigger DAG cancellation propagation
	if taskBefore.DAGID != "" {
		cancelledTaskIDs, _ := s.dagEngine.OnTaskCancelled(ctx, taskBefore)
		for _, ctID := range cancelledTaskIDs {
			if s.raftNode != nil {
				cmdCancel := &consensus.Command{
					Op:        consensus.OpCancelTask,
					TaskID:    ctID,
					Reason:    fmt.Sprintf("upstream %s cancelled", req.GetTaskId()),
					Timestamp: time.Now(),
				}
				_, _ = s.raftNode.Apply(cmdCancel, 3*time.Second)
			}
		}
	}

	return &schedulerv1.CancelTaskResponse{
		TaskId:        taskBefore.ID,
		PreviousState: mapDomainStateToProto(taskBefore.State),
		CurrentState:  schedulerv1.TaskState_TASK_STATE_CANCELLED,
	}, nil
}

func (s *Server) GetClusterHealth(ctx context.Context, req *schedulerv1.GetClusterHealthRequest) (*schedulerv1.GetClusterHealthResponse, error) {
	workers := s.store.GetActiveWorkers()
	readyTasks := s.store.GetReadyTasks()

	isLeader := true
	leaderID := s.cfg.NodeID
	leaderAddr := fmt.Sprintf(":%d", s.cfg.GRPCPort)
	var term uint64 = 1
	var commitIndex uint64 = 1

	if s.raftNode != nil {
		isLeader = s.raftNode.IsLeader()
		rLeaderAddr, rLeaderID := s.raftNode.LeaderWithID()
		leaderID = string(rLeaderID)
		leaderAddr = string(rLeaderAddr)
		term = s.raftNode.CurrentTerm()
		commitIndex = s.raftNode.LastIndex()
	}

	return &schedulerv1.GetClusterHealthResponse{
		NodeId:        s.cfg.NodeID,
		IsLeader:      isLeader,
		LeaderId:      leaderID,
		LeaderAddress: leaderAddr,
		CurrentTerm:   term,
		CommitIndex:   commitIndex,
		ActiveWorkers: int32(len(workers)),
		QueuedTasks:   int32(len(readyTasks)),
	}, nil
}

// ============================================================================
// WorkerServiceServer RPC Implementations (Worker-Pull Protocol)
// ============================================================================

func (s *Server) RegisterWorker(ctx context.Context, req *schedulerv1.RegisterWorkerRequest) (*schedulerv1.RegisterWorkerResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	if req.GetWorkerId() == "" {
		return nil, status.Error(codes.InvalidArgument, "worker_id is required")
	}
	if req.GetMaxSlots() <= 0 {
		return nil, status.Error(codes.InvalidArgument, "max_slots must be > 0")
	}

	worker := &domain.Worker{
		ID:        req.GetWorkerId(),
		SessionID: req.GetSessionId(),
		Address:   req.GetAddress(),
		MaxSlots:  req.GetMaxSlots(),
		Metadata:  req.GetMetadata(),
	}

	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:        consensus.OpRegisterWorker,
			Worker:    worker,
			Timestamp: time.Now(),
		}
		if _, err := s.raftNode.Apply(cmd, 3*time.Second); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to replicate worker registration: %v", err)
		}
	} else {
		if err := s.store.RegisterWorker(worker); err != nil {
			return nil, status.Errorf(codes.Internal, "failed to register worker: %v", err)
		}
	}
	_ = s.store.UpdateWorkerHeartbeat(worker.ID, worker.SessionID)

	if s.metrics != nil {
		s.metrics.WorkerRegisteredTotal.WithLabelValues(s.cfg.NodeID).Inc()
		s.metrics.WorkerActive.WithLabelValues(s.cfg.NodeID).Set(float64(len(s.store.GetActiveWorkers())))
	}

	s.logger.Info("worker registered",
		"worker_id", worker.ID,
		"session_id", worker.SessionID,
		"max_slots", worker.MaxSlots,
	)

	return &schedulerv1.RegisterWorkerResponse{
		Accepted:            true,
		LeaderId:            s.cfg.NodeID,
		HeartbeatIntervalMs: s.cfg.RaftHeartbeat.Milliseconds(),
		LeaseDurationMs:     s.cfg.WorkerLeaseDur.Milliseconds(),
	}, nil
}

func (s *Server) Heartbeat(ctx context.Context, req *schedulerv1.HeartbeatRequest) (*schedulerv1.HeartbeatResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	if s.metrics != nil {
		s.metrics.WorkerHeartbeatTotal.WithLabelValues(s.cfg.NodeID).Inc()
	}

	if err := s.store.UpdateWorkerHeartbeat(req.GetWorkerId(), req.GetSessionId()); err != nil {
		if errors.Is(err, domain.ErrStaleWorkerSession) {
			if s.metrics != nil {
				s.metrics.StaleCommandRejectionsTotal.WithLabelValues(s.cfg.NodeID, "stale_session_heartbeat").Inc()
			}
			return nil, status.Error(codes.FailedPrecondition, "stale worker session: heartbeat rejected")
		}
		return nil, status.Error(codes.NotFound, "worker not registered")
	}

	// Verify all active leases claimed by worker (Adoption & Fencing Rule)
	var revokedTasks []string
	for _, active := range req.GetActiveLeases() {
		task, err := s.store.GetTask(active.GetTaskId())
		if err != nil ||
			task.AssignedWorkerID != req.GetWorkerId() ||
			(req.GetSessionId() != "" && task.AssignedSessionID != "" && task.AssignedSessionID != req.GetSessionId()) ||
			task.LeaseEpoch != active.GetLeaseEpoch() ||
			(task.State != domain.StateLeased && task.State != domain.StateRunning) {
			revokedTasks = append(revokedTasks, active.GetTaskId())
		}
	}

	return &schedulerv1.HeartbeatResponse{
		Acknowledged:   true,
		RevokedTaskIds: revokedTasks,
	}, nil
}

func (s *Server) PullTask(ctx context.Context, req *schedulerv1.PullTaskRequest) (*schedulerv1.PullTaskResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	ctx, span := telemetry.StartSpan(ctx, "coordinator.PullTask",
		trace.WithAttributes(
			telemetry.AttrWorkerID.String(req.GetWorkerId()),
			telemetry.AttrSessionID.String(req.GetSessionId()),
			telemetry.AttrNodeID.String(s.cfg.NodeID),
		),
	)
	defer span.End()

	worker, err := s.store.GetWorker(req.GetWorkerId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "worker not registered; please register first")
	}

	if req.GetSessionId() != "" && worker.SessionID != "" && worker.SessionID != req.GetSessionId() {
		if s.metrics != nil {
			s.metrics.StaleCommandRejectionsTotal.WithLabelValues(s.cfg.NodeID, "stale_session_pull").Inc()
		}
		return nil, status.Error(codes.FailedPrecondition, "stale worker session: pull rejected")
	}

	if worker.AvailableSlots <= 0 {
		return &schedulerv1.PullTaskResponse{HasTask: false}, nil
	}

	// Consult ReadyQueue WITHOUT holding store lock
	evalStart := time.Now()
	candidate, reason, err := s.readyQueue.SelectNextTask(ctx, worker)
	if s.metrics != nil {
		s.metrics.SchedulerDecisionLatency.WithLabelValues("priority").Observe(time.Since(evalStart).Seconds())
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "scheduler evaluation error: %v", err)
	}
	if candidate == nil {
		return &schedulerv1.PullTaskResponse{HasTask: false}, nil
	}

	var leasedTask *domain.Task
	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:            consensus.OpGrantTaskLease,
			TaskID:        candidate.ID,
			WorkerID:      worker.ID,
			SessionID:     req.GetSessionId(),
			LeaseDuration: s.cfg.WorkerLeaseDur,
			Timestamp:     time.Now(),
		}
		res, applyErr := s.raftNode.Apply(cmd, 3*time.Second)
		if applyErr != nil {
			// Leased concurrently by another worker; return false
			return &schedulerv1.PullTaskResponse{HasTask: false}, nil
		}
		leasedTask = res.(*domain.Task)
	} else {
		var grantErr error
		leasedTask, grantErr = s.store.GrantTaskLease(candidate.ID, worker.ID, req.GetSessionId(), s.cfg.WorkerLeaseDur)
		if grantErr != nil {
			return &schedulerv1.PullTaskResponse{HasTask: false}, nil
		}
	}

	if s.metrics != nil {
		s.metrics.SchedulerDispatchTotal.WithLabelValues(leasedTask.TenantID, "dispatched").Inc()
		s.metrics.TaskQueueWaitDuration.WithLabelValues(leasedTask.TenantID).Observe(time.Since(leasedTask.CreatedAt).Seconds())
		s.metrics.SchedulerQueueDepth.WithLabelValues(leasedTask.TenantID).Set(float64(s.readyQueue.Len()))
	}

	s.logger.Info("task dispatched to worker",
		"task_id", leasedTask.ID,
		"worker_id", worker.ID,
		"session_id", leasedTask.AssignedSessionID,
		"lease_epoch", leasedTask.LeaseEpoch,
		"reason", reason,
		"replicated", s.raftNode != nil,
	)

	return &schedulerv1.PullTaskResponse{
		HasTask: true,
		Assignment: &schedulerv1.TaskAssignment{
			TaskId:             leasedTask.ID,
			DagId:              leasedTask.DAGID,
			TenantId:           leasedTask.TenantID,
			Priority:           leasedTask.Priority,
			LeaseEpoch:         leasedTask.LeaseEpoch,
			LeaseDurationMs:    leasedTask.LeaseDuration.Milliseconds(),
			ExecutionTimeoutMs: leasedTask.Timeout.Milliseconds(),
			PayloadType:        leasedTask.PayloadType,
			Payload:            leasedTask.Payload,
			SessionId:          leasedTask.AssignedSessionID,
		},
	}, nil
}

func (s *Server) AckTaskRunning(ctx context.Context, req *schedulerv1.AckTaskRunningRequest) (*schedulerv1.AckTaskRunningResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	ctx, span := telemetry.StartSpan(ctx, "coordinator.AckTaskRunning",
		trace.WithAttributes(
			telemetry.AttrTaskID.String(req.GetTaskId()),
			telemetry.AttrWorkerID.String(req.GetWorkerId()),
			telemetry.AttrLeaseEpoch.Int64(int64(req.GetLeaseEpoch())),
			telemetry.AttrNodeID.String(s.cfg.NodeID),
		),
	)
	defer span.End()

	now := time.Now()
	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:         consensus.OpAckTaskRunning,
			TaskID:     req.GetTaskId(),
			WorkerID:   req.GetWorkerId(),
			SessionID:  req.GetSessionId(),
			LeaseEpoch: req.GetLeaseEpoch(),
			Timestamp:  now,
		}
		if _, err := s.raftNode.Apply(cmd, 3*time.Second); err != nil {
			if s.metrics != nil {
				s.metrics.StaleCommandRejectionsTotal.WithLabelValues(s.cfg.NodeID, "stale_ack").Inc()
			}
			return nil, s.mapDomainErrorToGRPC(err)
		}
	} else {
		if err := s.store.MarkTaskRunning(req.GetTaskId(), req.GetWorkerId(), req.GetSessionId(), req.GetLeaseEpoch()); err != nil {
			if s.metrics != nil {
				s.metrics.StaleCommandRejectionsTotal.WithLabelValues(s.cfg.NodeID, "stale_ack").Inc()
			}
			return nil, s.mapDomainErrorToGRPC(err)
		}
	}

	s.logger.Info("task execution acked",
		"task_id", req.GetTaskId(),
		"worker_id", req.GetWorkerId(),
		"session_id", req.GetSessionId(),
		"lease_epoch", req.GetLeaseEpoch(),
	)
	_ = s.store.UpdateWorkerHeartbeat(req.GetWorkerId(), req.GetSessionId())

	return &schedulerv1.AckTaskRunningResponse{Acknowledged: true}, nil
}

func (s *Server) ReportTaskCompleted(ctx context.Context, req *schedulerv1.ReportTaskCompletedRequest) (*schedulerv1.ReportTaskCompletedResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	ctx, span := telemetry.StartSpan(ctx, "coordinator.ReportTaskCompleted",
		trace.WithAttributes(
			telemetry.AttrTaskID.String(req.GetTaskId()),
			telemetry.AttrWorkerID.String(req.GetWorkerId()),
			telemetry.AttrLeaseEpoch.Int64(int64(req.GetLeaseEpoch())),
			telemetry.AttrNodeID.String(s.cfg.NodeID),
		),
	)
	defer span.End()

	now := time.Now()
	var completedTask *domain.Task

	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:         consensus.OpCompleteTask,
			TaskID:     req.GetTaskId(),
			WorkerID:   req.GetWorkerId(),
			SessionID:  req.GetSessionId(),
			LeaseEpoch: req.GetLeaseEpoch(),
			Result:     req.GetResult(),
			Timestamp:  now,
		}
		res, err := s.raftNode.Apply(cmd, 3*time.Second)
		if err != nil {
			if s.metrics != nil {
				s.metrics.StaleCommandRejectionsTotal.WithLabelValues(s.cfg.NodeID, "stale_completion").Inc()
			}
			return nil, s.mapDomainErrorToGRPC(err)
		}
		completedTask = res.(*domain.Task)
	} else {
		if err := s.store.CompleteTask(req.GetTaskId(), req.GetWorkerId(), req.GetSessionId(), req.GetLeaseEpoch(), req.GetResult()); err != nil {
			if s.metrics != nil {
				s.metrics.StaleCommandRejectionsTotal.WithLabelValues(s.cfg.NodeID, "stale_completion").Inc()
			}
			return nil, s.mapDomainErrorToGRPC(err)
		}
		completedTask, _ = s.store.GetTask(req.GetTaskId())
	}

	tenantID := "default"
	if completedTask != nil {
		tenantID = completedTask.TenantID
		if completedTask.StartedAt != nil && s.metrics != nil {
			s.metrics.TaskExecutionDuration.WithLabelValues(tenantID).Observe(time.Since(*completedTask.StartedAt).Seconds())
		}
	}
	if s.metrics != nil {
		s.metrics.TasksCompletedTotal.WithLabelValues(tenantID).Inc()
	}

	s.logger.Info("task completed successfully",
		"task_id", req.GetTaskId(),
		"worker_id", req.GetWorkerId(),
		"session_id", req.GetSessionId(),
		"lease_epoch", req.GetLeaseEpoch(),
	)

	// Trigger DAG dependency evaluation
	if completedTask != nil && completedTask.DAGID != "" {
		readyTaskIDs, _ := s.dagEngine.OnTaskCompleted(ctx, completedTask)
		if s.metrics != nil && len(readyTaskIDs) > 0 {
			s.metrics.DAGDependencyPromotions.WithLabelValues(tenantID).Add(float64(len(readyTaskIDs)))
		}
		for _, rtID := range readyTaskIDs {
			if s.raftNode != nil {
				cmdReady := &consensus.Command{
					Op:        consensus.OpMarkTaskReady,
					TaskID:    rtID,
					Timestamp: time.Now(),
				}
				_, _ = s.raftNode.Apply(cmdReady, 3*time.Second)
			}
		}
	}

	return &schedulerv1.ReportTaskCompletedResponse{Acknowledged: true}, nil
}

func (s *Server) ReportTaskFailed(ctx context.Context, req *schedulerv1.ReportTaskFailedRequest) (*schedulerv1.ReportTaskFailedResponse, error) {
	if err := s.ensureLeader(); err != nil {
		return nil, err
	}

	ctx, span := telemetry.StartSpan(ctx, "coordinator.ReportTaskFailed",
		trace.WithAttributes(
			telemetry.AttrTaskID.String(req.GetTaskId()),
			telemetry.AttrWorkerID.String(req.GetWorkerId()),
			telemetry.AttrLeaseEpoch.Int64(int64(req.GetLeaseEpoch())),
			telemetry.AttrNodeID.String(s.cfg.NodeID),
		),
	)
	defer span.End()

	task, err := s.store.GetTask(req.GetTaskId())
	if err != nil {
		return nil, status.Error(codes.NotFound, "task not found")
	}

	willRetry := req.GetRetryable() && (task.AttemptCount < task.MaxRetries)
	targetState := domain.StateFailed
	var retryDelay time.Duration
	if willRetry {
		targetState = domain.StateRetryWait
		retryDelay = s.calculateBackoff(task.AttemptCount)
	}

	if s.raftNode != nil {
		cmd := &consensus.Command{
			Op:           consensus.OpFailTask,
			TaskID:       req.GetTaskId(),
			WorkerID:     req.GetWorkerId(),
			SessionID:    req.GetSessionId(),
			LeaseEpoch:   req.GetLeaseEpoch(),
			ErrorMessage: req.GetErrorMessage(),
			NextState:    targetState,
			RetryDelay:   retryDelay,
			Timestamp:    time.Now(),
		}
		if _, err := s.raftNode.Apply(cmd, 3*time.Second); err != nil {
			return nil, s.mapDomainErrorToGRPC(err)
		}
	} else {
		_, err = s.store.FailTask(req.GetTaskId(), req.GetWorkerId(), req.GetSessionId(), req.GetLeaseEpoch(), req.GetErrorMessage(), req.GetRetryable())
		if err != nil {
			return nil, s.mapDomainErrorToGRPC(err)
		}
	}

	taskAfter, _ := s.store.GetTask(req.GetTaskId())
	remainingRetries := int32(0)
	if taskAfter != nil {
		remainingRetries = taskAfter.MaxRetries - taskAfter.AttemptCount
	}

	if s.metrics != nil {
		retryStr := "false"
		if willRetry {
			retryStr = "true"
			s.metrics.TasksRetriedTotal.WithLabelValues(task.TenantID).Inc()
		}
		s.metrics.TasksFailedTotal.WithLabelValues(task.TenantID, retryStr).Inc()
	}

	s.logger.Warn("task execution failed",
		"task_id", req.GetTaskId(),
		"worker_id", req.GetWorkerId(),
		"session_id", req.GetSessionId(),
		"will_retry", willRetry,
		"remaining_retries", remainingRetries,
		"error", req.GetErrorMessage(),
	)

	// If retryable, schedule timer on leader
	if willRetry && taskAfter != nil {
		s.scheduleRetryTimer(taskAfter.ID, retryDelay)
	} else if !willRetry && taskAfter != nil && taskAfter.DAGID != "" {
		// Terminal failure: propagate cancellation to downstream dependents
		cancelledTaskIDs, _ := s.dagEngine.OnTaskFailed(ctx, taskAfter)
		for _, ctID := range cancelledTaskIDs {
			if s.raftNode != nil {
				cmdCancel := &consensus.Command{
					Op:        consensus.OpCancelTask,
					TaskID:    ctID,
					Reason:    fmt.Sprintf("upstream %s failed", taskAfter.ID),
					Timestamp: time.Now(),
				}
				_, _ = s.raftNode.Apply(cmdCancel, 3*time.Second)
			}
		}
	}

	return &schedulerv1.ReportTaskFailedResponse{
		Acknowledged:     true,
		WillRetry:        willRetry,
		RemainingRetries: remainingRetries,
	}, nil
}

func (s *Server) scheduleRetryTimer(taskID string, delay time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()

	timer := time.AfterFunc(delay, func() {
		s.mu.Lock()
		delete(s.retryTimers, taskID)
		s.mu.Unlock()

		if s.raftNode != nil && !s.raftNode.IsLeader() {
			return
		}

		s.logger.Info("retry backoff elapsed; promoting task to READY", "task_id", taskID)
		if s.raftNode != nil {
			cmd := &consensus.Command{
				Op:        consensus.OpMarkTaskReady,
				TaskID:    taskID,
				Timestamp: time.Now(),
			}
			_, _ = s.raftNode.Apply(cmd, 3*time.Second)
		} else {
			_, _ = s.store.MarkTaskReady(taskID)
		}
	})

	s.retryTimers[taskID] = timer
}

func (s *Server) calculateBackoff(attemptCount int32) time.Duration {
	baseBackoff := 200.0 * math.Pow(2.0, float64(attemptCount-1))
	if baseBackoff > 10000.0 {
		baseBackoff = 10000.0
	}
	jitter := baseBackoff * 0.2 * rand.Float64()
	return time.Duration(baseBackoff+jitter) * time.Millisecond
}

// runLeaseReaper periodically checks for dead workers, reclaims expired tasks, and recovers retry timers.
func (s *Server) runLeaseReaper() {
	ticker := time.NewTicker(s.cfg.ReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
			if s.raftNode != nil && !s.raftNode.IsLeader() {
				return
			}
			s.SweepExpiredLeases()
			s.recoverRetryTimers()
		}
	}
}

// SweepExpiredLeases sweeps registered workers and reclaims tasks from dead workers.
func (s *Server) SweepExpiredLeases() {
	if s.raftNode != nil && !s.raftNode.IsLeader() {
		return
	}

	workers := s.store.GetActiveWorkers()
	now := time.Now()
	timeoutThreshold := s.cfg.WorkerLeaseDur + s.cfg.LeaseGraceWindow

	for _, w := range workers {
		lastHb, ok := s.store.GetWorkerLastHeartbeat(w.ID)
		if !ok || now.Sub(lastHb) > timeoutThreshold {
			s.logger.Warn("worker lease expired; marking DEAD and reclaiming tasks",
				"worker_id", w.ID,
				"session_id", w.SessionID,
				"elapsed", now.Sub(lastHb),
			)

			if s.raftNode != nil {
				cmdExpire := &consensus.Command{
					Op:        consensus.OpExpireWorker,
					WorkerID:  w.ID,
					Timestamp: time.Now(),
				}
				_, _ = s.raftNode.Apply(cmdExpire, 3*time.Second)
			} else {
				_ = s.store.MarkWorkerDead(w.ID)
			}

			if s.metrics != nil {
				s.metrics.WorkerLeaseExpirations.WithLabelValues(s.cfg.NodeID).Inc()
				s.metrics.WorkerActive.WithLabelValues(s.cfg.NodeID).Set(float64(len(s.store.GetActiveWorkers())))
			}

			// Reclaim active tasks
			for _, task := range s.store.GetActiveTasksByWorker(w.ID) {
				willRetry := (task.AttemptCount < task.MaxRetries)
				nextState := domain.StateReady
				if !willRetry {
					nextState = domain.StateFailed
				}

				if s.raftNode != nil {
					cmdReclaim := &consensus.Command{
						Op:        consensus.OpReclaimTask,
						TaskID:    task.ID,
						NextState: nextState,
						Reason:    "worker lease expired",
						Timestamp: time.Now(),
					}
					_, _ = s.raftNode.Apply(cmdReclaim, 3*time.Second)
				} else {
					resultingState, err := s.store.ReclaimTask(task.ID)
					if err == nil && resultingState == domain.StateFailed && task.DAGID != "" {
						_, _ = s.dagEngine.OnTaskFailed(context.Background(), task)
					}
				}

				if s.metrics != nil {
					s.metrics.TasksReclaimedTotal.WithLabelValues(task.TenantID, "lease_expired").Inc()
					s.metrics.TaskRecoveryLatency.WithLabelValues("worker_timeout").Observe(time.Since(now).Seconds())
				}
			}
		}
	}
}

func (s *Server) mapDomainErrorToGRPC(err error) error {
	if errors.Is(err, domain.ErrStaleWorkerSession) {
		return status.Error(codes.FailedPrecondition, "stale worker session: operation rejected")
	}
	if errors.Is(err, domain.ErrStaleLeaseEpoch) {
		return status.Error(codes.FailedPrecondition, "stale lease epoch: operation rejected")
	}
	if errors.Is(err, domain.ErrWorkerMismatch) {
		return status.Error(codes.PermissionDenied, "worker does not hold lease")
	}
	if errors.Is(err, domain.ErrInvalidTransition) {
		return status.Error(codes.FailedPrecondition, "invalid state transition")
	}
	if errors.Is(err, domain.ErrTaskNotFound) {
		return status.Error(codes.NotFound, "task not found")
	}
	if errors.Is(err, domain.ErrTerminalState) {
		return status.Error(codes.FailedPrecondition, "task is in terminal state and cannot be mutated")
	}
	return status.Errorf(codes.Internal, "operation failed: %v", err)
}

func mapDomainStateToProto(state domain.State) schedulerv1.TaskState {
	switch state {
	case domain.StatePending:
		return schedulerv1.TaskState_TASK_STATE_PENDING
	case domain.StateBlocked:
		return schedulerv1.TaskState_TASK_STATE_BLOCKED
	case domain.StateReady:
		return schedulerv1.TaskState_TASK_STATE_READY
	case domain.StateLeased:
		return schedulerv1.TaskState_TASK_STATE_LEASED
	case domain.StateRunning:
		return schedulerv1.TaskState_TASK_STATE_RUNNING
	case domain.StateSucceeded:
		return schedulerv1.TaskState_TASK_STATE_SUCCEEDED
	case domain.StateFailed:
		return schedulerv1.TaskState_TASK_STATE_FAILED
	case domain.StateRetryWait:
		return schedulerv1.TaskState_TASK_STATE_RETRY_WAIT
	case domain.StateCancelled:
		return schedulerv1.TaskState_TASK_STATE_CANCELLED
	case domain.StateLost:
		return schedulerv1.TaskState_TASK_STATE_LOST
	case domain.StateRecovering:
		return schedulerv1.TaskState_TASK_STATE_RECOVERING
	default:
		return schedulerv1.TaskState_TASK_STATE_UNSPECIFIED
	}
}
