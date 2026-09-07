package telemetry

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Metrics encapsulates all Prometheus collectors across Raft, Scheduler, Workers, Tasks, DAGs, and Recovery.
type Metrics struct {
	Registry *prometheus.Registry

	// RAFT METRICS
	RaftLeaderChanges  *prometheus.CounterVec
	RaftCurrentTerm    *prometheus.GaugeVec
	RaftCommitIndex    *prometheus.GaugeVec
	RaftAppliedIndex   *prometheus.GaugeVec
	RaftCommitLatency  *prometheus.HistogramVec
	RaftReplicationLag *prometheus.GaugeVec
	RaftCommandsTotal  *prometheus.CounterVec

	// SCHEDULER METRICS
	SchedulerDecisionLatency *prometheus.HistogramVec
	SchedulerDispatchTotal   *prometheus.CounterVec
	SchedulerQueueDepth      *prometheus.GaugeVec
	SchedulerReadyTasks      *prometheus.GaugeVec
	SchedulerRunningTasks    *prometheus.GaugeVec
	SchedulerRejectedTasks   *prometheus.CounterVec

	// WORKER METRICS
	WorkerRegisteredTotal    *prometheus.CounterVec
	WorkerHeartbeatTotal     *prometheus.CounterVec
	WorkerActive             *prometheus.GaugeVec
	WorkerAvailableSlots     *prometheus.GaugeVec
	WorkerLeaseExpirations   *prometheus.CounterVec
	WorkerReconnectionsTotal *prometheus.CounterVec

	// TASK METRICS
	TasksSubmittedTotal   *prometheus.CounterVec
	TasksCompletedTotal   *prometheus.CounterVec
	TasksFailedTotal      *prometheus.CounterVec
	TasksRetriedTotal     *prometheus.CounterVec
	TasksReclaimedTotal   *prometheus.CounterVec
	TaskExecutionDuration *prometheus.HistogramVec
	TaskQueueWaitDuration *prometheus.HistogramVec

	// DAG METRICS
	DAGSubmissionsTotal     *prometheus.CounterVec
	DAGValidationLatency    *prometheus.HistogramVec
	DAGCycleRejectionsTotal *prometheus.CounterVec
	DAGDependencyPromotions *prometheus.CounterVec

	// CHAOS / RECOVERY METRICS
	LeaderFailoversTotal        *prometheus.CounterVec
	StaleCommandRejectionsTotal *prometheus.CounterVec
	PartitionEventsTotal        *prometheus.CounterVec
	TaskRecoveryLatency         *prometheus.HistogramVec
	NodeConvergenceLatency      *prometheus.HistogramVec
}

// NewMetrics initializes and registers all Prometheus metric collectors with a dedicated registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	latencyBuckets := []float64{0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0}

	m := &Metrics{
		Registry: reg,

		// Raft
		RaftLeaderChanges: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "raft_leader_changes_total",
			Help: "Total count of Raft leadership changes observed by this node.",
		}, []string{"node_id"}),

		RaftCurrentTerm: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_current_term",
			Help: "Current consensus term of the Raft cluster as observed by this node.",
		}, []string{"node_id"}),

		RaftCommitIndex: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_commit_index",
			Help: "Current commit index of the Raft log.",
		}, []string{"node_id"}),

		RaftAppliedIndex: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_applied_index",
			Help: "Current index applied to the deterministic state machine.",
		}, []string{"node_id"}),

		RaftCommitLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "raft_commit_latency_seconds",
			Help:    "Duration of Raft quorum proposal commits in seconds.",
			Buckets: latencyBuckets,
		}, []string{"node_id", "op"}),

		RaftReplicationLag: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "raft_replication_lag",
			Help: "Difference between last log index and applied index.",
		}, []string{"node_id"}),

		RaftCommandsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "raft_commands_total",
			Help: "Total commands proposed through Raft consensus.",
		}, []string{"node_id", "op", "status"}),

		// Scheduler
		SchedulerDecisionLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "scheduler_decision_latency_seconds",
			Help:    "Time taken to evaluate candidate task dispatches.",
			Buckets: latencyBuckets,
		}, []string{"policy"}),

		SchedulerDispatchTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_dispatch_total",
			Help: "Total tasks dispatched to workers by the scheduler.",
		}, []string{"tenant_id", "status"}),

		SchedulerQueueDepth: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_queue_depth",
			Help: "Current depth of ready queues per tenant.",
		}, []string{"tenant_id"}),

		SchedulerReadyTasks: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_ready_tasks",
			Help: "Current number of tasks in READY state.",
		}, []string{"tenant_id"}),

		SchedulerRunningTasks: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "scheduler_running_tasks",
			Help: "Current number of tasks in RUNNING state.",
		}, []string{"tenant_id"}),

		SchedulerRejectedTasks: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "scheduler_rejected_tasks_total",
			Help: "Total task submissions rejected due to backpressure or quota limits.",
		}, []string{"tenant_id", "reason"}),

		// Workers
		WorkerRegisteredTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_registered_total",
			Help: "Total worker registration events.",
		}, []string{"node_id"}),

		WorkerHeartbeatTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_heartbeat_total",
			Help: "Total worker heartbeat events received.",
		}, []string{"node_id"}),

		WorkerActive: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "worker_active",
			Help: "Current number of active healthy workers.",
		}, []string{"node_id"}),

		WorkerAvailableSlots: factory.NewGaugeVec(prometheus.GaugeOpts{
			Name: "worker_available_slots",
			Help: "Total available execution slots across active workers.",
		}, []string{"node_id"}),

		WorkerLeaseExpirations: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_lease_expirations_total",
			Help: "Total worker lease expiration events detected by reaper.",
		}, []string{"node_id"}),

		WorkerReconnectionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "worker_reconnections_total",
			Help: "Total worker reconnect events.",
		}, []string{"node_id"}),

		// Tasks
		TasksSubmittedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_submitted_total",
			Help: "Total tasks submitted to the cluster.",
		}, []string{"tenant_id"}),

		TasksCompletedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_completed_total",
			Help: "Total tasks successfully completed.",
		}, []string{"tenant_id"}),

		TasksFailedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_failed_total",
			Help: "Total task failures recorded.",
		}, []string{"tenant_id", "retryable"}),

		TasksRetriedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_retried_total",
			Help: "Total task retry attempts scheduled.",
		}, []string{"tenant_id"}),

		TasksReclaimedTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "tasks_reclaimed_total",
			Help: "Total task leases reclaimed after worker timeout.",
		}, []string{"tenant_id", "reason"}),

		TaskExecutionDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_execution_duration_seconds",
			Help:    "Execution time of tasks from RUNNING to terminal state.",
			Buckets: latencyBuckets,
		}, []string{"tenant_id"}),

		TaskQueueWaitDuration: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_queue_wait_duration_seconds",
			Help:    "Wait time of tasks in READY state prior to worker assignment.",
			Buckets: latencyBuckets,
		}, []string{"tenant_id"}),

		// DAG
		DAGSubmissionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dag_submissions_total",
			Help: "Total DAG workflows submitted.",
		}, []string{"tenant_id"}),

		DAGValidationLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "dag_validation_latency_seconds",
			Help:    "Time taken to validate DAG acyclicity and dependencies.",
			Buckets: latencyBuckets,
		}, []string{"tenant_id"}),

		DAGCycleRejectionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dag_cycle_rejections_total",
			Help: "Total DAGs rejected due to cycle detection.",
		}, []string{"tenant_id"}),

		DAGDependencyPromotions: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "dag_dependency_promotions_total",
			Help: "Total downstream task readiness promotions upon dependency completion.",
		}, []string{"tenant_id"}),

		// Chaos / Recovery
		LeaderFailoversTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "leader_failovers_total",
			Help: "Total failover events where a new leader was elected.",
		}, []string{"node_id"}),

		StaleCommandRejectionsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "stale_command_rejections_total",
			Help: "Total stale or zombie commands rejected by fencing mechanisms.",
		}, []string{"node_id", "reason"}),

		PartitionEventsTotal: factory.NewCounterVec(prometheus.CounterOpts{
			Name: "partition_events_total",
			Help: "Total network partition events injected or detected.",
		}, []string{"event_type"}),

		TaskRecoveryLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "task_recovery_latency_seconds",
			Help:    "Time elapsed between worker lease expiration and task re-dispatch.",
			Buckets: latencyBuckets,
		}, []string{"reason"}),

		NodeConvergenceLatency: factory.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "node_convergence_latency_seconds",
			Help:    "Time taken for a rejoined follower to synchronize with current leader index.",
			Buckets: latencyBuckets,
		}, []string{"node_id"}),
	}

	return m
}

// Global default metrics instance
var DefaultMetrics = NewMetrics()
