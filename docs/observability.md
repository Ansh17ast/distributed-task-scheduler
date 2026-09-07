# Observability Architecture & Metrics Inventory

This document defines the production observability architecture of the distributed scheduler, including Prometheus metrics, OpenTelemetry distributed tracing, structured JSON logging schemas, and alerting specifications.

---

## 1. Observability Design Principles

1. **Strict Low-Cardinality Metric Labels**:
   - High-cardinality values (`task_id`, `dag_id`, `worker_id`, `session_id`, `error_message`) are **NEVER** permitted as Prometheus metric labels to prevent exponential time-series explosion and Prometheus OOMs.
   - Metric labels are strictly bounded to small, fixed sets: `node_id`, `tenant_id`, `op`, `status`, `reason`, `policy`, and `retryable`.
2. **Correlation ID Isolation via Tracing & Logs**:
   - Fine-grained entity correlation belongs exclusively in **OpenTelemetry trace spans** and **structured `log/slog` entries**, where cardinality is unbounded by design.
3. **Defense-in-Depth Health Checking**:
   - Multi-tiered HTTP operational endpoints (`/healthz`, `/readyz`, `/status`, `/metrics`) decouple liveness probes from consensus quorum health.

---

## 2. Complete Prometheus Metrics Inventory

All metrics are exposed at `GET /metrics` in standard OpenMetrics format.

### Raft Consensus Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `raft_leader_changes_total` | Counter | `node_id` | Total count of Raft leadership transition events observed by this node. |
| `raft_current_term` | Gauge | `node_id` | Current consensus term of the Raft cluster as observed by this node. |
| `raft_commit_index` | Gauge | `node_id` | Current commit index of the Raft log. |
| `raft_applied_index` | Gauge | `node_id` | Current index applied to the deterministic state machine. |
| `raft_commit_latency_seconds` | Histogram | `node_id`, `op` | Duration of Raft quorum proposal commits in seconds (Buckets: 0.1ms to 5s). |
| `raft_replication_lag` | Gauge | `node_id` | Difference between the last log index and applied index. |
| `raft_commands_total` | Counter | `node_id`, `op`, `status` | Total commands proposed through Raft consensus (`status`: `success` \| `failure`). |

### Scheduler Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `scheduler_decision_latency_seconds` | Histogram | `policy` | Time taken by `ReadyQueue.SelectNextTask` to evaluate candidates (Buckets: 0.1ms to 5s). |
| `scheduler_dispatch_total` | Counter | `tenant_id`, `status` | Total tasks dispatched to workers by the scheduler. |
| `scheduler_queue_depth` | Gauge | `tenant_id` | Current count of tasks in READY state awaiting worker assignment. |
| `scheduler_ready_tasks` | Gauge | `tenant_id` | Current number of tasks in READY state across the store. |
| `scheduler_running_tasks` | Gauge | `tenant_id` | Current number of tasks in RUNNING state across the store. |
| `scheduler_rejected_tasks_total` | Counter | `tenant_id`, `reason` | Total submissions rejected due to backpressure or quota limits. |

### Worker & Lease Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `worker_registered_total` | Counter | `node_id` | Total worker registration events. |
| `worker_heartbeat_total` | Counter | `node_id` | Total worker heartbeat events received. |
| `worker_active` | Gauge | `node_id` | Current number of active, healthy workers tracked by the coordinator. |
| `worker_available_slots` | Gauge | `node_id` | Total available execution slots across active workers. |
| `worker_lease_expirations_total` | Counter | `node_id` | Total worker lease expiration events detected by the background reaper. |
| `worker_reconnections_total` | Counter | `node_id` | Total worker reconnection / adoption events. |

### Task Lifecycle Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `tasks_submitted_total` | Counter | `tenant_id` | Total tasks admitted to the cluster. |
| `tasks_completed_total` | Counter | `tenant_id` | Total tasks successfully executed to terminal `SUCCEEDED` state. |
| `tasks_failed_total` | Counter | `tenant_id`, `retryable` | Total task failures recorded (`retryable`: `true` \| `false`). |
| `tasks_retried_total` | Counter | `tenant_id` | Total task retry backoff timers scheduled. |
| `tasks_reclaimed_total` | Counter | `tenant_id`, `reason` | Total task leases reclaimed after worker timeout or disappearance. |
| `task_execution_duration_seconds` | Histogram | `tenant_id` | Execution duration from `RUNNING` to terminal state (Buckets: 0.1ms to 5s). |
| `task_queue_wait_duration_seconds` | Histogram | `tenant_id` | Duration tasks wait in `READY` queue prior to worker dispatch. |

### DAG Workflow Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `dag_submissions_total` | Counter | `tenant_id` | Total DAG workflows submitted. |
| `dag_validation_latency_seconds`| Histogram | `tenant_id` | Time taken to validate DAG acyclicity and dependencies. |
| `dag_cycle_rejections_total` | Counter | `tenant_id` | Total DAGs rejected due to cycle detection. |
| `dag_dependency_promotions_total`| Counter | `tenant_id` | Total downstream task readiness promotions upon dependency completion. |

### Chaos, Recovery & Fencing Metrics

| Metric Name | Type | Labels | Description |
| :--- | :--- | :--- | :--- |
| `leader_failovers_total` | Counter | `node_id` | Total failover events where a new leader assumed cluster authority. |
| `stale_command_rejections_total` | Counter | `node_id`, `reason` | Total stale or zombie commands blocked by `(SessionID, LeaseEpoch)` fencing. |
| `partition_events_total` | Counter | `event_type` | Total network partition events injected or detected. |
| `task_recovery_latency_seconds` | Histogram | `reason` | Time elapsed between worker lease expiration and task re-dispatch. |
| `node_convergence_latency_seconds`| Histogram| `node_id` | Time taken for a rejoined follower to synchronize with current leader index. |

---

## 3. OpenTelemetry Distributed Tracing

Tracing is instrumented with `go.opentelemetry.io/otel` using Tracer `distributed-scheduler/telemetry`.

### Standard Trace Span Hierarchy
```
coordinator.SubmitTask [Span]
 ├── Attr: task.id = "task-1234"
 ├── Attr: tenant.id = "tenant-alpha"
 ├── Attr: node.id = "coord-1"
 └── raft.Apply (Proposal & Consensus Quorum Replication)

coordinator.PullTask [Span]
 ├── Attr: worker.id = "worker-host-1"
 ├── Attr: session.id = "b24479ca-18de-4475-bf7d-ecbcfbbfb8a8"
 └── scheduler.SelectNextTask (Priority Queue Evaluation)

coordinator.AckTaskRunning [Span]
 ├── Attr: task.id = "task-1234"
 ├── Attr: worker.id = "worker-host-1"
 └── Attr: lease.epoch = 1

coordinator.ReportTaskCompleted [Span]
 ├── Attr: task.id = "task-1234"
 ├── Attr: worker.id = "worker-host-1"
 └── Attr: lease.epoch = 1
```

### Semantic Trace Attributes

| Attribute Name | Constant | Description |
| :--- | :--- | :--- |
| `task.id` | `AttrTaskID` | Unique task correlation identifier. |
| `tenant.id` | `AttrTenantID` | Submitting tenant identifier. |
| `dag.id` | `AttrDAGID` | Parent DAG workflow identifier. |
| `worker.id` | `AttrWorkerID` | Assigned worker daemon host identifier. |
| `session.id` | `AttrSessionID` | Ephemeral worker process incarnation UUID. |
| `lease.epoch` | `AttrLeaseEpoch` | Monotonically increasing lease incarnation counter. |
| `raft.term` | `AttrRaftTerm` | Consensus election term. |
| `raft.index` | `AttrRaftIndex` | Replicated log entry index. |
| `node.id` | `AttrNodeID` | Coordinator node processing the request. |

---

## 4. Structured Operational Logging Schema

All system logs use Go's standard library `log/slog` emitting JSON objects to stdout:

```json
{
  "time": "2026-09-08T04:31:56.120Z",
  "level": "INFO",
  "msg": "task dispatched to worker",
  "task_id": "task-88912",
  "worker_id": "worker-pool-3",
  "session_id": "047965b3-a7c5-40c9-8919-b434777ebc2f",
  "lease_epoch": 1,
  "reason": "highest_priority",
  "replicated": true
}
```

```json
{
  "time": "2026-09-08T04:31:57.402Z",
  "level": "WARN",
  "msg": "worker lease expired; marking DEAD and reclaiming tasks",
  "worker_id": "worker-node-7",
  "session_id": "f83a21bc-341e-4589-9a76-e134608c2310",
  "elapsed": "12.304s"
}
```

---

## 5. Recommended Prometheus Alerting Rules

```yaml
groups:
  - name: distributed_scheduler_alerts
    rules:
      - alert: NoClusterLeader
        expr: count(raft_current_term{role="Leader"}) == 0
        for: 5s
        labels:
          severity: critical
        annotations:
          summary: "No coordinator leader detected in the cluster"

      - alert: HighTaskQueueWaitLatency
        expr: histogram_quantile(0.95, sum(rate(task_queue_wait_duration_seconds_bucket[5m])) by (le)) > 2.0
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "95th percentile task queue wait time exceeds 2 seconds (worker shortage)"

      - alert: HighStaleCommandRate
        expr: rate(stale_command_rejections_total[2m]) > 5
        labels:
          severity: warning
        annotations:
          summary: "Abnormal rate of stale worker commands rejected by fencing"

      - alert: TenantQueueExhaustion
        expr: rate(scheduler_rejected_tasks_total[5m]) > 10
        labels:
          severity: warning
        annotations:
          summary: "Tasks are being rejected due to tenant queue limit backpressure"
```
