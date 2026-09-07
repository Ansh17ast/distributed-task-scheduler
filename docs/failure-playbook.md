# Operational Failure Playbook

This playbook provides actionable incident response, diagnostic procedures, and recovery runbooks for distributed failure scenarios in the distributed scheduler.

---

## Playbook Index
1. [Leader Crash & Failover Stall](#1-leader-crash--failover-stall)
2. [Worker Disappearance & Lease Expiration](#2-worker-disappearance--lease-expiration)
3. [Network Partition & Split-Brain Mitigation](#3-network-partition--split-brain-mitigation)
4. [Zombie Worker Reactivation & Fencing Failures](#4-zombie-worker-reactivation--fencing-failures)
5. [Tenant Quota Exhaustion & Queue Growth](#5-tenant-quota-exhaustion--queue-growth)
6. [Retry Storms & Cascading Failures](#6-retry-storms--cascading-failures)
7. [Persistent State Corruption & Snapshot Restore](#7-persistent-state-corruption--snapshot-restore)

---

## 1. Leader Crash & Failover Stall

### Symptoms
- Clients receive `Unavailable` or `FailedPrecondition: not cluster leader`.
- Metrics: `count(raft_current_term{role="Leader"}) == 0` triggers `NoClusterLeader` alert.
- `/readyz` returns `503 Service Unavailable`.

### Root Cause
- The active coordinator leader process crashed, lost power, or experienced an unrecoverable network disconnection.
- Followers are unable to establish quorum (e.g., fewer than 2 nodes reachable in a 3-node cluster).

### Diagnostic Steps
1. Query `/status` on all 3 coordinator endpoints (`http://<coord-IP>:<HTTPPort>/status`):
   - Identify whether any node reports `"role": "Leader"`.
   - Check if current `term` has incremented across followers.
2. Inspect log outputs on followers:
   ```bash
   grep -E "election|leadership|heartbeat" /var/log/coordinator.log
   ```
3. Check network reachability between coordinator nodes on `RaftBindAddr`.

### Mitigation Runbook
1. **If 2 of 3 nodes are online**:
   - The cluster should automatically elect a new leader within 150–300 ms.
   - Wait 4 seconds for the failover reconciliation window to conclude.
   - Restart the crashed node; it will automatically rejoin as a follower and synchronize its BoltDB log.
2. **If fewer than 2 nodes are online (Quorum Loss)**:
   - System correctly halts all state mutations to prevent split-brain.
   - Immediately restart at least one failed coordinator host.
   - Once quorum (2/3) is restored, leadership is elected automatically within 300 ms.

---

## 2. Worker Disappearance & Lease Expiration

### Symptoms
- Tasks remain in `RUNNING` or `LEASED` state past their expected timeout.
- Worker process crashes or disconnects without sending an error report.

### Automated Recovery Mechanism
1. The coordinator leader tracks worker health via monotonic timestamps (`WorkerHeartbeats`).
2. The background lease reaper runs every `ReaperInterval` (default 1s).
3. If `Now - LastHeartbeat > WorkerLeaseDur + LeaseGraceWindow`:
   - Leader proposes `OpExpireWorker`, marking worker `DEAD`.
   - Active task leases held by that worker are reclaimed via `OpReclaimTask`.
   - Tasks with remaining retries transition from `LEASED/RUNNING` $\rightarrow$ `LOST` $\rightarrow$ `RECOVERING` $\rightarrow$ `READY`.
   - Tasks that have exhausted retries transition to `FAILED`.

### Diagnostic Steps
1. Check Prometheus metric `worker_lease_expirations_total` and `tasks_reclaimed_total`.
2. Inspect coordinator logs:
   ```bash
   grep "worker lease expired" /var/log/coordinator.log
   ```
3. Verify worker host health and system resources (OOM kills, network disconnects).

### Manual Intervention
- If workers are frequently timing out due to heavy compute workloads rather than crashes, increase `WorkerLeaseDur` (e.g., from 10s to 30s) or reduce task batch concurrency.

---

## 3. Network Partition & Split-Brain Mitigation

### Symptoms
- Coordinator node isolated from the other two.
- Submissions to the partitioned minority coordinator fail with `Unavailable` or timeouts.

### Invariant Verification (Minority vs Majority)
- **Minority Partition (1 node isolated)**:
  - The isolated node cannot acquire Raft quorum ($1/3 < 2$).
  - Any task submission or lease mutation is rejected.
  - Linearizable reads via `verifyLinearizableRead()` are rejected because heartbeats fail.
- **Majority Partition (2 nodes connected)**:
  - Elects or retains leader ($2/3 \ge 2$).
  - Operates normally with zero data loss.

### Partition Healing Runbook
1. Restore network connectivity between coordinator subnets.
2. The isolated node automatically receives `AppendEntries` from the majority leader.
3. Node synchronizes its applied log index and transitions to a healthy follower.
4. Metric `node_convergence_latency_seconds` observes healing convergence time ($< 500\text{ ms}$).

---

## 4. Zombie Worker Reactivation & Fencing Failures

### Symptoms
- A worker presumed dead (due to network freeze or GC pause) suddenly wakes up and attempts to ack, complete, or fail a task that has already been reassigned.
- Log message: `stale lease epoch: operation rejected` or `stale worker session: operation rejected`.
- Metric `stale_command_rejections_total` increments.

### Defense Mechanism
The scheduler enforces strict dual-layer fencing:
1. **SessionID Check**: If the worker crashed and restarted, its newly generated UUID session differs from the task's assigned session $\rightarrow$ Rejected.
2. **LeaseEpoch Check**: When a lease is reclaimed and re-assigned to a new worker, `LeaseEpoch` increments ($E+1$). The zombie worker holds epoch $E$ $\rightarrow$ Rejected with `FailedPrecondition`.

### Verification
- Check Prometheus metric:
  ```promql
  rate(stale_command_rejections_total[5m])
  ```
- If this metric spikes, identify whether a specific worker host has severe CPU starvation or disk I/O pauses causing delayed responses.

---

## 5. Tenant Quota Exhaustion & Queue Growth

### Symptoms
- Clients receive `ResourceExhausted: tenant queue limit exceeded`.
- Prometheus metric `scheduler_rejected_tasks_total{reason="quota_exceeded"}` increments.
- Queue depth exceeds SLAs for a particular tenant.

### Mitigation Runbook
1. Query `/metrics` to identify the overflowing tenant:
   ```promql
   scheduler_queue_depth{tenant_id="<tenant>"}
   ```
2. Check tenant quota limits in the coordinator state store.
3. If the workload is legitimate, dynamically adjust tenant quota via Raft without downtime:
   - Issue `OpSetTenantQuota` command with increased `MaxQueueDepth` or `MaxConcurrency`.
4. Scale up worker daemons to increase drain throughput.

---

## 6. Retry Storms & Cascading Failures

### Symptoms
- High failure rates across downstream dependencies or third-party databases causing a surge of task retries.
- High values of `tasks_retried_total`.

### Built-in Protection
- Exponential backoff:
  $$T_{\text{backoff}} = \min\left(200 \times 2^{\text{attempt}-1},\, 10000\right)\text{ ms} + \text{jitter}$$
- A 20% random jitter prevents synchronized waves of retries from hitting downstream systems simultaneously.

### Remediation
1. If an external service is down, cancel the parent DAG or task batch via `CancelTask` or `CancelDAG`.
2. Cancellation automatically propagates downstream through the DAG engine, setting all dependent tasks to `CANCELLED` and halting retry timers.

---

## 7. Persistent State Corruption & Snapshot Restore

### Symptoms
- Coordinator fails to boot with BoltDB file corruption errors.
- Unclean disk unmount or host hardware failure.

### Restore Procedure
1. Stop the damaged coordinator node.
2. If other cluster nodes are healthy, delete the corrupt `data/raft-<node>/` directory.
3. Restart the coordinator as an empty node with the same `NodeID` and peer configuration.
4. The active leader will detect that the node has an empty log, stream the latest state machine snapshot over Raft, and replay delta logs automatically.
5. Verify restoration using `/status` to ensure `applied_index` matches the cluster leader.
