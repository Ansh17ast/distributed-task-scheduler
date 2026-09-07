# Operational Guide: Distributed Task Scheduler

This guide details deployment topology, operational lifecycles, configuration parameters, health checking, maintenance procedures, and disaster recovery for the distributed scheduler.

---

## 1. Cluster Architecture & Sizing

The distributed scheduler is structured around a replicated 3-coordinator consensus core with an elastic, horizontally scalable pool of worker daemons.

```
                  +--------------------------+
                  | External Client / API GW |
                  +-------------+------------+
                                |
                   (Linearizable gRPC)
                                v
               +----------------------------------+
               | Coordinator Leader (Authoritative)|
               |  - Raft Leader (Term N)          |
               |  - Authoritative State Machine   |
               |  - Worker Lease Reaper           |
               |  - In-Memory Ready Queue         |
               +--------+----------------+--------+
                        |                |
             Raft AppendEntries    Raft AppendEntries
                        v                v
      +----------------------+      +----------------------+
      | Coordinator Follower |      | Coordinator Follower |
      |  - Replicated FSM    |      |  - Replicated FSM    |
      |  - Read Forwarding   |      |  - Read Forwarding   |
      +----------------------+      +----------------------+
                        ^                ^
                        | (Worker-Pull)  |
               +--------+--------+-------+--------+
               |                 |                |
        +------+------+   +------+------+   +-----+-------+
        | Worker 1    |   | Worker 2    |   | Worker N    |
        | (Session A) |   | (Session B) |   | (Session C) |
        +-------------+   +-------------+   +-------------+
```

### Minimum Production Sizing
- **Coordinators**: 3 dedicated nodes (Quorum = 2). Odd node counts prevent split-brain while providing 1-node fault tolerance ($F = \lfloor (N-1)/2 \rfloor = 1$).
- **Worker Daemons**: 3 to 100+ nodes, auto-scaled based on queue depth.
- **CPU/Memory Baseline**:
  - Coordinator: 2 vCPU, 4 GB RAM, high-IOPS NVMe (BoltDB disk sync).
  - Worker: Scaled to workload payload requirements.

---

## 2. Configuration Reference

Configurations are loaded via `config.CoordinatorConfig` and `config.WorkerConfig`.

### Coordinator Configuration Options

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `NodeID` | `string` | *(Required)* | Unique cluster node identifier (e.g., `coord-1`). |
| `GRPCPort` | `int` | `50051` | Port bound for client and worker gRPC RPCs. |
| `HTTPPort` | `int` | `8080` | Port bound for `/healthz`, `/readyz`, `/status`, and `/metrics`. |
| `RaftBindAddr` | `string` | `127.0.0.1:7000` | Address used for inter-coordinator Raft transport. |
| `RaftDataDir` | `string` | `./data/raft` | Directory storing persistent BoltDB log, stable state, and snapshots. |
| `RaftPeers` | `[]string`| `[]` | List of peer nodes participating in consensus. |
| `RaftBootstrap`| `bool` | `false` | True exclusively on the initial cluster initiator node. |
| `RaftHeartbeat`| `Duration` | `50ms` | Leader heartbeat ping interval. |
| `RaftElectionMin` | `Duration` | `150ms` | Minimum randomized election timeout window. |
| `RaftElectionMax` | `Duration` | `300ms` | Maximum randomized election timeout window. |
| `WorkerLeaseDur` | `Duration` | `10s` | Duration of exclusive task execution lease granted to worker. |
| `LeaseGraceWindow`| `Duration`| `2s` | Buffer period before reaper reclaims lease from missing worker. |
| `FailoverReconcile` | `Duration` | `4s` | Reconciliation grace window after new leader election. |
| `ReaperInterval` | `Duration` | `1s` | Frequency of background worker lease expiration audit loop. |

### Worker Configuration Options

| Parameter | Type | Default | Description |
| :--- | :--- | :--- | :--- |
| `WorkerID` | `string` | *(Required)* | Stable identifier of the worker host. |
| `SessionID` | `string` | *(UUID)* | Ephemeral incarnation UUID generated on process start. |
| `Address` | `string` | *(Auto)* | Local network address announced to coordinator. |
| `CoordinatorEndpoint` | `string` | *(Required)* | gRPC endpoint of initial coordinator to contact. |
| `CoordinatorAddrs` | `[]string` | `[]` | Seed list of all coordinator addresses for automatic failover. |
| `MaxSlots` | `int32` | `4` | Maximum parallel task execution concurrency. |
| `HeartbeatInterval` | `Duration` | `3s` | Cadence of lease renewal heartbeats sent to coordinator. |
| `PreemptBuffer` | `Duration` | `1.5s` | Worker pre-emptively cancels tasks if lease is not renewed. |

---

## 3. Operational HTTP Endpoints

Every coordinator exposes an integrated HTTP management and telemetry server on `HTTPPort`.

### `GET /healthz` (Liveness Probe)
- **Status 200 OK**: Node process is alive and HTTP server is operational.
- **Payload**:
  ```json
  {
    "status": "healthy",
    "time": "2026-09-08T04:30:00Z"
  }
  ```

### `GET /readyz` (Readiness Probe)
- **Status 200 OK**: Node is ready to handle traffic (either authoritative leader or synchronized follower connected to leader).
- **Status 503 Service Unavailable**: Node is partitioned, shutting down, or disconnected from cluster quorum.
- **Payload**:
  ```json
  {
    "status": "ready",
    "time": "2026-09-08T04:30:00Z"
  }
  ```

### `GET /status` (Operational State)
- **Status 200 OK**: Live cluster state metadata.
- **Payload**:
  ```json
  {
    "node_id": "coord-1",
    "role": "Leader",
    "leader_id": "coord-1",
    "leader_addr": "127.0.0.1:50051",
    "term": 4,
    "commit_index": 18240,
    "applied_index": 18240,
    "active_workers": 12,
    "queue_depth": 3,
    "start_time": "2026-09-08T04:00:00Z",
    "uptime_seconds": 1800.0
  }
  ```

### `GET /metrics` (Prometheus Metrics)
- Standard Prometheus text format with OpenMetrics support.
- Fully described in [Observability Guide](observability.md).

---

## 4. Lifecycle Procedures

### Bootstrapping a New Cluster
1. Provision 3 nodes: `coord-1` (10.0.0.1), `coord-2` (10.0.0.2), `coord-3` (10.0.0.3).
2. Configure `coord-1` with `RaftBootstrap: true`.
3. Configure `coord-2` and `coord-3` with `RaftBootstrap: false` and peer address lists pointing to all 3 nodes.
4. Launch all 3 processes simultaneously. Quorum is achieved when 2 nodes connect; `coord-1` triggers initial election and becomes Term 2 leader.

### Graceful Coordinator Shutdown
1. Send `SIGTERM` to the target coordinator.
2. The coordinator calls `Server.Stop()`:
   - Closes `stopCh` to halt background reaper and retry timers.
   - Flushes telemetry tracer provider.
   - Shuts down HTTP telemetry listener.
   - Terminates Raft consensus engine (`raftNode.Shutdown()`).
   - Invokes `grpcServer.GracefulStop()`, allowing in-flight requests to complete.

### Safe Follower Maintenance
1. Confirm cluster health via `/status` on all nodes (ensure `term` and `commit_index` match).
2. Stop the target follower. Quorum of 2 remains active; cluster throughput is unaffected.
3. Perform OS / binary upgrade.
4. Restart follower with existing `RaftDataDir`. Follower reconnects via TCP, replays delta Raft log entries, and re-synchronizes in <300ms.

### Safe Leader Maintenance
1. Query `/status` on all nodes to identify the current leader.
2. Issue graceful step-down or terminate the leader process.
3. Remaining 2 followers detect heartbeat absence after randomized election timeout (75–150ms).
4. One follower requests votes, receives quorum (2/2), and assumes leadership in Term $N+1$.
5. Failover reconciliation window activates for 4 seconds, preserving worker task leases.
6. Upgrade and restart original leader as follower.

---

## 5. Persistent Storage & Backup

### BoltDB Storage Layout
```
data/raft-coord-1/
├── raft-log.bolt      # Replicated Raft append-only write log
├── raft-stable.bolt   # Hard consensus state (CurrentTerm, VotedFor)
└── snapshots/         # Periodic state machine snapshots & metadata
    └── 1-12000-1725.../
        ├── meta.json  # Snapshot index, term, and configuration
        └── state.bin  # Serialized state store (Workers, Tasks, DAGs)
```

### Snapshot & Recovery Policy
- **Automatic Snapshots**: Triggered every 5,000 applied log entries.
- **Log Compaction**: Old log entries prior to the snapshot index are automatically pruned to bound disk usage.
- **Disaster Backup**: To take a physical backup, query `/status` to confirm quiescence or low load, then snapshot the `snapshots/` directory.

---

## 6. Capacity Planning & Limits

| Dimension | Tested / Recommended Limit | Operational Constraint |
| :--- | :--- | :--- |
| **Max Tasks in Flight** | 100,000 tasks | Memory-backed state store (~1.5 KB per task). |
| **Max Payload Size** | 1 MB per task | Enforced at gRPC API boundary to avoid network stalls. |
| **Max DAG Tasks** | 500 tasks per DAG | Enforced at DAG submission to bound validation latency. |
| **Max Task Dependencies** | 50 dependencies | Prevents cycle validation overhead and topological bottlenecks. |
| **Sustained Throughput** | 2,500 – 4,000 tasks/sec | Replicated consensus commit limit over TCP. |
| **Max Workers** | 500 active worker daemons | Monitored by periodic heartbeat reaper sweep (1s). |
| **Disk Growth** | ~100 MB per 100,000 tasks | Bound by automatic Raft snapshot compaction. |
