# Raft Consensus & Multi-Coordinator Design

This document details the consensus subsystem of the coordinator cluster. It defines the role of `hashicorp/raft`, custom state machine integration, quorum mechanics, leader failover routines, linearizability barriers, and stale-leader fencing.

---

## 1. Consensus Boundary: What HashiCorp Raft Handles vs. What We Build

To achieve production stability without reinventing low-level networking and storage bugs, we utilize `hashicorp/raft` as the underlying consensus substrate while building our entire distributed scheduler on top of it.

```
+========================================================================================+
|                              OUR CUSTOM ENGINE LAYER                                   |
|                                                                                        |
|  - Authoritative Task State Machine (Raft FSM)                                         |
|  - Command SerDe & Versioning (Protobuf log entry serialization)                       |
|  - Stale Leader Fencing (raft.Barrier() linearizability checks)                        |
|  - Leader State Re-alignment & In-Memory Queue Rebuilding                              |
|  - Follower Request Forwarding & Leadership Status RPC Metadata                        |
|  - Monotonic Worker Lease Tracking & Zombie Reclamation Proposals                      |
|  - DAG Topology Validation & Promotion Engine                                          |
+========================================================================================+
                                            |
                                            v (via raft.FSM interface)
+========================================================================================+
|                            HASHICORP/RAFT SUBSYSTEM                                    |
|                                                                                        |
|  - Leader Election (Term increments, Randomized Election Timeouts)                     |
|  - Heartbeat & Liveness (Leader-to-follower heartbeat pings)                           |
|  - Log Replication (AppendEntries RPCs, Commit Index tracking)                         |
|  - Persistent Storage (File / BoltDB WAL, snapshot metadata)                           |
|  - Dynamic Cluster Membership (AddPeer, RemovePeer)                                    |
+========================================================================================+
```

---

## 2. Core Raft Parameters & Quorum Mechanics

For our dedicated 3-node coordinator cluster ($N = 3$):
- **Quorum Size**:
  $$Q = \left\lfloor \frac{N}{2} \right\rfloor + 1 = \left\lfloor \frac{3}{2} \right\rfloor + 1 = 2 \text{ nodes}$$
- **Configurable Benchmark Timing Baseline**:
  - `raft.heartbeat_timeout`: `50ms` (baseline interval between leader heartbeats).
  - `raft.election_timeout_min`: `150ms` (baseline lower bound for randomized election timeout).
  - `raft.election_timeout_max`: `300ms` (baseline upper bound for randomized election timeout).
- **Tolerated Failures**: $F = \left\lfloor \frac{N - 1}{2} \right\rfloor = 1$ node failure. The cluster maintains full consensus availability with 2 surviving nodes.

---

## 3. The Custom Task FSM (`raft.FSM`)

All authoritative mutations to tasks, DAGs, and tenants must pass through the Raft log.
High-frequency worker heartbeats are handled **locally in memory on the leader** and are **not** replicated through Raft.
Our coordinator implements `raft.FSM`:

```go
type TaskFSM struct {
    mu      sync.RWMutex
    tasks   map[string]*Task
    dags    map[string]*DAG
    tenants map[string]*TenantQuota
    workers map[string]*WorkerNode
    logger  *slog.Logger
}

// Apply is invoked by Raft once a log entry has been committed across quorum.
func (fsm *TaskFSM) Apply(log *raft.Log) interface{} {
    cmd, err := decodeCommand(log.Data)
    if err != nil {
        return &ApplyResult{Error: err}
    }

    fsm.mu.Lock()
    defer fsm.mu.Unlock()

    switch c := cmd.(type) {
    case *CmdSubmitTask:
        return fsm.applySubmitTask(c, log.Index, log.Term)
    case *CmdSubmitDAG:
        return fsm.applySubmitDAG(c, log.Index, log.Term)
    case *CmdMarkTaskReady:
        return fsm.applyMarkTaskReady(c, log.Index, log.Term)
    case *CmdGrantTaskLease:
        return fsm.applyGrantTaskLease(c, log.Index, log.Term)
    case *CmdMarkTaskRunning:
        return fsm.applyMarkTaskRunning(c, log.Index, log.Term)
    case *CmdCompleteTask:
        return fsm.applyCompleteTask(c, log.Index, log.Term)
    case *CmdFailTask:
        return fsm.applyFailTask(c, log.Index, log.Term)
    case *CmdReclaimTask:
        return fsm.applyReclaimTask(c, log.Index, log.Term)
    case *CmdCancelTask:
        return fsm.applyCancelTask(c, log.Index, log.Term)
    case *CmdRegisterWorker:
        return fsm.applyRegisterWorker(c, log.Index, log.Term)
    case *CmdExpireWorker:
        return fsm.applyExpireWorker(c, log.Index, log.Term)
    default:
        return &ApplyResult{Error: ErrUnknownCommand}
    }
}
```

---

## 4. Leader Establishment & Linearizability via `raft.Barrier()`

### Accurate Definition of `Barrier()`
In distributed systems using Raft, electing a new leader does not instantly guarantee that the node is safe to serve linearizable operations. A partitioned node might falsely believe it is leader, or prior committed log entries might still be in the process of applying to the local FSM.

To establish authoritative leadership and guarantee linearizability, a newly elected leader executes `raft.Barrier()`:
1. **Quorum Leadership Verification**: `Barrier()` submits a no-op entry through consensus and blocks until it is replicated to a quorum. If the node is partitioned from quorum, `Barrier()` times out and fails, preventing stale leader split-brain.
2. **State Application Guarantee**: Successful completion of `Barrier()` guarantees that all committed log entries up to this index from prior terms have been fully applied to the local `TaskFSM`.
3. Only after `Barrier()` returns successfully does the leader rebuild its in-memory ready queues and begin serving worker pulls and client submissions.

---

## 5. Operations Requiring Raft Consensus vs. Local Operations

| Subsystem | Operation | Uses Raft? | Implementation Details |
| :--- | :--- | :--- | :--- |
| **Ingestion** | `SubmitTask` | **YES** | Log entry replicates task definition and payload. |
| **Ingestion** | `SubmitDAG` | **YES** | Log entry replicates graph adjacency list and tasks. |
| **Scheduler** | Task Ranking & Sorting | **NO** | In-memory sort by priority, arrival, and tenant weights. |
| **Scheduler** | Task Lease Grant | **YES** | Log entry commits assignment, increments `LeaseEpoch`. |
| **Worker** | Periodic Heartbeat | **NO** | Local monotonic timer check on leader; zero Raft overhead. |
| **Worker** | Worker Expired / Reclaim | **YES** | Log entry transitions task to `LOST` and revokes lease. |
| **Worker** | `AckStart` / `ReportSuccess` | **YES** | Log entry transitions task to `RUNNING` or `SUCCEEDED`. |
| **Worker** | `ReportFailure` | **YES** | Log entry transitions task to `RETRY_WAIT` or `FAILED`. |
| **Client** | `GetTaskStatus` | **NO (FSM)** | Served from local committed FSM. |
