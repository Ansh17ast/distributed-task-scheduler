# System Architecture: Distributed Fault-Tolerant Task Scheduler

This document specifies the end-to-end architecture, authority boundaries, consensus model, timing parameters, and failure recovery flows of the Distributed Fault-Tolerant Task Scheduler.

---

## 1. Explicit 3-Node Topology

The cluster operates with a dedicated **3-node coordinator cluster**:
- **Coordinator 1**: Currently Elected Raft Leader (Authoritative)
- **Coordinator 2**: Raft Follower
- **Coordinator 3**: Raft Follower
- **Cluster Quorum**:
  $$Q = \left\lfloor \frac{3}{2} \right\rfloor + 1 = 2 \text{ nodes}$$
- **Fault Tolerance**: Up to 1 coordinator node failure tolerated with zero interruption to consensus or scheduling availability.

```
                                  +-----------------------+
                                  |      Client CLI       |
                                  +-----------------------+
                                              |
                                              | (gRPC: SubmitTask / SubmitDAG)
                                              v
      +============================= COORDINATOR CLUSTER =============================+
      |                                                                               |
      |   +-------------------+  Raft Replication (Q=2)  +-------------------+        |
      |   | Coordinator 1     |<========================>| Coordinator 2     |        |
      |   | (Elected Leader)  |                          | (Follower)        |        |
      |   +-------------------+                          +-------------------+        |
      |             ^                                              ^                  |
      |             |             Raft Replication (Q=2)           |                  |
      |             +==============================================+                  |
      |             |                                                                 |
      |             v                                                                 |
      |   +-------------------+                                                       |
      |   | Coordinator 3     |                                                       |
      |   | (Follower)        |                                                       |
      |   +-------------------+                                                       |
      |                                                                               |
      |   [AUTHORITY BOUNDARY: Inside Coordinator 1 (Leader)]                         |
      |                                                                               |
      |     (In-Memory Calculation & Trigger Engines - NOT Sources of Truth)          |
      |     +-------------------------------------------------------------+           |
      |     | - DAG Engine: Evaluates topology & unblocked dependencies   |           |
      |     | - Scheduler: Ranks ready tasks & matches worker capacity    |           |
      |     | - Lease Tracker: Evaluates monotonic expirations & zombies  |           |
      |     +-------------------------------------------------------------+           |
      |                                    |                                          |
      |                                    | Proposes Raft Commands                   |
      |                                    v                                          |
      |     +-------------------------------------------------------------+           |
      |     |  REPLICATED RAFT LOG & COMMITTED STATE MACHINE (Raft FSM)   |           |
      |     |  ** THE ONLY AUTHORITATIVE SOURCE OF TRUTH **               |           |
      |     +-------------------------------------------------------------+           |
      +====================================|==========================================+
                                           |
                                           | Bi-directional gRPC Streaming (Pull/Lease)
                                           v
      +============================== WORKER POOL ====================================+
      |                                                                               |
      |   +--------------------------------+     +--------------------------------+   |
      |   | Worker Node A                  |     | Worker Node B                  |   |
      |   | - Capacity Slot Manager        |     | - Capacity Slot Manager        |   |
      |   | - Task Puller Loop             |     | - Task Puller Loop             |   |
      |   | - Monotonic Lease Heartbeater  |     | - Monotonic Lease Heartbeater  |   |
      |   | - Sandboxed Process Executor   |     | - Sandboxed Process Executor   |   |
      |   | - Lease Fencing Token Checker  |     | - Lease Fencing Token Checker  |   |
      |   +--------------------------------+     +--------------------------------+   |
      +===============================================================================+
```

---

## 2. The Authority & Consistency Boundary

### Critical Architectural Rule
The following subsystems are **Calculation & Trigger Engines**, **NOT** independent sources of truth:
- **DAG Engine**: Calculates topological dependency resolution; detects when parent tasks complete; triggers proposals to promote children.
- **Scheduler**: Calculates optimal candidate-to-worker matches based on priority, FIFO order, available slots, and tenant quotas.
- **Worker Lease Tracker**: Measures monotonic timer deltas to detect dead workers; triggers task reclamation proposals.

**None of these engines mutate state directly.**
All authoritative mutations must be proposed as a strongly typed **Raft command**, committed across a **quorum (2 of 3 coordinators)**, and applied deterministically to the **Replicated FSM**.

```
Client / Worker Event
         |
         v
Elected Leader
   |
   +---> DAG Engine (Calculates if dependencies are satisfied)
   |
   +---> Scheduler Engine (Calculates task-to-worker match)
   |
   +---> Lease Tracker (Calculates if lease deadline expired)
   |
   v (Constructs state change proposal)
Raft Command
   |
   v (Replicated across Quorum = 2 of 3)
Replicated FSM
   |
   v (Deterministic state transition)
Committed State
   |
   v (Externalized to execution plane)
Worker Assignment / Client Response
```

---

## 3. Operation Classification: Reads, Local Calculations, and Raft Mutations

| Operation Category | Specific Operations | Execution Location | Consistency & Authority Mechanism |
| :--- | :--- | :--- | :--- |
| **READ Operations** | `GetTaskStatus`, `GetDAGStatus`, `ListWorkers`, `GetClusterHealth` | Leader (or Follower with redirect) | Read from local committed FSM. Linearizable reads on Leader verify quorum lease via `raft.Barrier()`. Followers redirect mutations to leader. |
| **LOCAL Calculations** | DAG Cycle Detection (Kahn's), Priority Heap Sort, Worker Capacity Matching, Monotonic Heartbeat Tracking | Leader In-Memory | Pure in-memory calculation. Has zero authority until proposed and committed to Raft. |
| **RAFT-COMMITTED State Mutations** | Task state transitions, task assignments, worker registrations, lease expirations, task cancellations | Replicated to Quorum (2 of 3 nodes) | Must be proposed via `raft.Apply()`. Only exists authoritatively after index is committed and applied by the FSM. |

---

## 4. State-Changing Raft Commands (Clean Consensus Boundary)

To prevent saturating the Raft consensus log with high-frequency heartbeats, **worker heartbeats are tracked locally in-memory by the elected leader**.
The Raft log is reserved strictly for authoritative state mutations:

```protobuf
syntax = "proto3";
package scheduler.consensus;

message RaftCommand {
    oneof command {
        CmdSubmitTask        submit_task         = 1;
        CmdSubmitDAG         submit_dag          = 2;
        CmdMarkTaskReady     mark_task_ready     = 3;
        CmdGrantTaskLease    grant_task_lease    = 4;
        CmdMarkTaskRunning   mark_task_running   = 5;
        CmdCompleteTask      complete_task       = 6;
        CmdFailTask          fail_task           = 7;
        CmdReclaimTask       reclaim_task        = 8;
        CmdCancelTask        cancel_task         = 9;
        CmdRegisterWorker    register_worker     = 10;
        CmdExpireWorker      expire_worker       = 11;
    }
}
```

### Detailed Command Semantics

1. **`CmdSubmitTask`**: Admits a new task; sets state to `PENDING` with tenant limits and idempotency key.
2. **`CmdSubmitDAG`**: Admits a complete DAG; records adjacency lists and root tasks.
3. **`CmdMarkTaskReady`**: Transitions task from `PENDING`, `BLOCKED`, or `RETRY_WAIT` to `READY`; enqueues into ready queue.
4. **`CmdGrantTaskLease`**: Transitions task from `READY` to `LEASED`; assigns `WorkerID`; increments `LeaseEpoch`; records lease grant timestamp and duration.
5. **`CmdMarkTaskRunning`**: Transitions task from `LEASED` to `RUNNING` on worker ACK; sets `StartedAt`.
6. **`CmdCompleteTask`**: Transitions task from `RUNNING` to `SUCCEEDED`; records execution output; evaluates downstream DAG dependents.
7. **`CmdFailTask`**: Transitions task to `FAILED` (terminal); records error diagnostics; triggers transitive DAG cancellations.
8. **`CmdReclaimTask`**: Transitions an expired/abandoned task from `LEASED`/`RUNNING` to `LOST` $\to$ `RECOVERING`; clears worker binding.
9. **`CmdCancelTask`**: Transitions task to `CANCELLED` via client request or upstream DAG failure.
10. **`CmdRegisterWorker`**: Registers new worker with address and hardware slot capacity.
11. **`CmdExpireWorker`**: Marks dead worker `EXPIRED`; revokes its active scheduling capacity.

---

## 5. Configurable Timing Parameters (Benchmark Baselines)

All temporal parameters are configurable via configuration files or environment variables. The values below represent initial baseline settings for local benchmarking:

| Parameter Name | Config Key | Initial Benchmark Value | Description |
| :--- | :--- | :--- | :--- |
| **Raft Heartbeat Interval** | `raft.heartbeat_timeout` | `50ms` | Interval between leader-to-follower Raft heartbeats. |
| **Raft Election Timeout Min** | `raft.election_timeout_min`| `150ms` | Lower bound for follower randomized election timeout. |
| **Raft Election Timeout Max** | `raft.election_timeout_max`| `300ms` | Upper bound for follower randomized election timeout. |
| **Worker Heartbeat Interval** | `worker.heartbeat_interval`| `3s` | Interval at which workers send liveness pings to leader. |
| **Worker Lease Duration** | `worker.lease_duration` | `10s` | Duration a worker holds a task without renewal. |
| **Lease Expiration Grace** | `lease.grace_window` | `2s` | Buffer added by leader before triggering zombie reclamation. |
| **Failover Reconciliation** | `lease.failover_reconcile` | `4s` | Bounded window for workers to establish contact with a new leader. |

---

## 6. What Happens When the Leader Dies?

```
[Coordinator 1 (Leader) Dies]
             |
             v
1. Followers (C2, C3) Miss Raft Heartbeats (raft.heartbeat_timeout = 50ms)
             |
             v
2. Follower Election Timers Expire (Randomized: 150ms - 300ms)
             |
             v
3. Coordinator 2 Becomes Candidate & Wins Election:
   - Increments CurrentTerm, votes for itself.
   - Sends RequestVote RPC to Coordinator 3; receives vote.
   - Quorum achieved (2 of 3 votes). C2 is elected New Raft Leader.
             |
             v
4. Establish Leadership & Linearizability via raft.Barrier():
   - C2 invokes raft.Barrier().
   - Barrier() commits a no-op entry through consensus to verify that C2 is recognized
     by a quorum and that all committed log entries from prior terms are applied to C2's FSM.
             |
             v
5. Clock-Independent Failover Lease Reconciliation:
   - C2 reconstructs the latest committed logical lease state from the FSM:
     For every active task in LEASED or RUNNING, C2 reads: (TaskID, WorkerID, LeaseEpoch, LeaseDuration).
     Notice: No cross-process monotonic timestamp from C1 is evaluated.
   - Bounded Local Reconciliation Window (T_reconcile = 4s):
     C2 starts a local timer on its own monotonic clock:
     reconcileDeadline = C2_local_monotonic_now + T_reconcile.
     C2 opens its worker gRPC endpoints to accept heartbeats from active workers.
   - Adoption vs. Reclamation:
     If a worker connects and proves liveness for (TaskID, LeaseEpoch) within T_reconcile,
     C2 adopts the lease into its local in-memory tracker and resets its local lease deadline.
     If no heartbeat is received within T_reconcile, C2 concludes the worker died and
     immediately proposes CmdReclaimTask to Raft.
             |
             v
6. Stale Old Leader (C1) Fenced:
   - If C1 recovers from a freeze, it attempts writes with its old Term.
   - C2 rejects C1's messages with the higher Term.
   - C1 immediately steps down to Follower, rolls back uncommitted entries, and syncs log from C2.
```

---

## 7. What Happens When a Worker Dies?

```
[Worker Node A Crashes / Dies Mid-Task]
             |
             v
1. Worker A ceases sending periodic heartbeats (T_renew = 3s)
             |
             v
2. Coordinator Leader Detects Lease Expiration:
   - Monotonic timer exceeds LeaseDeadline (10s) + GraceWindow (2s).
   - Leader-local Lease Tracker flags Worker A as UNHEALTHY.
             |
             v
3. Leader Proposes CmdReclaimTask to Raft:
   - Replicated across Quorum (2 of 3 coordinators).
   - Task transitions: RUNNING -> LOST -> RECOVERING.
             |
             v
4. Fencing Token (LeaseEpoch) Guarantees Stale Worker Rejection:
   - If Worker A was merely frozen (e.g. GC pause) and wakes up late to report completion:
     - Worker A sends ReportSuccess(TaskID, LeaseEpoch = 1).
     - Coordinator checks task's current LeaseEpoch (incremented to 2 upon reclamation).
     - Because incoming LeaseEpoch (1) != current LeaseEpoch (2), the coordinator
       rejects the completion with ErrStaleLeaseEpoch.
             |
             v
5. Outstanding Task Identified & Recovered:
   - If AttemptCount < MaxRetries:
     - Task transitions to READY; AssignedWorker is cleared.
     - Enqueued into scheduler priority queue for reassignment.
   - If AttemptCount >= MaxRetries:
     - Task transitions to FAILED; downstream DAG dependencies cancelled.
             |
             v
6. Scheduler Reassigns Task:
   - Worker B pulls task; coordinator commits CmdGrantTaskLease (with LeaseEpoch = 2).
   - Worker B executes the task fresh.
```

---

## 8. What Happens During a Network Partition?

We rigorously separate **Coordinator Partitions** from **Worker-to-Coordinator Partitions**.

### 8.1. Coordinator Partition
Partition divides 3-node cluster into **Minority ($\{C_1\}$)** and **Majority ($\{C_2, C_3\}$)**:
- **Leader in Minority Partition ($C_1$)**:
  $C_1$ attempts to replicate incoming mutations to $C_2$ and $C_3$. Receives 0 ACKs. Votes = 1 of 3 (Quorum is 2).
  **Mutations never commit.** Client/worker RPCs block or time out. $C_1$ cannot make authoritative mutations.
- **Majority Partition ($\{C_2, C_3\}$)**:
  $C_2$ and $C_3$ elect $C_2$ as leader with 2 of 3 votes. $C_2$ calls `raft.Barrier()` to confirm leadership and applies committed entries. $C_2$ continues committing tasks with full quorum.
- **Follower in Minority Partition**:
  If $C_3$ were isolated, $C_1$ and $C_2$ form a quorum of 2 and operate unimpeded. $C_3$ synchronizes upon reconnect.
- **Stale Leader Attempting Mutation**:
  When the partition heals, $C_1$ contacts $C_2$ with old Term. $C_2$ responds with higher Term. $C_1$ steps down to Follower, rolls back uncommitted log entries, and replicates committed log. **Zero split-brain.**

### 8.2. Worker-to-Coordinator Partition
A network partition isolates a Worker Node ($W_1$) from the Coordinator Leader:
- **Local Worker Execution**: $W_1$ continues executing its active task only until its **local monotonic preemption deadline**:
  $$t_{\text{preempt}} = t_{\text{last\_heartbeat}} + T_{\text{lease}} - T_{\text{preempt}}$$
  (e.g., $10\text{s} - 1.5\text{s} = 8.5\text{s}$ after last successful heartbeat).
- **Worker Self-Preemption**: When $t_{\text{preempt}}$ elapses without successful heartbeat renewal, $W_1$'s `context.WithDeadline` fires. $W_1$ forcibly issues `SIGTERM`/`SIGKILL` to the running subprocess and terminates the task.
- **Worker Reconnect Loop**: $W_1$'s background reconnect loop enters exponential backoff with jitter and attempts to establish a connection to all configured coordinator endpoints ($C_1, C_2, C_3$).
- **Fencing on Reconnect**: If $W_1$ reconnects after the coordinator reclaimed the task:
  The coordinator incremented `LeaseEpoch`. Any status report or heartbeat from $W_1$ carrying the old lease epoch is rejected with `ErrStaleLeaseEpoch`. $W_1$ drops the old task and frees its capacity slot.

---

## 9. Formal Execution Guarantees

We explicitly define the boundaries of our system guarantees:

> **Execution Guarantee**:
> **At-least-once task execution with strongly consistent, idempotent task state transitions.**
> **Arbitrary external side effects cannot be guaranteed exactly-once.**
> Task-level idempotency keys and `LeaseEpoch` fencing tokens prevent duplicate state mutations within the scheduler.
