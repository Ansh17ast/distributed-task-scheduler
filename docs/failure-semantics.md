# Failure Semantics & Exact-Once Analysis

This document provides a rigorous analysis of failure modes in the distributed scheduler. It addresses execution semantics, side-effect boundaries, and provides a formal fault matrix mapping failure detection to recovery actions and safety guarantees.

---

## 1. The Distributed Execution Reality: Exactly-Once vs. At-Least-Once

### 1.1. Why True "Exactly-Once" Execution is Impossible for Arbitrary Tasks
A fundamental result in distributed systems (FLP Impossibility and the Two Generals Problem) proves that **arbitrary distributed execution with external side effects cannot guarantee mathematical exactly-once execution** in the presence of unannounced node crashes or network partitions.

Consider an external task: *"Send an email"* or *"Charge a credit card via Stripe API"*:
1. The worker executes the task and charges the customer.
2. The response packet travels over the wire.
3. The worker machine suffers sudden power loss or kernel panic before reporting completion to the coordinator.
4. The coordinator's lease timer expires. The task is recovered and reassigned to another worker.
5. The new worker executes the task again, resulting in a duplicate charge unless the external payment API supports idempotency keys.

### 1.2. The Formal Guarantee of This Engine

> **System Guarantee**:
> **At-least-once task execution with strongly consistent, idempotent task state transitions.**
> **Arbitrary external side effects cannot be guaranteed exactly-once.**

| Property | Guarantee Level | Implementation Mechanism |
| :--- | :--- | :--- |
| **Scheduler State Transitions** | **Strongly Consistent (Linearizable)** | Transitions are replicated across Raft quorum before being externalized. |
| **Task Dispatching** | **At-Most-Once per LeaseEpoch** | A single `(TaskID, LeaseEpoch)` pair is never dispatched to multiple workers simultaneously. |
| **Task Execution** | **At-Least-Once** | If a worker dies mid-execution, the task is safely recovered and re-executed. |
| **State Mutation Idempotency** | **Effectively-Once** | Redundant completion reports or duplicate RPC calls result in idempotent state no-ops. |
| **Fencing & Zombie Protection** | **Guaranteed Exclusion** | Stale workers completing tasks after lease expiration are rejected by `LeaseEpoch` validation. |

---

## 2. Worker Edge Cases & Resolution

### Edge Case 1: Worker Finished Before Lease Expiry, but Completion RPC Delayed
- **Scenario**: Worker $W_1$ completes task execution at $t = 8\text{s}$ (lease expires at $10\text{s} + 2\text{s} = 12\text{s}$). Due to network congestion, the `ReportSuccess` packet arrives at the coordinator at $t = 13\text{s}$.
- **Resolution**:
  - At $t = 12\text{s}$, the coordinator lease reaper detected timeout and committed `CmdReclaimTask`, incrementing the task's epoch from $1 \to 2$ and moving the task to `LOST` $\to$ `READY`.
  - When $W_1$'s delayed `ReportSuccess(Epoch=1)` arrives at $t = 13\text{s}$, the coordinator sees $\text{Incoming}(1) < \text{Current}(2)$.
  - The delayed completion is **rejected** with `ErrStaleLeaseEpoch`.
  - **Safety**: Prevents a late-arriving completion from overwriting a new worker's active execution. Because execution is at-least-once, downstream side effects rely on idempotency keys.

### Edge Case 2: Duplicate Completion RPC (Network Retry)
- **Scenario**: Worker finishes, sends `ReportSuccess`, and coordinator successfully commits `SUCCEEDED`. However, the coordinator's TCP ACK drops. Worker retries `ReportSuccess`.
- **Resolution**:
  - Coordinator inspects task state. Task is already `SUCCEEDED` with matching `LeaseEpoch`.
  - Coordinator executes an **idempotent no-op**, acknowledges success to the worker, and does not re-trigger downstream DAG promotions.

### Edge Case 3: Worker Resumes After Being Considered Dead (Zombie Worker)
- **Scenario**: Worker $W_1$ suffers a 15-second kernel/GC freeze. Coordinator reclaims task and reassigns it to Worker $W_2$ under `LeaseEpoch = 2`. $W_1$ unfreezes and attempts to resume.
- **Resolution**:
  - Two layers of defense:
    1. **Worker-Side Preemption**: $W_1$'s local runner enforces a preemption deadline at $t_{\text{preempt}} = T_{\text{lease}} - 1.5\text{s} = 8.5\text{s}$. If the worker runtime paused completely, when it resumes, `time.Now().After(deadline)` is immediately true, triggering context cancellation and killing local subprocesses.
    2. **Coordinator Fencing**: If $W_1$ attempts to report status anyway, its `LeaseEpoch = 1` is rejected with `ErrStaleLeaseEpoch`.

### Edge Case 4: Worker Reconnects With an Old Lease
- **Scenario**: Worker $W_1$ reboots after a crash and reconnects to the coordinator, sending a heartbeat claiming it still holds task $T_1$ with `LeaseEpoch = 1`.
- **Resolution**:
  - Coordinator checks active lease map: $T_1$ has already been reclaimed and assigned to $W_2$ with `LeaseEpoch = 2`.
  - Coordinator response flags $T_1$ in `revoked_tasks`.
  - $W_1$ tears down any lingering local execution state, cleans scratch files, and marks its slot as free.

---

## 3. Network Partition Analysis: Coordinator vs. Worker

### 3.1. Coordinator Cluster Partition (Minority vs. Majority)
- **Minority ($\{C_1\}$)**:
  - Cannot reach quorum ($1 < 2$).
  - All write mutations (`SubmitTask`, `GrantTaskLease`, `CompleteTask`) fail to replicate and time out.
  - Linearizable reads via `raft.Barrier()` fail.
  - **No split-brain state mutations can be committed.**
- **Majority ($\{C_2, C_3\}$)**:
  - Elects a leader with 2 of 3 votes.
  - Issues `raft.Barrier()` to verify quorum leadership and apply committed entries.
  - Continues serving submissions, worker pulls, and task state transitions uninterrupted.
- **Partition Heals**:
  - $C_1$ receives higher Term, steps down to Follower, rolls back uncommitted log entries, and synchronizes from the majority leader.

### 3.2. Worker-to-Coordinator Partition
- **Local Execution**: The worker continues running its task **only until its local monotonic preemption deadline**:
  $$t_{\text{preempt}} = t_{\text{last\_heartbeat}} + T_{\text{lease}} - T_{\text{preempt}}$$
- **Preemption & Halt**: When $t_{\text{preempt}}$ elapses without successful heartbeat renewal, the worker cancels its execution context, terminating child processes.
- **Reconnect Loop**: The worker enters exponential backoff and probes all coordinator addresses ($C_1, C_2, C_3$).
- **Fencing on Reconnect**: If the coordinator reclaimed the task during the partition, `LeaseEpoch` was incremented. Any status report carrying the old epoch is rejected with `ErrStaleLeaseEpoch`.

---

## 4. Comprehensive Fault Matrix

Every potential failure is mapped according to:
$$\text{DETECTED} \longrightarrow \text{STATE TRANSITION} \longrightarrow \text{RECOVERY ACTION} \longrightarrow \text{SAFETY PROPERTY}$$

```
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| Failure Event               | Detected By         | State Transition   | Recovery Action                     | Safety Property Guaranteed                  |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 1. Leader Crash             | Follower election   | None (retained in  | Followers elect new Raft leader;    | Zero lost committed state; linearizability  |
|                             | timer expires       | Raft FSM)          | new leader reconciles active leases.| maintained across election terms.           |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 2. Follower Crash           | Leader Raft append  | None               | Leader continues serving quorum;    | System remains fully available as long as   |
|                             | RPC fails           |                    | catches up follower on reboot.      | a quorum (2 of 3) survives.                 |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 3. Worker Crash             | Monotonic lease     | LEASED/RUNNING     | Reclaim task; if attempts < max,    | Tasks are never permanently orphaned;      |
|                             | deadline expires    | -> LOST -> RECOVER | re-enqueue to READY; else FAILED.   | at-least-once execution preserved.          |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 4. Coordinator Partition    | Raft heartbeat      | Minority: no-op    | Majority cluster continues;         | No split-brain writes; minority cannot      |
|    (Majority vs Minority)   | failure to quorum   | Majority: normal   | minority rejects mutations.         | commit state transitions.                   |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 5. Worker Partition         | Monotonic timer     | Coordinator: LOST  | Worker self-preempts at t_preempt;  | Partitioned workers cannot execute past     |
|                             | on both sides       | Worker: Abort      | Coordinator reclaims and fences.    | lease deadline; stale reports rejected.     |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 6. Stale Worker             | Coordinator         | Rejection:         | Return ErrStaleLeaseEpoch; worker   | Zombie workers cannot overwrite active      |
|    (Late completion report) | LeaseEpoch mismatch | State unchanged    | context cancelled and slot freed.   | leases or commit stale task results.        |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 7. Duplicate Completion     | Coordinator         | Idempotent no-op   | Acknowledge success to worker;      | Task result and completion timestamp remain |
|    (Network retry of ACK)   | state check         | (already SUCCEEDED)| do not re-execute completion hooks. | immutable; no redundant state mutations.    |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 8. Coordinator Cold Restart | Boot sequence       | Replay Raft log    | Reconstruct FSM, reload snapshot,   | Committed data survives restarts;           |
|                             | & WAL loader        | into memory        | rejoin Raft cluster.                | deterministic recovery from disk WAL.       |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
| 9. Worker Reconnect         | Worker registration | Re-register with   | Worker starts with clean slot count;| Expired tasks are not duplicated on         |
|    After Crash / Reboot     | handshake RPC       | fresh session ID   | old un-acked tasks already cleared. | reconnected worker node.                    |
+----------------------------------------------------------------------------------------------------------------------------------------------------+
```
