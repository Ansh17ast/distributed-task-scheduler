# Task State Machine: Formal Specification

This document formally defines the finite state machine (FSM) governing all task lifecycle operations. In this system, state transitions are deterministic, validated against a strict transition matrix, applied linearly via Raft log replication, and guaranteed to be idempotent.

---

## 1. Task States

Every task in the system resides in exactly one of the following 11 states:

| State | Definition | Nature |
| :--- | :--- | :--- |
| `PENDING` | Task has been admitted into the system; initial tenant and validation checks underway. | Transient |
| `BLOCKED` | Task belongs to a DAG and has one or more unsatisfied upstream dependencies. | Waiting |
| `READY` | All dependencies satisfied (or standalone task); queued for worker assignment. | Queueable |
| `LEASED` | Assigned to a worker node; lease deadline running; awaiting initial worker ack. | Leased |
| `RUNNING` | Worker confirmed active execution of the task payload. | Executing |
| `SUCCEEDED`| Task finished successfully; terminal result payload stored. | Terminal |
| `FAILED` | Task execution failed; retry limit reached or error is non-retryable. | Terminal |
| `RETRY_WAIT`| Task failed but attempts remain; awaiting exponential backoff duration. | Waiting |
| `CANCELLED`| Explicitly aborted by client request or transitively aborted due to upstream failure. | Terminal |
| `LOST` | Worker lease expired or worker crashed without reporting completion. | Recovery |
| `RECOVERING`| Scheduler verifying task idempotency / side-effects before re-enqueue or failure. | Recovery |

---

## 2. Complete State Transition Table

The following table defines every permitted transition in the engine. Any transition not explicitly listed in this table is **strictly invalid** and will be rejected with `ErrInvalidStateTransition`.

| Current State | Triggering Event | Preconditions | Target State | State Mutations | Consensus Req. (Raft Command) | Invalid Cases & Guard Violations |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **NONE** | `SubmitTask` | Valid payload, tenant quota available | `PENDING` | Set `CreatedAt`, `TenantID`, `State=PENDING` | `CmdTaskSubmit` | Duplicate `IdempotencyKey` returns existing task; quota exceeded rejects with `ResourceExhausted`. |
| `PENDING` | `EvaluateDAG` | Upstream dependencies exist and are not yet `SUCCEEDED` | `BLOCKED` | `State=BLOCKED` | `CmdTaskUpdateState` | Cannot transition if dependencies are already `SUCCEEDED`. |
| `PENDING` | `EvaluateDAG` | No upstream dependencies exist OR all upstream dependencies are `SUCCEEDED` | `READY` | `State=READY`, Enqueue in ready queue | `CmdTaskUpdateState` | Cannot transition if any dependency is `BLOCKED`, `FAILED`, or `CANCELLED`. |
| `BLOCKED` | `UpstreamSucceeded` | Final upstream dependency transitioned to `SUCCEEDED` | `READY` | `State=READY`, Enqueue in ready queue | `CmdTaskUpdateState` | Cannot transition if any required dependency remains incomplete. |
| `BLOCKED` | `UpstreamFailed` | Any upstream dependency transitioned to `FAILED` or `CANCELLED` | `CANCELLED` | `State=CANCELLED`, `Error="upstream dependency failed"` | `CmdTaskUpdateState` | Cannot transition if all dependencies are still pending/healthy. |
| `READY` | `LeaseToWorker` | Worker has free slot, tenant concurrency allows, worker healthy | `LEASED` | `State=LEASED`, `AssignedWorker=W`, `LeaseEpoch++`, `LeaseDeadline=Now+T_lease` | `CmdTaskLease` | Cannot lease to worker with 0 slots; cannot lease if tenant concurrency cap reached. |
| `LEASED` | `WorkerAckStart` | Incoming message has matching `WorkerID` and `LeaseEpoch` | `RUNNING` | `State=RUNNING`, `StartedAt=Now` | `CmdTaskStateChange` | Reject if incoming `LeaseEpoch` is stale or `WorkerID` does not match. |
| `LEASED` | `LeaseTimeout` | Current time > `LeaseDeadline` and no heartbeat received | `LOST` | `State=LOST`, `LostAt=Now` | `CmdTaskReclaim` | Cannot expire if heartbeat renewed lease prior to deadline. |
| `RUNNING` | `WorkerReportSuccess`| Worker completed execution; incoming `LeaseEpoch` matches | `SUCCEEDED` | `State=SUCCEEDED`, `CompletedAt=Now`, `Result=payload` | `CmdTaskStateChange` | Stale `LeaseEpoch` rejected with `ErrStaleLeaseEpoch`. Task cannot complete under expired lease. |
| `RUNNING` | `WorkerReportFailure`| Worker error; incoming `LeaseEpoch` matches; `Attempts < MaxAttempts` | `RETRY_WAIT` | `State=RETRY_WAIT`, `Attempts++`, `NextRetryAt=Now+Backoff` | `CmdTaskStateChange` | Stale `LeaseEpoch` rejected. If `Attempts >= MaxAttempts`, transition to `FAILED` instead. |
| `RUNNING` | `WorkerReportFailure`| Worker error; incoming `LeaseEpoch` matches; `Attempts >= MaxAttempts` | `FAILED` | `State=FAILED`, `CompletedAt=Now`, `Error=err` | `CmdTaskStateChange` | Non-terminal error with retries remaining cannot transition to `FAILED`. |
| `RUNNING` | `LeaseTimeout` | Worker stopped heartbeating; `Now > LeaseDeadline` | `LOST` | `State=LOST`, `LostAt=Now` | `CmdTaskReclaim` | Active heartbeat present. |
| `LOST` | `InitiateRecovery` | Background recovery scanner processes lost task | `RECOVERING` | `State=RECOVERING` | `CmdTaskUpdateState` | Task is not in `LOST` state. |
| `RECOVERING`| `RecoveryCheck` | Task retry limit not exceeded (`Attempts < MaxAttempts`) | `READY` | `State=READY`, `AssignedWorker=nil`, `Attempts++` | `CmdTaskUpdateState` | Cannot move to `READY` if retries exhausted. |
| `RECOVERING`| `RecoveryCheck` | Task retry limit exceeded (`Attempts >= MaxAttempts`) | `FAILED` | `State=FAILED`, `Error="worker lease expired; retries exhausted"` | `CmdTaskUpdateState` | Cannot fail if retries are still available. |
| `RETRY_WAIT`| `BackoffElapsed` | Monotonic backoff duration elapsed (`Now >= NextRetryAt`) | `READY` | `State=READY`, Enqueue in ready queue | `CmdTaskUpdateState` | Cannot transition early before backoff duration elapses. |
| `PENDING`, `BLOCKED`, `READY`, `LEASED`, `RUNNING`, `RETRY_WAIT` | `ClientCancel` | Client explicitly issues cancel request | `CANCELLED` | `State=CANCELLED`, `CompletedAt=Now`, `Error="cancelled by client"` | `CmdTaskUpdateState` | Tasks in terminal states (`SUCCEEDED`, `FAILED`) cannot be cancelled. |

---

## 3. Idempotency Invariants

In distributed networks, RPC retries, network delays, and message replays are common. The task state machine enforces three idempotency rules:

### Rule 1: Duplicate Event Idempotency
If an event triggers a transition to a state that the task **already occupies**, the operation succeeds as a no-op:
$$\text{ApplyTransition}(T, \text{TargetState}) = \text{OK if } T.\text{State} == \text{TargetState}$$
*Example*: If a worker sends `TaskCompleted` twice due to a network timeout on the ACK, the second completion matches `SUCCEEDED` and returns success without re-executing state mutation hooks.

### Rule 2: Stale Event Fencing
Any event from a worker (`AckStart`, `ReportSuccess`, `ReportFailure`, `Heartbeat`) MUST supply:
- `TaskID`
- `WorkerID`
- `LeaseEpoch`

The coordinator evaluates:
$$\text{IsValid} = (T.\text{AssignedWorker} == \text{WorkerID}) \land (T.\text{LeaseEpoch} == \text{LeaseEpoch})$$
If `Incoming.LeaseEpoch < Task.LeaseEpoch`, the transition is **rejected immediately** with:
```go
var ErrStaleLeaseEpoch = errors.New("fencing token mismatch: worker lease epoch is stale")
```

### Rule 3: Terminal State Finality
Once a task enters `SUCCEEDED`, `FAILED`, or `CANCELLED`, its state is **immutable**. No subsequent event (lease timeout, late worker heartbeat, retry trigger) can alter the state.

---

## 4. Go State Machine Interface & Transition Engine

The state machine is implemented as a standalone package (`internal/statemachine`) free of external network dependencies, ensuring 100% testability.

```go
type State string

const (
    StatePending    State = "PENDING"
    StateBlocked    State = "BLOCKED"
    StateReady      State = "READY"
    StateLeased     State = "LEASED"
    StateRunning    State = "RUNNING"
    StateSucceeded  State = "SUCCEEDED"
    StateFailed     State = "FAILED"
    StateRetryWait  State = "RETRY_WAIT"
    StateCancelled  State = "CANCELLED"
    StateLost       State = "LOST"
    StateRecovering State = "RECOVERING"
)

type TransitionEngine struct {
    validTransitions map[State]map[State]bool
}

func (e *TransitionEngine) ValidateTransition(from, to State) error {
    if from == to {
        return nil // Idempotent no-op
    }
    if allowed, exists := e.validTransitions[from][to]; exists && allowed {
        return nil
    }
    return fmt.Errorf("illegal transition from %s to %s", from, to)
}
```
