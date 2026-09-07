# Scheduling & DAG Engine Specification

This document details the scheduling subsystem and Directed Acyclic Graph (DAG) dependency resolution engine. It defines the pluggable scheduler interface, scheduling policies, multi-tenant quota enforcement, and the DAG lifecycle.

---

## 1. Pluggable Scheduler Interface

To enable rigorous performance comparison and benchmarking between scheduling policies, all scheduling logic is decoupled behind a clean, deterministic Go interface:

```go
// Candidate represents a task ready for execution.
type Candidate struct {
    TaskID     string
    TenantID   string
    Priority   int32
    CreatedAt  time.Time
    CostSlots  int32
}

// WorkerCapacity represents an active worker available to take work.
type WorkerCapacity struct {
    WorkerID       string
    AvailableSlots int32
    MaxSlots       int32
    LastHeartbeat  time.Time
}

// Assignment matches a candidate task to a specific worker.
type Assignment struct {
    TaskID   string
    WorkerID string
    Reason   string // Diagnostic reason explaining why this choice was made
}

// Scheduler defines the pluggable policy contract.
type Scheduler interface {
    Name() string
    Schedule(
        ctx context.Context,
        candidates []*Candidate,
        workers []*WorkerCapacity,
        tenantQuotas map[string]*TenantQuota,
        currentTenantRunning map[string]int32,
    ) ([]Assignment, error)
}
```

---

## 2. Implemented Scheduling Policies

### 2.1. First-In, First-Out (FIFO) Policy
- **Logic**: Candidates are sorted strictly by `CreatedAt` ascending.
- **Assignment**: Greedy matching against available worker capacity slots.
- **Use Case**: Simple baseline policy for latency comparison.

### 2.2. Priority-Based Scheduling Policy
- **Logic**: Candidates are sorted using a multi-key comparison:
  1. `Priority` descending (higher integer = higher precedence: e.g., 100 > 10 > 0).
  2. `CreatedAt` ascending (earlier submission breaks ties).
- **Starvation Risk**: High-priority bursts can starve low-priority tasks.
- **Mitigation**: Configurable priority aging (each minute in queue increases effective priority by $\Delta$).

### 2.3. Fair-Share Multi-Tenant Scheduler (Deficit Round Robin)
- **Problem**: In a multi-tenant environment, a single tenant submitting 10,000 tasks must not monopolize all worker execution slots.
- **Algorithm**: Weighted Deficit Round Robin (DRR):
  - Each tenant $T_i$ is allocated a weight $W_i$ and a deficit counter $D_i$.
  - In each scheduling round, $D_i \leftarrow D_i + Q \times W_i$ (where $Q$ is the base quantum).
  - The scheduler iterates over active tenants. If $D_i \ge \text{TaskCost}$, the tenant's highest priority task is dispatched, and $D_i \leftarrow D_i - \text{TaskCost}$.
  - If a tenant has no ready tasks, its deficit is reset to 0 to prevent hoarding credits.

---

## 3. Multi-Tenant Concurrency Quotas & Backpressure

Each tenant is configured with explicit execution bounds:
```go
type TenantQuota struct {
    TenantID           string
    MaxConcurrency     int32 // Maximum simultaneous RUNNING/LEASED tasks
    MaxQueueDepth      int32 // Maximum pending tasks allowed in queue
    PriorityMultiplier float64
}
```

### Enforcement Rules
1. **Admission Backpressure**:
   When a client calls `SubmitTask`, the coordinator evaluates:
   $$\text{CurrentQueuedTasks}(T) \ge \text{TenantQuota}.\text{MaxQueueDepth}$$
   If exceeded, the coordinator rejects the submission immediately with gRPC status `codes.ResourceExhausted` ("tenant queue limit exceeded").
2. **Scheduling Concurrency Cap**:
   During the scheduling pass, if $\text{ActiveRunningTasks}(T) \ge \text{TenantQuota}.\text{MaxConcurrency}$, any remaining `READY` tasks for that tenant are **skipped** for this cycle, allowing other tenants to utilize worker slots.

---

## 4. DAG Engine Specification

The system natively supports complex task workflows represented as Directed Acyclic Graphs (DAGs).

```
                      +---------+
                      | Task A  | (Root)
                      +---------+
                       /       \
                      v         v
                 +---------+   +---------+
                 | Task B  |   | Task C  | (Fan-Out)
                 +---------+   +---------+
                      \         /
                       v       v
                      +---------+
                      | Task D  | (Fan-In)
                      +---------+
```

### 4.1. Graph Representation
A DAG is represented as an adjacency list with explicit reverse dependency tracking:
```go
type DAGDefinition struct {
    DAGID string
    Tasks map[string]*TaskSpec
}

type TaskSpec struct {
    TaskID       string
    Dependencies []string // Upstream tasks that MUST complete before this task
    Dependents   []string // Downstream tasks waiting on this task
}
```

### 4.2. Cycle Detection via Kahn’s Algorithm
During DAG submission (before proposing to Raft), the coordinator validates the graph for cycles:

```
Algorithm: Kahn's Topological Sort Cycle Detector
Input: Graph G = (V, E)
Output: Valid (true/false)

1. Compute in-degree for all vertices v in V:
   in_degree[v] = count of upstream dependencies for v
2. Initialize queue Q with all vertices where in_degree[v] == 0 (roots)
3. visited_count = 0
4. While Q is not empty:
     u = Q.Dequeue()
     visited_count++
     For each neighbor w in u.Dependents:
       in_degree[w]--
       If in_degree[w] == 0:
         Q.Enqueue(w)
5. If visited_count != |V|:
     Return CycleDetectedError (contains at least one directed cycle)
6. Return Valid
```
- **Time Complexity**: $O(|V| + |E|)$, where $|V|$ is the number of tasks and $|E|$ is the number of dependency edges.
- **Space Complexity**: $O(|V|)$ for in-degree and queue arrays.
- **Safety**: Cycles are rejected **prior** to any Raft consensus proposal, preventing deadlocked workflows from entering the system.

### 4.3. Fan-In, Fan-Out, and Dependency Completion Rules
1. **Root Tasks**: Any task with 0 dependencies is immediately marked `READY` and enqueued for scheduling.
2. **Fan-Out**: When Task A finishes with `SUCCEEDED`, the DAG engine inspects all tasks in `TaskA.Dependents`. For each downstream task (e.g. Task B and Task C), it checks if all their upstream dependencies have succeeded.
3. **Fan-In**: Task D depends on both Task B and Task C. When Task B succeeds, Task D remains in `BLOCKED` (1 unsatisfied dependency). Only when Task C also reaches `SUCCEEDED` does the engine promote Task D to `READY`.

### 4.4. Downstream Cancellation Policy

> [!IMPORTANT]
> **Explicit Engine Policy**: "A task whose declared dependency reaches terminal FAILED or CANCELLED is not eligible for successful execution. Under the current DAG policy, the downstream task is therefore transitively CANCELLED rather than remaining indefinitely BLOCKED."
>
> This behavior is an explicit product/engine design policy to prevent orphaned tasks and reclaim scheduler resources, rather than an inherent topological property of DAGs.

#### Policy Consistency Matrix

The following table defines the deterministic resolution of a downstream dependent task $D$ waiting on upstream dependencies:

| Scenario / Upstream Dependency States | Downstream Task $D$ State | Rationale & Engine Policy |
| :--- | :--- | :--- |
| **Dependency that is still RUNNING** | `BLOCKED` | Prerequisites are still actively in progress; downstream remains BLOCKED awaiting outcome. |
| **Retrying dependency (`RETRY_WAIT`)** | `BLOCKED` | Non-terminal failure; dependency has remaining retry attempts. Downstream remains BLOCKED until retry completes. |
| **Dependency that is FAILED (terminal)** | `CANCELLED` | Dependency reached terminal failure (`MaxRetries` exhausted). Task $D$ cannot satisfy its input contract; transitively cancelled. |
| **Dependency that is CANCELLED (terminal)** | `CANCELLED` | Dependency was cancelled by client or upstream cascade. Task $D$ cannot satisfy its input contract; transitively cancelled. |
| **One failed dependency + other successful dependencies** | `CANCELLED` | All dependencies are required (AND-join). Because one prerequisite has permanently failed, task $D$ is transitively cancelled despite other successes. |
| **One cancelled dependency + other successful dependencies** | `CANCELLED` | Because one prerequisite was cancelled, task $D$ cannot execute and is transitively cancelled despite other successes. |
| **Multiple failed dependencies** | `CANCELLED` | Handled idempotently on the first terminal failure encountered; downstream task $D$ is marked CANCELLED immediately. |
| **All dependencies `SUCCEEDED`** | `READY` | All prerequisites are satisfied; task $D$ is promoted from `BLOCKED` to `READY` and enqueued for dispatch. |
| **Independent sibling branch of a failed/cancelled branch** | `SUCCEEDED` (if runs to completion) | Sibling branches execute independently; only downstream descendants of the failed/cancelled task are pruned. |

### 4.5. Retries in DAGs: Non-Terminal Distinction

> [!NOTE]
> **Critical State Boundary**: When an upstream task fails but has retries remaining (`AttemptCount < MaxRetries`), it transitions to `RETRY_WAIT`.
> 
> Because `RETRY_WAIT` is **non-terminal**, downstream tasks remain safely in `BLOCKED`. Transitive downstream cancellation is **ONLY** triggered when an upstream task transitions to a terminal state (`FAILED` or `CANCELLED`). This preserves fault tolerance for transient network/worker failures while preventing deadlock on permanent failure.

