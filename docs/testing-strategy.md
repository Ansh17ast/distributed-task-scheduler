# Testing Strategy & Invariant Verification Plan

This document outlines the testing methodology, fault injection harness, concurrency analysis, and core invariant verification matrix for the Distributed Task Scheduler.

---

## 1. Testing Philosophy & Invariants

A distributed systems project cannot be defended in an interview based solely on happy-path demonstrations. Every core invariant of the scheduler must be backed by an automated test that purposefully induces failure and verifies mathematical correctness.

### The 12 Core Invariants

```
+========================================================================================================================+
| Invariant ID | Formal Invariant Statement                                             | Test Harness Category          |
+========================================================================================================================+
| INV-01       | Exactly one committed leader exists for any given Raft term.           | Consensus Test (Jepsen-style)  |
| INV-02       | A task cannot become RUNNING before it is READY and LEASED.            | State Machine Transition Matrix|
| INV-03       | A task cannot become READY while any upstream DAG dependency is not    | DAG Engine Unit & Adversarial  |
|              | in SUCCEEDED state.                                                    |                                |
| INV-04       | A committed task state transition cannot silently disappear after       | Raft Failover Integration Test |
|              | coordinator crash or restart.                                          |                                |
| INV-05       | Expired worker leases must trigger deterministic recovery within       | Monotonic Lease Reaper Test    |
|              | T_lease + T_grace + Delta.                                             |                                |
| INV-06       | A partitioned / stale coordinator cannot commit state transitions.     | Majority/Minority Partition    |
| INV-07       | Tenant concurrency limits (MaxConcurrency) are never exceeded under   | Multi-Tenant Concurrency Test  |
|              | burst loads.                                                           |                                |
| INV-08       | DAG cycles (direct, indirect, or self-referential) are strictly        | Kahn's Algorithm Adversarial   |
|              | rejected at submission time.                                           |                                |
| INV-09       | Any state transition not defined in the transition matrix is rejected | Exhaustive State Mutation Test |
|              | with ErrInvalidStateTransition.                                        |                                |
| INV-10       | Task recovery cannot permanently drop a task whose retry count has     | Worker Kill Chaos Test         |
|              | not reached MaxRetries.                                                |                                |
| INV-11       | Duplicate completion / ack messages from workers are completely        | Idempotency Unit Test          |
|              | idempotent and do not duplicate side effects.                          |                                |
| INV-12       | Completion reports from stale lease epochs (LeaseEpoch < current) must  | Fencing Token Rejection Test   |
|              | be rejected with ErrStaleLeaseEpoch.                                   |                                |
+========================================================================================================================+
```

---

## 2. Testing Pyramid & Execution Levels

```
                     / \
                    /   \
                   / Chaos\       Level 4: Fault Injection & Network Partitions
                  /--------\
                 /  Integ   \     Level 3: Multi-Process Coordinator + Worker Flows
                /------------\
               /  Concurrency \   Level 2: Goroutine Race Detector (-race) & Locks
              /----------------\
             /    Unit Tests    \ Level 1: Deterministic State Machine, DAG & Scheduler
            +--------------------+
```

### Level 1: Unit Tests (Deterministic, Zero Sleeps)
- **State Machine Matrix**: Runs an $11 \times 11$ matrix test attempting every possible transition $(S_i \to S_j)$. Asserts that valid transitions update state and invalid transitions return `ErrInvalidStateTransition`.
- **DAG Cycle Engine**: Tests linear pipelines, diamond topologies, disconnected components, self-referential tasks ($A \to A$), and indirect cycles ($A \to B \to C \to A$). Asserts Kahn’s algorithm detects and rejects all cyclic variants.
- **Scheduler Policies**: Evaluates FIFO, Priority, and DRR Fair-Share schedulers with fixed seed inputs to assert exact ordering and tenant isolation.

### Level 2: Concurrency & Race Detection
- Executed with `go test -race ./...`.
- Spawns hundreds of concurrent goroutines submitting tasks, pulling tasks, and acknowledging completions to verify zero data races across mutexes, channels, and FSM maps.

### Level 3: Integration Tests
- Boots an in-memory 3-node Raft coordinator cluster and 3 worker daemons.
- Runs end-to-end task execution: submission $\to$ lease $\to$ worker execution $\to$ completion $\to$ audit log verification.
- Tests retry backoff: task fails, transitions to `RETRY_WAIT`, waits for backoff, transitions to `READY`, and succeeds on retry.

### Level 4: Chaos & Fault Injection
- **Worker Crash**: Simulates process termination mid-task. Verifies lease expiration, task transition to `LOST` $\to$ `RECOVERING`, and re-assignment to another worker.
- **Leader Crash**: Kills the current Raft leader process mid-flight. Verifies new leader election within 300ms and verifies that in-flight and committed tasks resume execution without loss.
- **Network Partition**: Simulates a network split isolating Node 1. Verifies that Node 1 cannot commit writes, while Nodes 2 and 3 continue committing. Partition heals, and Node 1 synchronizes seamlessly.

---

## 3. Fault Injection Framework Architecture

To enable repeatable fault-injection tests on any host without requiring root permissions, we implement a lightweight in-process TCP proxy and interceptor (`pkg/netutil/chaos`):

```go
type ChaosTransport struct {
    mu         sync.Mutex
    partitions map[string]map[string]bool // [src][dst] == partitioned
    dropRate   float64
    latency    time.Duration
}

func (c *ChaosTransport) PartitionNode(nodeID string) {
    c.mu.Lock()
    defer c.mu.Unlock()
    // Drops all inbound and outbound packets for nodeID
}

func (c *ChaosTransport) HealPartition() {
    c.mu.Lock()
    defer c.mu.Unlock()
    c.partitions = make(map[string]map[string]bool)
}
```

This transport can be passed directly to `hashicorp/raft` and gRPC dialers during automated test runs.

---

## 4. Benchmark Methodology

Benchmarks measure actual system performance under real load, avoiding simulated or theoretical claims:

1. **Submission Throughput Benchmark**: Measures tasks submitted per second across 1 to 32 concurrent client workers.
2. **Dispatch Latency Benchmark**: Measures end-to-end latency from client submission to worker receipt (measuring P50, P95, and P99 percentiles).
3. **Failover Duration Benchmark**: Measures elapsed time between leader termination and the new leader's first successful write commit.
4. **DAG Resolution Benchmark**: Evaluates DAG topological sort and promotion latency for graphs ranging from 10 to 10,000 nodes.
