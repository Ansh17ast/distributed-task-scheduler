# Distributed Fault-Tolerant Task Scheduler

A high-performance, fault-tolerant distributed task scheduler built from scratch in Go. Designed around a replicated 3-node HashiCorp Raft consensus core, a worker-pull scheduling loop, lease-based task execution, dual-token zombie worker fencing, and multi-tenant fairness.

Tested under adversarial chaos engineering (process kills, network partitions, split-brain isolation, asymmetric network blackholes) and validated under real workloads up to **10,000 tasks** with complete production observability (Prometheus, OpenTelemetry, structured JSON logging).

---

## Key Highlights & Architectural Features

- **Consensus Core**: 3-node HashiCorp Raft cluster ($Q = 2$) over TCP with persistent BoltDB write logs, stable state, and automatic snapshot compaction.
- **Deterministic FSM**: State machine operations are fully serialized, side-effect free, and deterministic across all replicas.
- **Worker-Pull Model**: Workers pull tasks based on available local execution slots (`available_slots > 0`), eliminating worker-side task queue buildup and head-of-line blocking.
- **Clock-Independent Leases**: Dynamic task leases with monotonic heartbeat tracking and failover reconciliation windows that avoid wall-clock synchronization risks.
- **Dual-Token Fencing**: Zombie workers and partitioned nodes are strictly fenced using process incarnation `SessionID` and monotonically increasing `LeaseEpoch` tokens.
- **DAG Workflow Engine**: Supports complex dependency graphs (e.g., Diamond DAGs) with cycle validation and automatic downstream promotion upon upstream task completion.
- **Multi-Tenant Fairness**: Prevents noisy neighbor starvation using Deficit Weighted Round Robin (DWRR) and priority aging.
- **Production Observability**: Integrated HTTP telemetry server providing Prometheus metrics (strict low-cardinality labels), OpenTelemetry distributed tracing, `/healthz`, `/readyz`, `/status`, and `/metrics`.
- **Chaos Hardened**: Comprehensive 12-suite chaos test harness validating leader failover, split-brain immunity, and partition recovery with zero data loss.

---

## System Architecture

```
                                  +--------------------------+
                                  | External Client / API GW |
                                  +-------------+------------+
                                                |
                                   (Linearizable gRPC API)
                                                v
                               +----------------------------------+
                               | Coordinator Leader (Term N)      |
                               |  - Raft Leader Consensus         |
                               |  - Authoritative State Machine   |
                               |  - In-Memory Ready Priority Queue|
                               |  - Monotonic Worker Lease Reaper |
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

### Task State Machine Lifecycle

```
            +-------------+
            |   PENDING   |
            +------+------+
                   |
            (Root Admitted)
                   v
            +-------------+
            |    READY    | <------------------------------------+
            +------+------+                                      |
                   |                                             |
            (Worker Pulls)                                       |
                   v                                             |
            +-------------+       (Lease Expired / Worker Crash) |
            |   LEASED    | -------------------------------------+
            +------+------+                                      |
                   |                                             |
            (Worker Acks)                                        |
                   v                                             |
            +-------------+       (Lease Expired / Worker Crash) |
            |   RUNNING   | -------------------------------------+
            +------+------+                                      |
                   |                                             |
      +------------+------------+                                |
      |                         |                                |
(Work Succeeds)          (Work Fails)                            |
      v                         v                                |
+-----------+            +-------------+  (Attempt < MaxRetries) |
| SUCCEEDED |            | RETRY_WAIT  | ------------------------+
+-----------+            +------+------+
                                |
                         (Max Retries Exceeded)
                                v
                         +-------------+
                         |   FAILED    |
                         +-------------+
```

---

## Measured Benchmark Results

All metrics represent **actual observed measurements** from automated benchmark and load test runs on a 3-coordinator cluster:

### End-to-End Pipeline Latency Breakdown (300 Samples)

| Pipeline Stage | Implementation Detail | p50 | p95 | p99 |
| :--- | :--- | :---: | :---: | :---: |
| **1. Submission Latency** | Client gRPC -> Leader RPC -> Raft replication | `< 500 µs` | `538.3 µs` | `707.0 µs` |
| **2. Raft Quorum Commit Latency** | Direct Raft proposal -> Quorum commit | `< 500 µs` | `514.0 µs` | `555.5 µs` |
| **3. Scheduling Latency** | `ReadyQueue.SelectNextTask` priority evaluation | `< 100 µs` | `< 100 µs` | `515.0 µs` |
| **4. Worker Assignment Latency** | Worker `PullTask` RPC + `OpGrantTaskLease` | `< 500 µs` | `557.2 µs` | `1.30 ms` |
| **5. Execution Latency** | Worker Ack -> Work payload -> ReportCompleted | `1.03 ms` | `2.09 ms` | `2.45 ms` |
| **6. Total Completion Latency** | Turnaround time from submission to complete | `1.51 ms` | `2.35 ms` | `2.73 ms` |

### Load Testing & Scale Metrics

| Metric / Scenario | Measured Value | Operational Meaning |
| :--- | :---: | :--- |
| **Burst Submission Throughput** | **`2,171 tasks/sec`** | 1,000 tasks burst-submitted sequentially (p95: `1.28 ms`). |
| **Concurrent Multi-Tenant** | **`3,559 tasks/sec`** | 10 concurrent clients submitting 2,000 tasks (0 errors). |
| **Sustained Scale Benchmark** | **`10,000 tasks in 8.67s`** | **`1,152 tasks/sec`** sustained end-to-end execution. |
| **Worker Scaling (1 -> 5 -> 20)** | `1,016 -> 1,980 -> 2,685` | Sub-linear capacity scaling up to consensus limits. |
| **Diamond DAG Execution (50 Tasks)**| **`4,159 tasks/sec`** | 1 Root -> 48 Middle branches -> 1 Sink in 0.012s. |
| **Leader Failover Time** | **`135.8 ms`** | Automated election and quorum establishment. |
| **Resident Memory Footprint** | **`66.62 MB`** | Heap allocation during 10,000-task sustained execution. |
| **Goroutine Leak Audit** | **`0 leaks`** (41 start / 41 end) | Stable goroutine counts under heavy load. |

---

## Repository Structure

```
.
├── api/proto/v1/          # Protocol Buffer definitions (CoordinatorService, WorkerService)
├── cmd/
│   ├── coordinator/       # Coordinator daemon entrypoint
│   ├── worker/            # Worker daemon entrypoint
│   ├── demo_chaos/        # Phase 8 live TCP/BoltDB chaos resilience demonstration
│   └── demo_leader_failover/ # Live leader kill and recovery demonstration
├── internal/
│   ├── chaos/             # 12-suite chaos engineering & fault-injection harness
│   ├── config/            # Runtime configuration definitions and validators
│   ├── consensus/         # HashiCorp Raft node wrapper, FSM, and commands
│   ├── coordinator/       # gRPC server, leader reconciliation, reaper, harness
│   ├── dag/               # Directed Acyclic Graph validation and promotion engine
│   ├── domain/            # Core entities (Task, Worker, Lease, DAG, TenantQuota)
│   ├── scheduler/         # PriorityQueue, FIFO, and Fair (DWRR) policies
│   ├── state/             # In-memory authoritative state store with fine-grained locks
│   ├── storage/           # Persistent audit store and BoltDB wrappers
│   ├── telemetry/         # Prometheus metrics, OpenTelemetry tracing, HTTP server
│   └── worker/            # Worker daemon, slot manager, and lease renewal loops
├── tests/
│   ├── benchmarks/        # End-to-end latency breakdown and profiling suites
│   └── load/              # High-throughput load tests up to 10,000 tasks scale
└── docs/
    ├── operations.md      # Deployment, operations, and configuration reference
    ├── observability.md   # Prometheus metrics catalog, OTel tracing, alerting
    ├── performance.md     # In-depth benchmark results, latency breakdown, profiling
    ├── failure-playbook.md# Incident response runbooks for cluster failure modes
    └── interview-guide.md # 18 deep-dive distributed systems Q&A answers
```

---

## Getting Started

### Prerequisites
- Go 1.22+ (tested on Go 1.27.1)
- Protocol Buffer compiler (optional, generated code included)

### Building the Binaries
```powershell
go build -o bin/coordinator.exe ./cmd/coordinator
go build -o bin/worker.exe ./cmd/worker
```

### Running Automated Tests
```powershell
# Run all unit, integration, and cluster tests
go test -count=1 ./...

# Run the 12-scenario Chaos Engineering suite
go test -v -count=1 ./internal/chaos

# Run the End-to-End Pipeline Latency benchmark
go test -v -run TestE2EPipelineLatencyBreakdown ./tests/benchmarks

# Run the 10,000-Task Load Test suite
go test -v -timeout 120s ./tests/load
```

### Running Live Demos
```powershell
# Phase 8 Live Chaos Resilience Demo (Real TCP, Persistent BoltDB, Leader Kill & Rejoin)
go run ./cmd/demo_chaos/main.go

# Phase 7 Leader Failover Demonstration
go run ./cmd/demo_leader_failover/main.go
```

---

## Detailed Documentation

For comprehensive technical deep dives, consult the documentation suite:
- [Operations Guide](docs/operations.md): Deployment topologies, configuration options, health endpoints, and maintenance.
- [Observability Architecture](docs/observability.md): Prometheus metric inventory, OpenTelemetry tracing, and alerting rules.
- [Performance & Benchmarks](docs/performance.md): Measured latency breakdowns, load testing results, and pprof profiling.
- [Failure Playbook](docs/failure-playbook.md): Step-by-step incident response runbooks for partitions, crashes, and zombie workers.
- [Interview Guide](docs/interview-guide.md): Concise, technically defensible answers to 18 advanced distributed systems questions.
