# Query Service ↔ Engine Integration Evaluation Plan

> **Date**: 2026-02-18  
> **Scope**: Evaluate correctness and production-readiness of the integration between the Arrow Flight query service (`worker-flight`) and the query engine (`worker-storage`), including distributed (LDP) execution, control plane readiness, and the Flight API contract required by the future SQL Gateway.

---

## 1. Production Architecture Context

### 1.1 End-to-End Query Flow

```
                         ┌──────────────┐
                         │  SQL Gateway  │
                         │  (stateless)  │
                         │  meta-cache   │
                         └──────┬───────┘
                                │  selects coordinator
                   ┌────────────┼────────────┐
                   ▼            ▼            ▼
             ┌──────────┐ ┌──────────┐ ┌──────────┐
             │ Worker-1  │ │ Worker-2  │ │ Worker-3  │
             │ (coord)   │ │ (part.)   │ │ (part.)   │
             │ LDP+DuckDB│ │ DuckDB    │ │ DuckDB    │
             └──────────┘ └──────────┘ └──────────┘
                   ▲            ▲            ▲
                   └────────────┼────────────┘
                                │  Arrow Flight DoAction / DoGet
                         ┌──────┴───────┐
                         │ Control Plane │
                         │  (leibrix)    │
                         └──────────────┘
```

- All queries enter through the **SQL Gateway**, which is a stateless proxy holding only a **meta-cache** of epoch→worker and tenant→worker mappings, sourced from the control plane's bidirectional stream.
- The gateway selects a **coordinator worker** per query (strategies: link weight, CPU load, random, data-locality-aware) and forwards the SQL to that worker's Flight endpoint.
- The coordinator plans (LDP) and orchestrates execution across participant workers, then streams results back through the gateway to the client.
- Workers are designed to be **restartable at any time**. If a worker goes offline, the gateway re-routes to an available node. Users never observe infrastructure failures.

### 1.2 SQL Gateway Design Rationale

The SQL Gateway exists to **completely shield users from infrastructure dynamics**:

| Property | Implication |
|----------|-------------|
| **Stateless** | Only meta-cache, no persistent state. Horizontally scalable, any instance replaceable. |
| **Meta-cache from CP stream** | Subscribes to the same `CoordinateWorker` bidi stream as workers. Receives `DataAssignmentEvent`s to build epoch→worker mappings. |
| **Failover-transparent** | If a coordinator dies mid-query, the gateway retries on a different worker. |
| **Coordinator selection** | Influences data locality — prefer the worker holding the most relevant epochs to reduce network exchanges. |
| **Unified entry** | Clients see a single Flight endpoint. The distributed nature of the cluster is invisible. |

### 1.3 Worker-Side Obligations for Gateway Compatibility

The SQL Gateway itself is **out of scope** for this worker repository (it will be a standalone repo). This plan defines the **Flight API contract** that workers must satisfy:

| Gateway Requirement | Worker-Side Obligation |
|---------------------|------------------------|
| Any worker can be coordinator | `LdpCoordinator` must be wirable into every worker's Flight service |
| Workers are restartable anytime | Graceful shutdown must drain queries; stale Flight connections must be recoverable |
| Gateway retries on different worker | `DoGet`/`DoAction` must be idempotent or safely retriable (read-only queries are inherently safe) |
| Gateway routes by meta-cache | Worker must advertise epoch inventory via CP heartbeat |
| Per-query coordinator selection | `LdpCoordinator` must accept per-query cluster metadata, not assume static topology |
| Gateway needs cluster topology | CP proto must support topology observation by gateway instances |

---

## 2. Current Integration Status

### 2.1 What Works

| Component | Status | Evidence |
|-----------|--------|----------|
| Single-node SQL via Flight (`DoGet` → `SqlTransformer` → DuckDB) | ✅ Working | `basic_server.rs` example |
| `SqlTransformer` rewrites logical tables to `scan_<table>()` macros | ✅ Working | Tested in SQL module tests |
| `DuckDBQueryEngine` streams Arrow batches via connection pool | ✅ Working | `engine_accuracy_tests.rs` |
| `SharedDatabase` with r2d2 pool (32 connections, shared in-memory DB) | ✅ Working | `engine_concurrency_tests.rs` |
| LDP planner — distribution annotation, exchange selection, stage cutting | ✅ Working | `ldp_planner_correctness_test.rs` |
| LDP local execution — `LocalStageExecutor` with per-worker `SharedDatabase` | ✅ Working | `ldp_e2e_exchange_test.rs`, `ldp_e2e_tpch_join_test.rs` |
| Arrow IPC serialization for exchange data in proto | ✅ Working | `proto_convert.rs` |
| Flight ticket encoding/decoding (JSON, backward-compatible) | ✅ Working | `ticket.rs` |
| Error mapping (DuckDB/SQL/Timeout → gRPC status codes) | ✅ Working | `error.rs` |
| SQL admission control (reject recursive CTEs, require date predicates) | ✅ Working | `admission.rs` |
| SQL transformation idempotency (`transform(transform(Q)) = transform(Q)`) | ✅ Working | Design doc guarantee |

### 2.2 Critical Bugs

#### Bug A: Stage DuckDB Isolation (Critical — Blocks Distributed Execution)

- **Location**: `src/worker-flight/src/service.rs` — `execute_stage_with_duckdb()` method
- **Problem**: When a coordinator submits a stage to a participant worker via `DoAction("submit_stage")`, the handler creates a **standalone** `Connection::open_in_memory()`. This is an entirely separate DuckDB database — it cannot access epoch tables or scan macros that exist in the worker's `SharedDatabase`.
- **Impact**: Any stage SQL referencing real data tables (e.g., `SELECT * FROM scan_orders('2025-01-01','2025-02-01')`) fails with "table not found" on the participant worker.
- **Root Cause**: The method bypasses `SharedDatabase` and creates an isolated DuckDB connection. Exchange input tables registered via `register_arrow_batches()` work (they're created fresh), but local catalog data is invisible.
- **Remediation**: Pass `Arc<SharedDatabase>` into `WorkerFlightService` and use `shared_db.get()` instead of `Connection::open_in_memory()`. This avoids polluting the `QueryEngine` trait with implementation details while giving stage execution access to the worker's loaded data.

#### Bug B: `fetch_output` Query ID Mismatch (Critical — Blocks Result Retrieval)

- **Location**: `src/worker-storage/src/ldp/executor/flight.rs` — `fetch_output()` method
- **Problem**: The `query_id` is fabricated as `format!("query_{}", ticket.stage_id)` instead of using the actual `ticket.query_id`. The `submit_to_worker()` method caches tickets using the real `query_id`, so `fetch_output` will **always** fail with "No ticket cached for stage X on worker Y" because the lookup keys never match.
- **Impact**: After successfully submitting and executing a stage, the coordinator can never retrieve the results.
- **Code Comment**: The code itself acknowledges this: *"We need to track which query this ticket belongs to. For now, we reconstruct from stage_id (this is a limitation we'll address)"*.
- **Remediation**: Replace the fabricated `query_id` with `ticket.query_id` (the field exists on `StageTicket`).

### 2.3 Architectural Gaps

#### Gap C: No Coordinator in Flight Service

- **Location**: `src/worker-flight/src/service.rs` — `WorkerFlightService<Q>`
- **Problem**: `WorkerFlightService` holds `Arc<Q: QueryEngine>` and `Arc<SqlTransformer>` but has **no reference** to `LdpCoordinator`. A client calling `DoGet` always gets local single-node execution. There is no mechanism to trigger distributed execution from a Flight request.
- **Impact**: The SQL Gateway will have no way to invoke distributed query execution — it can only trigger single-node queries.
- **Remediation**: Add `Option<Arc<LdpCoordinator<M>>>` to `WorkerFlightService`. When present and the query spans multiple workers (determined by metadata), delegate to `LdpCoordinator::execute_query()` instead of local execution. This preserves the unified Flight entry point that the gateway programs against.

#### Gap D: `SqlTransformer` Immutability

- **Location**: `WorkerFlightService` wraps `Arc<SqlTransformer>` (immutable after construction)
- **Problem**: Control plane events need to register new datasets at runtime as `DataAssignmentEvent`s arrive. The current design prevents post-startup mutation.
- **Remediation**: Change to `Arc<RwLock<SqlTransformer>>` or use `DashMap` internally for the dataset registry.

#### Gap E: Stale Connection on Retry

- **Location**: `src/worker-storage/src/ldp/executor/flight.rs` — `WorkerConnectionPool`
- **Problem**: Connections are cached indefinitely. If a worker restarts, the cached `FlightServiceClient<Channel>` becomes stale. `execute_stage_with_retry` retries on the same broken connection because `get_connection()` returns the cached entry. `remove_connection()` exists but is never called on error.
- **Remediation**: On connection error in `submit_to_worker()`, call `remove_connection(worker_id)` before returning the error, so the next retry attempt creates a fresh connection.

#### Gap F: Distributed Stage Cancellation

- **Location**: `src/worker-storage/src/ldp/executor/coordinator.rs` — `cancel_query()`
- **Problem**: For distributed execution, stage cancellation logs a warning: *"Distributed stage cancellation not fully implemented"*. Remote workers continue executing cancelled stages.
- **Remediation**: Implement `DoAction("cancel_query")` propagation to participant workers via Flight.

---

## 3. Evaluation Layers

### Layer 1: Single-Node Flight Integration Tests

**Objective**: Verify the complete `DoGet` → `SqlTransformer` → `DuckDB` → Arrow streaming path works correctly under realistic conditions.

**Test Cases**:

| # | Test | What It Validates | Expected Result |
|---|------|-------------------|-----------------|
| 1.1 | Simple SELECT with registered dataset | End-to-end Flight query execution | Correct rows returned |
| 1.2 | Multi-table query (JOIN two registered datasets) | SQL transformation handles multiple tables | Correct join result |
| 1.3 | Query with subqueries referencing datasets | Nested SQL transformation | Correct results |
| 1.4 | Query referencing unregistered table | Error propagation | gRPC `NOT_FOUND` status |
| 1.5 | Malformed SQL | Parse error → Flight error | gRPC `INVALID_ARGUMENT` status |
| 1.6 | Query exceeding timeout | Timeout supervision | gRPC `DEADLINE_EXCEEDED` status |
| 1.7 | 20 concurrent `DoGet` requests | Connection pool contention | All queries return correct results |
| 1.8 | `GetFlightInfo` → `DoGet` round-trip | Schema matches data | Schema from FlightInfo matches returned batches |
| 1.9 | `ListFlights` with registered datasets | Dataset catalog | All registered datasets listed |
| 1.10 | `DoGet` with wrong tenant_id | Tenant validation | gRPC `PERMISSION_DENIED` |
| 1.11 | Empty result set | Edge case handling | Empty batch stream, valid schema |
| 1.12 | Large result (100K+ rows) | Streaming correctness | All rows received, no truncation |

**Approach**: Integration test that:
1. Creates `SharedDatabase` + populates with epoch data + scan macros
2. Creates `DuckDBQueryEngine` + `SqlTransformer`
3. Starts `FlightServerBuilder` on random port (`0.0.0.0:0`)
4. Connects Arrow Flight client
5. Exercises each test case

**Key files to create**: `src/worker-flight/tests/flight_integration_test.rs`

---

### Layer 2: Stage Submission + Result Retrieval via Flight

**Objective**: Verify the distributed participant path: `DoAction("submit_stage")` → stage execution → `DoGet(StageResultTicket)`.

**Prerequisites**: Fix Bug A (DuckDB isolation) and Bug B (query_id mismatch) first.

**Test Cases**:

| # | Test | What It Validates | Expected Result |
|---|------|-------------------|-----------------|
| 2.1 | Submit stage with exchange-only inputs | Stage execution on fresh data | Correct results from exchange tables |
| 2.2 | Submit stage referencing local epoch data | SharedDatabase access (Bug A fix) | Stage reads from worker's loaded data |
| 2.3 | Submit stage + retrieve via DoGet | Full round-trip (Bug B fix) | Exact same RecordBatch data |
| 2.4 | Submit stage with wrong tenant_id | Tenant validation | gRPC `PERMISSION_DENIED` |
| 2.5 | Submit stage exceeding output row limit | StageLimits enforcement | Error with limit exceeded message |
| 2.6 | Submit stage exceeding output byte limit | StageLimits enforcement | Error with limit exceeded message |
| 2.7 | Submit stage with timeout | Timeout enforcement | gRPC `DEADLINE_EXCEEDED` |
| 2.8 | Retrieve result twice (retry scenario) | Destructive vs non-destructive read | Evaluate: should second read succeed? |
| 2.9 | Submit 10 stages concurrently | Concurrent stage execution | All complete correctly |
| 2.10 | Submit stage with empty exchange inputs | Edge case | Valid empty result or correct error |

**Approach**: Integration test starting a Flight server with a pre-loaded `SharedDatabase`, submitting stages via Flight client.

**Key files to create**: `src/worker-flight/tests/stage_submission_test.rs`

---

### Layer 3: Coordinator → Flight → Participant (Distributed E2E)

**Objective**: Verify the full distributed query path: `LdpCoordinator.execute_query()` → plan → submit stages to Flight workers → resolve exchanges → return results.

**Prerequisites**: Bug A fix, Bug B fix, Gap E fix (stale connection removal).

**Test Cases**:

| # | Test | What It Validates | Expected Result |
|---|------|-------------------|-----------------|
| 3.1 | 2-worker hash join (TPC-H Q3 pattern) | Distributed join with HashPartition exchange | Correct join results matching single-node reference |
| 3.2 | 3-worker aggregation with Gather exchange | Multi-worker aggregation | Correct aggregate matching reference |
| 3.3 | Broadcast join (small dimension table) | Broadcast exchange + join | Correct results, dimension replicated |
| 3.4 | Coordinator has data (data locality) | Coordinator is also a participant | Coordinator's local data included in result |
| 3.5 | Worker failure mid-query (simulated) | Retry + error propagation | Clear error after retry exhaustion |
| 3.6 | Parallel stage execution (independent stages) | Topological level parallelism | All stages execute, correct ordering |
| 3.7 | Multi-stage pipeline (3+ stages) | Complex plan with multiple exchanges | Correct final result |
| 3.8 | Query with CTE referencing distributed data | CTE handling in distributed plan | Correct results |
| 3.9 | Window function requiring Singleton distribution | Gather + compute | Correct window result |
| 3.10 | Empty partitions on some workers | Skew handling | Correct results despite empty inputs |

**Approach**: Multi-process test:
1. Start 2-3 Flight servers (each with its own `SharedDatabase` + loaded epoch data)
2. Create `LdpCoordinator` with `WorkerConnectionPool` pointing to these servers
3. Execute queries and compare against single-node reference results

**Key files to create**: `src/worker-storage/tests/distributed_flight_e2e_test.rs`

---

### Layer 4: Exchange Runtime Correctness (Distributed Mode)

**Objective**: Verify data movement correctness when exchanges operate through Flight rather than in-process.

**Test Cases**:

| # | Test | What It Validates | Expected Result |
|---|------|-------------------|-----------------|
| 4.1 | Gather: 3 workers → 1 coordinator | All partitions collected | Row count = sum of all partitions |
| 4.2 | HashPartition: deterministic routing | Same key always routes to same partition | Re-run produces identical partition assignments |
| 4.3 | HashPartition: NULL key handling | NULLs routed consistently | NULLs all land in one partition |
| 4.4 | Broadcast: data replicated to all targets | Every target gets full copy | Each target has identical data |
| 4.5 | Arrow IPC round-trip fidelity | Schema preservation across serialization | All types (including nested, dictionary) preserved |
| 4.6 | Large exchange data (>16MB, exceeds single gRPC message) | Chunking / streaming | Data fully transferred |
| 4.7 | Empty exchange input | Edge case | No error, downstream handles empty input |

**Approach**: Extend existing `ldp_e2e_exchange_test.rs` tests to use `FlightStageExecutor` in distributed mode.

---

### Layer 5: Control Plane Integration Assessment

**Objective**: Evaluate readiness of the CP integration for production use.

#### 5.1 Current State

| Component | Status | Detail |
|-----------|--------|--------|
| `worker-cp` crate | ❌ Stub | Single-line re-export: `pub use worker_proto::proto;` |
| CP client implementation | ❌ Missing | No `CoordinateWorker` stream logic |
| Worker registration | ❌ Missing | No `RegisterEvent` sending |
| Heartbeat loop | ❌ Missing | `HeartbeatEvent` exists in proto but is empty (no health data) |
| DataAssignment handling | ❌ Missing | No event handler to trigger data loading |
| Data loader adapters | ❌ Missing | StarRocks, Iceberg, JDBC all `todo!()` |
| Dataset registration at runtime | ❌ Missing | `SqlTransformer` is immutable post-construction |
| Epoch metadata sync | ❌ Missing | `ClusterMetadata` not populated from CP |
| Graceful shutdown coordination | ❌ Missing | No coordinated lifecycle management |

#### 5.2 Proto Gaps for Gateway Support

The SQL Gateway subscribes to the same `CoordinateWorker` bidi stream to build its meta-cache. Current proto gaps:

| Gap | Description | Remediation |
|-----|-------------|-------------|
| Empty `HeartbeatEvent` | Contains no health data — gateway needs CPU/memory/load for coordinator selection | Add fields: `memory_used_bytes`, `memory_total_bytes`, `active_query_count`, `loaded_epochs: Vec<EpochInfo>` |
| No topology broadcast | Gateway cannot discover when workers join/leave | Add `TopologyChangeEvent` to `EventStreamMessage` or use heartbeat absence (timeout-based detection) |
| No epoch inventory in heartbeat | Gateway meta-cache cannot be refreshed | Add `loaded_datasets: Vec<DatasetEpochSummary>` to heartbeat |
| DataAssignment is per-worker | Gateway doesn't receive these unless it subscribes | Confirm gateway can subscribe to same bidi stream and receive mirror events |

#### 5.3 Required CP Integration Work

| Task | Priority | Estimated Effort |
|------|----------|-----------------|
| Implement `ControlPlaneClient` with bidi stream management | P0 | 2-3 days |
| Implement `RegisterEvent` sending on startup | P0 | 0.5 day |
| Implement heartbeat loop with health metrics | P0 | 1 day |
| Enrich `HeartbeatEvent` proto with health fields | P0 | 0.5 day |
| Implement `DataAssignmentEvent` handler → trigger `DataLoader` | P0 | 1-2 days |
| Implement StarRocks `SourceAdapter` | P0 | 2-3 days |
| Runtime dataset registration (`SqlTransformer` + DuckDB macros) | P1 | 1 day |
| Populate `ClusterMetadata` from CP epoch assignments | P1 | 1 day |
| CP stream reconnection with exponential backoff | P1 | 0.5 day |
| Graceful shutdown: drain queries → shutdown engine → close CP stream | P1 | 1 day |

---

### Layer 6: CLI Runtime Wiring

**Objective**: Evaluate the worker binary startup path — does `liebrix-worker run` produce a functional worker?

#### 6.1 Current State

`src/worker-cli/src/main.rs` parses CLI config but starts nothing:
```rust
fn main() -> anyhow::Result<()> {
    let cli = config::Cli::parse();
    match cli.command {
        config::Command::Run(args) => {
            let _cfg = config::LeibrixWorkerConfig::from_run_args(args)?;
            println!("Hello, Worker CLI!");
        }
    }
    Ok(())
}
```

#### 6.2 Required Runtime Wiring

A `WorkerRuntime` struct should own and orchestrate all components:

```
WorkerRuntime
├── SharedDatabase           ← from DuckDBConfig
├── MemoryDuckDBEngine       ← StorageEngine actor (write path)
├── DuckDBQueryEngine        ← QueryEngine (read path, shares SharedDatabase)
├── SqlTransformer           ← Arc<RwLock<...>>, datasets from CP events
├── LdpCoordinator           ← optional, when worker is selected as coordinator
├── WorkerFlightService      ← holds QueryEngine + SqlTransformer + LdpCoordinator
├── FlightServer             ← tonic server on query_listen_addr
├── ControlPlaneClient       ← bidi stream to master
├── DataLoader               ← triggered by DataAssignment events
└── ShutdownCoordinator      ← orchestrates graceful shutdown
```

**Startup sequence**:
1. Parse CLI config → `LeibrixWorkerConfig`
2. Create `SharedDatabase` from `DuckDBConfig`
3. Spawn `MemoryDuckDBEngine` actor thread
4. Create `DuckDBQueryEngine`
5. Create `SqlTransformer` (empty)
6. Create `ClusterMetadata` (empty)
7. Create `LdpCoordinator` with metadata + query engine
8. Create `WorkerFlightService` with query engine, transformer, coordinator, `SharedDatabase`
9. Start Flight server (background)
10. Start CP client stream (background)
11. Await shutdown signal (SIGTERM/SIGINT)
12. Drain: stop accepting new queries → wait for active queries → shutdown engine → close CP stream

**Verification**:
```bash
cargo run --bin liebrix-worker -- run \
  --tenant-id test-tenant \
  --worker-id worker-1 \
  --master-endpoint http://localhost:9090 \
  --query-listen-addr 0.0.0.0:8815
```
- Worker should start Flight server, connect to CP, and accept queries.
- Without CP (master offline): Worker should start Flight server with warning, accept local queries with pre-loaded data.

---

## 4. Flight API Contract for SQL Gateway

The SQL Gateway will program against these Flight RPCs. This is the **stable interface contract**.

### 4.1 Query Execution (Gateway → Coordinator Worker)

| RPC | Purpose | Request | Response |
|-----|---------|---------|----------|
| `GetFlightInfo` | Get schema + ticket for a query | `FlightDescriptor` with SQL bytes | `FlightInfo` with `QueryTicket` |
| `DoGet` | Execute query and stream results | `Ticket` containing `QueryTicket` JSON | Stream of `FlightData` (Arrow batches) |
| `DoAction("health_check")` | Worker liveness probe | Empty body | `"OK"` bytes |
| `DoAction("cancel_query")` | Cancel in-flight query | `query_id` bytes | Ack |
| `ListFlights` | Discover available datasets | `Criteria` (optional filter) | Stream of `FlightInfo` per dataset |

### 4.2 Stage Execution (Coordinator → Participant Worker)

| RPC | Purpose | Request | Response |
|-----|---------|---------|----------|
| `DoAction("submit_stage")` | Execute an LDP stage on this worker | `SubmitStageRequest` proto bytes | `SubmitStageResponse` with `StageResultTicket` |
| `DoGet` | Retrieve stage execution results | `Ticket` containing `StageResultTicket` JSON | Stream of `FlightData` (Arrow batches) |

### 4.3 QueryTicket Schema

```json
{
  "type": "query",
  "sql": "SELECT * FROM orders WHERE dt >= '2025-01-01'",
  "tenant_id": "tenant-abc",
  "memory_limit_mb": 1024,
  "timeout_seconds": 300
}
```

### 4.4 StageResultTicket Schema

```json
{
  "type": "stage_result",
  "tenant_id": "tenant-abc",
  "query_id": "q-12345",
  "stage_id": 3,
  "partition": null
}
```

### 4.5 Gateway Retry Contract

| Scenario | Gateway Behavior | Worker Requirement |
|----------|------------------|--------------------|
| Worker unreachable | Route to different coordinator | None (worker is offline) |
| `DoGet` returns `UNAVAILABLE` | Retry same worker, then re-route | Connection must be droppable |
| `DoGet` returns `DEADLINE_EXCEEDED` | Cancel + re-route to different coordinator | `cancel_query` must clean up resources |
| `DoGet` returns `INTERNAL` | Log error, re-route | DuckDB errors should be deterministic |
| Worker restarts mid-query | Gateway detects via health_check failure, re-routes | Graceful shutdown must drain or abort active queries |

### 4.6 Idempotency Guarantees

All queries are **read-only** (writes rejected by admission control), making re-execution on a different worker inherently safe:

- `SqlTransformer.transform()` is idempotent: `transform(transform(Q)) = transform(Q)`
- Stage SQL is standard `SELECT` — no side effects
- The only state mutation is `StageResultStore` caching, which is local and ephemeral

---

## 5. Bug Fix Specifications

### 5.1 Fix A: Stage DuckDB Isolation

**File**: `src/worker-flight/src/service.rs`

**Current** (broken):
```rust
let conn = Connection::open_in_memory()
    .map_err(|e| format!("Failed to create DuckDB connection: {}", e))?;
```

**Target fix**:
```rust
// WorkerFlightService gains: shared_db: Arc<SharedDatabase>
let conn = self.shared_db.get()
    .map_err(|e| format!("Failed to get pooled connection: {}", e))?;
```

**Design choice**: Pass `Arc<SharedDatabase>` as a separate field in `WorkerFlightService`, rather than adding `get_connection()` to the `QueryEngine` trait. This keeps the trait clean — `SharedDatabase` is a DuckDB implementation detail, not a `QueryEngine` concern.

**Verification**: Test 2.2 — submit a stage referencing `scan_orders(...)` macro that exists only in the shared database. Must return correct results.

### 5.2 Fix B: `fetch_output` Query ID

**File**: `src/worker-storage/src/ldp/executor/flight.rs`

**Current** (broken):
```rust
let query_id = format!("query_{}", ticket.stage_id);
let ticket_key = Self::ticket_key(&query_id, ticket.stage_id, &ticket.worker_id);
```

**Target fix**:
```rust
let ticket_key = Self::ticket_key(&ticket.query_id, ticket.stage_id, &ticket.worker_id);
```

**Verification**: Test 2.3 — submit stage, then fetch output. Must return the exact same `RecordBatch` data.

### 5.3 Fix E: Stale Connection Removal

**File**: `src/worker-storage/src/ldp/executor/flight.rs`

**Current**: On connection error in `submit_to_worker()`, the error is returned but the stale connection remains cached. Next retry reuses the same broken connection.

**Target fix**: In `submit_to_worker()`, on `WorkerUnavailable` or connection-level error, call `self.connection_pool.remove_connection(worker_id)` before returning the error.

**Verification**: Test 3.5 — simulate worker restart (stop/start Flight server), verify coordinator reconnects on retry.

---

## 6. Additional Issues (Medium Severity)

### 6.1 StageResultStore Memory Leak

- **Problem**: No TTL-based expiration. `CachedStageResult.created_at` field exists but is never read. If a coordinator crashes after submitting stages but before fetching results, cached `Vec<RecordBatch>` data remains in memory indefinitely.
- **Remediation**: Add a background `tokio::spawn` task that periodically (every 60s) evicts entries older than a configurable TTL (default 5 minutes). Use the existing `created_at` field.
- **Priority**: P1 — memory leak in production under failure conditions.

### 6.2 StageResultStore Destructive Reads

- **Problem**: `take()` removes results on first read. If the gateway retries a `DoGet(StageResultTicket)`, the second attempt gets `NOT_FOUND`.
- **Options**: (a) Keep destructive reads — gateway must not retry `DoGet` for stage results; (b) Change to `get()` (non-destructive) and rely on TTL for cleanup.
- **Recommendation**: Option (b) — non-destructive reads with TTL cleanup. This is more resilient to transient network issues between coordinator and participant.
- **Priority**: P1 — affects retry reliability.

### 6.3 Tenant Validation Inconsistency

- **Problem**: `DoGet` and `submit_stage` validate `tenant_id`, but `GetFlightInfo`, `GetSchema`, and `ListFlights` do not.
- **Remediation**: Add `validate_tenant()` check to all RPCs that access tenant data.
- **Priority**: P2 — security gap, low risk in single-tenant-per-worker model.

### 6.4 `submit_stage_streaming` Not Implemented

- **Location**: `src/worker-storage/src/ldp/executor/flight.rs` — returns `Err("not yet implemented")`
- **Impact**: Large exchange inputs exceeding gRPC max message size will fail. The default `max_frame_size` is 16MB.
- **Remediation**: Implement streaming variant using `DoPut` or chunked `DoAction`. Or increase `max_frame_size` as a short-term mitigation.
- **Priority**: P2 — only affects large exchange payloads.

### 6.5 Distributed Broadcast Push

- **Location**: `src/worker-storage/src/ldp/executor/exchange.rs` — `DistributedExchangeRuntime` has a TODO for broadcast push.
- **Impact**: Broadcast exchanges in distributed mode may not correctly replicate data to all target workers.
- **Priority**: P1 — affects broadcast join correctness.

---

## 7. Execution Plan

### Phase 1: Fix Critical Bugs (0.5 day)

| Task | Bug | Files |
|------|-----|-------|
| Fix stage DuckDB isolation | Bug A | `service.rs` |
| Fix fetch_output query_id | Bug B | `flight.rs` |
| Fix stale connection on retry | Gap E | `flight.rs` |

### Phase 2: Single-Node Flight Integration Tests (1-2 days)

| Task | Tests |
|------|-------|
| Create Flight integration test harness | Test infra |
| Implement tests 1.1–1.12 | Layer 1 |     

### Phase 3: Distributed Stage Tests (1-2 days)

| Task | Tests |
|------|-------|
| Implement tests 2.1–2.10 | Layer 2 |
| Verify Bug A + Bug B fixes | Tests 2.2, 2.3 |

### Phase 4: Wire Coordinator into Flight Service (1-2 days)

| Task | Gap |
|------|-----|
| Add `Option<Arc<LdpCoordinator>>` to `WorkerFlightService` | Gap C |
| Dispatch logic: local vs distributed based on metadata | Gap C |
| `SqlTransformer` mutability (`Arc<RwLock<...>>`) | Gap D |

### Phase 5: Distributed E2E Tests (2 days)

| Task | Tests |
|------|-------|
| Multi-process Flight test harness | Test infra |
| Implement tests 3.1–3.10 | Layer 3 |
| Implement tests 4.1–4.7 | Layer 4 |

### Phase 6: Medium-Severity Fixes (1-2 days)

| Task | Issue |
|------|-------|
| StageResultStore TTL + non-destructive reads | §6.1, §6.2 |
| Tenant validation on all RPCs | §6.3 |
| Distributed stage cancellation | Gap F |

### Phase 7: CLI Runtime Wiring (2-3 days)

| Task | Description |
|------|-------------|
| Implement `WorkerRuntime` struct | §Layer 6 |
| Startup sequence (10 steps) | §6.2 |
| Graceful shutdown coordinator | §6.2 step 12 |

### Phase 8: Control Plane Integration (3-5 days)

| Task | Description |
|------|-------------|
| CP client with bidi stream | §5.3 |
| Heartbeat with health metrics | §5.3, §5.2 proto enrichment |
| DataAssignment → DataLoader → SqlTransformer pipeline | §5.3 |
| StarRocks SourceAdapter implementation | §5.3 |

### Timeline Summary

| Phase | Duration | Cumulative |
|-------|----------|------------|
| Phase 1: Critical bug fixes | 0.5 day | 0.5 day |
| Phase 2: Single-node tests | 1-2 days | 2.5 days |
| Phase 3: Stage submission tests | 1-2 days | 4.5 days |
| Phase 4: Coordinator wiring | 1-2 days | 6.5 days |
| Phase 5: Distributed E2E tests | 2 days | 8.5 days |
| Phase 6: Medium fixes | 1-2 days | 10.5 days |
| Phase 7: CLI runtime | 2-3 days | 13.5 days |
| Phase 8: CP integration | 3-5 days | 18.5 days |

**Total estimated effort**: 12-18 days for full production readiness.

---

## 8. Success Criteria

### Gate 1: Single-Node Production-Ready

- [ ] All Layer 1 tests pass (tests 1.1–1.12)
- [ ] All Layer 2 tests pass (tests 2.1–2.10) — bugs A+B fixed
- [ ] Flight server starts via CLI with pre-loaded data
- [ ] Health check responds correctly
- [ ] Error codes map correctly to gRPC status

### Gate 2: Distributed Execution Production-Ready

- [ ] All Layer 3 tests pass (tests 3.1–3.10)
- [ ] All Layer 4 tests pass (tests 4.1–4.7)
- [ ] Coordinator wired into Flight service — any worker can coordinate
- [ ] Worker restart during query produces clean error (after retry exhaustion)
- [ ] Stale connections recovered on retry (Gap E fixed)

### Gate 3: Full Worker Production-Ready

- [ ] `liebrix-worker run` starts a complete functional worker
- [ ] Worker registers with control plane on startup
- [ ] Worker receives DataAssignment → loads data → registers dataset → answers queries
- [ ] Heartbeat reports health metrics (memory, query count, epoch inventory)
- [ ] Graceful shutdown drains queries and closes CP stream
- [ ] SQL Gateway can route queries to any worker and get correct results

---

## 9. Risk Register

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| Bug A fix changes DuckDB transaction isolation semantics for stages | Medium | High | Test that exchange temp tables don't leak across pooled connections |
| Large exchange payloads exceed gRPC frame size | Medium | Medium | Increase `max_frame_size` short-term; implement streaming long-term |
| `SharedDatabase` contention under mixed query + stage load | Low | High | Benchmark concurrent query + stage execution; tune pool size |
| StarRocks adapter complexity exceeds estimate | Medium | Medium | Start with JDBC adapter as fallback |
| Control plane proto changes required for gateway support | High | Low | Proto changes are backward-compatible (additive fields) |
| Worker restart data loss (in-memory DuckDB) | Expected | None | By design — CP re-assigns epochs to restarted worker |

---

## Appendix A: Architecture Evaluation — Control Plane + Data Plane + SQL Gateway

This appendix evaluates the three-tier architecture strategy against established distributed data systems and assesses its suitability for the stated engineering goals.

### A.1 Validation Against Established Systems

The architecture maps cleanly to patterns used by several production systems:

| Component | FoundationDB | CockroachDB | TiDB | Snowflake | Neon |
|-----------|-------------|-------------|------|-----------|------|
| **Control Plane** (leibrix) | Coordinators + ClusterController | Meta ranges + Liveness | PD (Placement Driver) | Cloud Services Layer | Control Plane (Console + API) |
| **Data Plane** (this worker) | Storage Servers | KV Nodes | TiKV / TiFlash | Virtual Warehouses | Pageservers + Safekeepers |
| **SQL Gateway** | Client library (smart routing) | SQL Gateway / pgwire | TiDB Server (SQL layer) | — (embedded in Cloud Services) | Proxy (pgwire) |

The separation chosen here is **the dominant pattern** for cloud-native data systems.

### A.2 FoundationDB Comparison

FDB is the closest philosophical match to this design.

**Where the designs align**:
- **Strict control/data separation**: FDB's `ClusterController` + `MasterProxyServer` handle metadata; `StorageServer` processes handle data. The control path never touches query-hot data.
- **Stateless proxies**: FDB's `CommitProxy` and `GrvProxy` are stateless — they route transactions but hold no durable state. The SQL Gateway plays this exact role.
- **Workers as generic compute**: FDB's `fdbserver` processes are role-assigned at runtime by the coordinator. Workers here receive tenant assignments via the CP bidi stream — the same pattern.

**Where this design improves on FDB**:
- **Cleaner proxy independence**: FDB's proxies are managed by the `MasterServer`, making them harder to scale independently. A separate gateway repo with only a meta-cache is cleaner for independent scaling and scale-to-zero.
- **Tenant isolation by construction**: FDB achieved multi-tenancy late (v7.1+). The one-worker-per-tenant model provides hard isolation without coordination overhead — a simpler correctness argument.

**What to adopt from FDB**:
- **Failure detection via heartbeat absence**: FDB doesn't emit a "topology change" event. The `ClusterController` detects failed workers via heartbeat timeout (4–10 seconds). The proto gap around `TopologyChangeEvent` (§5.2) can follow this simpler model — heartbeat absence with a configurable timeout — rather than building an explicit event type.
- **Recovery is the coordinator's job**: When an FDB `StorageServer` fails, the coordinator re-replicates data to survivors. The CP already does this (re-assigning epochs to restarted workers). This is the correct division of responsibility.

### A.3 Evaluation Against Design Goals

#### Goal 1: Engineering Complexity Reduction ✅

The three-tier split gives each component a single concern:

| Component | Concern | Complexity Ceiling |
|-----------|---------|-------------------|
| Control Plane | Cluster membership, epoch placement, tenant assignment | Consensus + metadata (bounded) |
| Worker | Load data, execute queries, exchange results | DuckDB + Arrow Flight (bounded per-tenant) |
| Gateway | Route queries, retry on failure, stream results | Stateless proxy (minimal) |

Compare this to a monolithic approach (e.g., early ClickHouse clusters) where every node handles routing, metadata, replication, and execution — operational complexity grows combinatorially.

**FDB lesson**: FDB's success came precisely from this factoring. Their `Simulation` framework could test coordination logic independently from storage logic. This architecture enables the same: `LdpCoordinator` can be tested with `MockCluster` in-process; the Flight integration layer is tested separately.

#### Goal 2: Separation of Control Flow and Data Flow ✅✅

The data and control paths never intersect:

```
Control flow:  CP ←──bidi gRPC stream──→ Workers (heartbeat, assignment, topology)
Data flow:     Gateway ──Arrow Flight──→ Coordinator ──Arrow Flight──→ Participants
```

A control plane outage doesn't block in-flight queries. A data plane spike doesn't delay heartbeats (Tokio async vs. blocking task separation ensures this).

**Contrast with TiDB**: TiDB's PD sits in the request path for timestamp allocation (`TSO`), meaning PD latency directly impacts transaction latency. This design avoids that by having no CP involvement in the query-time hot path.

#### Goal 3: Stability + Cloud Services + Scale-to-Zero ✅✅

Scale-to-zero flow:

```
1. No queries for N minutes
2. Gateway observes idle workers via health_check metrics
3. Gateway signals CP: "tenant-X can be suspended"
4. CP sends graceful shutdown to worker (drain → close)
5. Worker shuts down → cloud infra deallocates compute
6. Gateway retains connection; meta-cache remains valid
7. New query arrives for tenant-X
8. Gateway signals CP: "tenant-X needs a worker"
9. CP provisions new worker, sends DataAssignment
10. Worker loads epochs, registers with gateway
11. Gateway routes query to new worker
```

This works because:
- The **gateway** is always running (extremely lightweight, pure routing)
- The **worker** is the expensive resource (DuckDB + in-memory data) — the scale-to-zero target
- The **CP** persists epoch metadata — step 9 is fast (just re-send existing assignments)
- Workers are **stateless from CP's perspective** — any new worker can receive the same tenant assignment

**Snowflake parallel**: This is exactly how Snowflake Virtual Warehouses operate. The Cloud Services Layer (analogous to CP + Gateway) persists metadata while warehouses (analogous to workers) are suspended and resumed. **Neon parallel**: Neon separates compute (ephemeral, scales to zero) from the Pageserver (persistent storage). Workers here map to Neon compute; the upstream source (Iceberg/StarRocks) and CP map to the persistent layer.

### A.4 Architectural Suggestions

#### A.4.1 Embedded Gateway Mode for On-Premises Deployments

For on-premises deployments where scale-to-zero is not needed, consider supporting a mode where routing logic is embedded into the worker binary. This eliminates one network hop:

```
Mode A (Cloud):   Client → Gateway → Worker (coordinator) → Workers
Mode B (On-prem): Client → Worker (built-in routing) → Workers
```

No architectural changes are required — an optional `--enable-coordinator-routing` flag in `worker-cli` that starts routing logic alongside the Flight server would suffice.

#### A.4.2 FDB-Style Locality Hints in Heartbeat

The gateway's coordinator selection benefits from knowing which workers hold the most relevant epochs for a given query's date range. Enrich the `HeartbeatEvent` proto (currently empty) with:

```protobuf
message HeartbeatEvent {
  uint64 memory_used_bytes = 1;
  uint64 memory_total_bytes = 2;
  uint32 active_query_count = 3;
  repeated DatasetEpochSummary loaded_datasets = 4;
}

message DatasetEpochSummary {
  string dataset_id = 1;
  repeated string epoch_ids = 2;
  uint64 total_bytes = 3;
}
```

This enables the gateway to pick the coordinator with the highest data locality for a query's time range, minimizing exchange network traffic. This is already identified as a proto gap in §5.2 and should be treated as a priority.

#### A.4.3 Preferred Coordinator per Dataset

Formalizing a "preferred coordinator" concept (analogous to CockroachDB's range leaseholder) would make the gateway's routing more deterministic:

- Each dataset has a preferred coordinator: the worker holding the most epochs for that dataset
- The gateway's meta-cache stores this preference alongside the epoch→worker mapping
- On preferred coordinator failure, the gateway falls back to any worker with relevant epochs

This aligns with `LdpCoordinator`'s existing design — it already accepts per-query cluster metadata rather than assuming static topology.

#### A.4.4 Guard Against the "Fat Gateway" Anti-Pattern

The primary long-term risk is the gateway accumulating SQL or data-aware logic over time:

| Healthy Gateway | Unhealthy (Fat) Gateway |
|----------------|-------------------------|
| Route by meta-cache | Parse SQL to extract table names |
| Retry on worker failure | Cache query results |
| Health-check workers | Manage cross-query transactions |
| Stream results through unchanged | Transform or rewrite SQL |

The current design is disciplined — SQL transformation lives in the worker (`SqlTransformer`), not the gateway. This discipline must be maintained. Once the gateway starts understanding SQL semantics, the three tiers lose their independence and scale-to-zero becomes harder.

### A.5 Summary

| Criterion | Assessment |
|-----------|-----------|
| Pattern validity | Proven by FDB, Snowflake, TiDB, Neon, CockroachDB |
| Engineering complexity | Optimal factoring — each component has one concern |
| Control/data separation | Clean — CP is never in the query hot path |
| Stability | Workers are restartable by design; gateway absorbs failures |
| Scale-to-zero | Architecturally enabled; gateway + CP persist while workers deallocate |
| Cloud-native readiness | Strong — maps directly to serverless compute patterns |

The architecture is sound and well-reasoned. The critical path to production is not architectural but implementational — the bugs and gaps documented in §2.2 and §2.3, particularly Bug A (stage DuckDB isolation) and Gap C (no coordinator in Flight service), are what block production use.
