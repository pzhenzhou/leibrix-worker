# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Leibrix Worker is the **data plane component** of a distributed in-memory analytics system built in Rust. It works in tandem with the **control plane** (leibrix, at git@github.com:pzhenzhou/leibrix.git) which handles cluster coordination, worker assignments, and tenant management.

This worker provides:
- Epoch-based data loading from upstream sources (Iceberg, StarRocks, JDBC)
- In-memory query execution using embedded DuckDB
- Distributed query planning (LDP) with Arrow Flight for data exchange
- Predictable low-latency analytics with strict multi-tenant isolation (one worker per tenant)

## Build and Development Commands

### Build
```bash
# Full workspace build with all features
cargo build

# Release build (optimized with LTO and native CPU features)
cargo build --release

# Build specific crate
cargo build -p worker-storage
cargo build -p worker-flight
cargo build -p worker-cli
```

### Testing
```bash
# Run all tests
cargo test

# Run tests for specific crate
cargo test -p worker-storage

# Run specific test file
cargo test --test ldp_e2e_exchange_test
cargo test --test ldp_e2e_tpch_join_test

# Run tests with output
cargo test -- --nocapture

# Run engine accuracy tests
cargo test --test engine_accuracy_tests
```

### Running

The CLI binary is named `liebrix-worker`:
```bash
# Run the worker CLI
cargo run --bin liebrix-worker -- [args]

# Run Flight server example
cargo run --example basic_server
```

### Code Quality
```bash
# Check without building
cargo check

# Format code
cargo fmt

# Lint with clippy
cargo clippy
```

## Workspace Structure

This is a Cargo workspace with 5 member crates under `src/`:

- **worker-storage**: Core query engine, DuckDB integration, SQL transformation, and LDP (Leibrix Distributed Plan) planner/executor. This is the main crate containing most business logic.
- **worker-flight**: Arrow Flight service for SQL queries and distributed stage execution
- **worker-cp**: Control plane communication with master (minimal)
- **worker-proto**: Protobuf definitions for distributed protocols
- **worker-cli**: Command-line interface binary (`liebrix-worker`)

All crates share workspace-level dependencies defined in root `Cargo.toml`.

## Architecture Highlights

### Core Algorithm 1: DuckDB Storage Engine with Arrow Integration

The storage engine is built on **embedded DuckDB 1.4.2** with the following architecture:

**Key Components**:
- **Arrow C Data Interface**: Zero-copy data movement between Arrow RecordBatches and DuckDB tables
- **Connection Pooling**: `DuckDbConnectionPool` using `deadpool 0.12` with `SharedDatabase` for concurrent query execution
- **In-Memory Execution**: Data loaded into DuckDB's in-memory tables with configurable memory limits per connection
- **Vectorized Processing**: Leverages DuckDB's columnar vectorized OLAP engine

**Implementation Details**:
- `query_engine_impl.rs` - Query execution with Arrow result streaming via `query_arrow()`
- `storage_engine_impl.rs` - Bulk data loading via Arrow appender (`appender-arrow` feature)
- `pool.rs` - Connection pool management with `SharedDatabase` (allows multiple connections to same in-memory database)
- Bundled build with features: `appender-arrow`, `vscalar-arrow`, `json`, `r2d2`

**Zero-Copy Pipeline**: Client → Arrow Flight → DuckDB (Arrow appender) → Query (Arrow results) → Arrow Flight → Client

**Pool Configuration Defaults**:
- `max_size`: 32 connections
- `memory_limit_mb`: 1024 MB per connection
- `statement_timeout`: 60s

### Core Algorithm 2: Boolean Interval Algebra for Epoch Pruning

The SQL module uses **Boolean Interval Algebra** to minimize data scanning by pruning epochs at the SQL level.

**Problem**: Given a query like `SELECT * FROM sales WHERE dt >= '2025-01-01' AND (region = 'US' OR region = 'UK')`, determine which epochs (time-bounded data segments) actually need to be scanned.

**Algorithm Flow**:

```
1. PARSE & EXTRACT (sql/discovery.rs)
   - Extract date predicates from WHERE clause
   - Example: dt >= '2025-01-01' AND dt < '2025-02-01'

2. BOOLEAN SIMPLIFICATION (sql/boolean_analyzer.rs)
   - Apply boolean algebra to simplify complex predicates
   - Handle OR, AND, NOT operations
   - Example: (A AND B) OR (A AND C) → A AND (B OR C)

3. INTERVAL COMPUTATION (sql/interval.rs)
   - Convert predicates to time intervals
   - Operations: union, intersection, complement
   - Example: [2025-01-01, 2025-02-01) ∩ [2025-01-15, ∞) = [2025-01-15, 2025-02-01)

4. EPOCH MAPPING
   - Map computed intervals to epochs that intersect the time range
   - Query metadata to find relevant epoch IDs

5. MACRO REWRITING (sql/transformer.rs)
   - Transform: SELECT * FROM sales WHERE dt >= '2025-01-01'
   - Into:     SELECT * FROM sales_macro('2025-01-01', '2025-02-01') WHERE dt >= '2025-01-01'
```

**Key Files**:
- `sql/interval.rs` - Interval algebra operations (union ∪, intersection ∩, complement ¬)
- `sql/boolean_analyzer.rs` - Boolean expression analysis and DNF/CNF conversion
- `sql/discovery.rs` - AST traversal to extract time predicates
- `sql/transformer.rs` - Query rewriting engine
- `sql/admission.rs` - Admission control (reject recursive CTEs, enforce date predicates)

**Result**: Only epochs intersecting the computed time interval are scanned, drastically reducing data volume.

### Core Algorithm 3: LDP Property-Driven Distribution Enforcement

The Leibrix Distributed Plan (LDP) uses a **single-pass unified algorithm** inspired by Volcano/Cascades optimizers, operating on a custom **LogicalPlan-based** intermediate representation.

**Core Insight**:
> "One recursive traversal handles all query shapes. Distribution requirements are the only operator-specific knowledge. Exchange decisions emerge naturally from comparing actual vs. required properties."

**Algorithm: Annotate and Enforce (Single Pass)**

The planning pipeline is: `SQL → parse_sql() → Statement → build_logical_plan() → LogicalPlan → annotate_logical_plan() → cut_into_stages() → LdpPlan`

```
For each operator in LogicalPlan tree (bottom-up recursion):

┌─────────────────────────────────────────────────────────┐
│ STEP 1: RECURSE                                         │
│   children = [annotate_and_enforce(child) for child]   │
│   // Now we know each child's output distribution       │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ STEP 2: GET REQUIREMENTS (only operator-specific code) │
│   requirements = get_requirements(operator_type)        │
│                                                         │
│   Examples:                                             │
│   - Sort → [Singleton]                                  │
│   - Join → [HashPartitioned(L_keys),                   │
│             HashPartitioned(R_keys)]                    │
│   - GroupBy → [HashPartitioned(group_keys)]            │
│   - Filter → [Any] (distribution-preserving)           │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ STEP 3: ENFORCE (generic comparison logic)             │
│   for (child, required) in zip(children, requirements): │
│       actual = child.distribution                       │
│       if NOT required.is_satisfied_by(actual):          │
│           exchange = determine_exchange(actual,         │
│                                         required,       │
│                                         stats,          │
│                                         policy)         │
│           insert_exchange(child, exchange)              │
│           child.distribution = post_exchange_dist       │
└─────────────────────────────────────────────────────────┘
                         ↓
┌─────────────────────────────────────────────────────────┐
│ STEP 4: COMPUTE OUTPUT DISTRIBUTION                    │
│   output_dist = compute_output(operator, child_dists)   │
│   return AnnotatedRel(operator, output_dist, children)  │
└─────────────────────────────────────────────────────────┘
```

**Distribution Properties**:
- **Singleton**: All data on one worker (e.g., after Gather)
- **EpochPartitioned**: Natural partitioning from epoch storage across workers
- **HashPartitioned(keys)**: Data partitioned by hash of columns
- **Replicated**: Same data on all workers (after Broadcast)

**Exchange Selection (Baseline-First)**:

```rust
determine_exchange(actual, required, stats, policy):

    if required is Singleton:
        return Gather(coordinator)

    if required is HashPartitioned(keys):
        // BASELINE: Shuffle is always correct
        baseline = HashPartition(keys, num_partitions)

        // OPTIMIZATION: Broadcast requires ALL three conditions:
        if is_join_context
           AND stats.is_exact
           AND stats.bytes ≤ 256MB:
            return Broadcast(target_workers)

        return baseline
```

**Stage Cutting**: After annotation, cut plan at exchange boundaries:
- Each exchange marks a stage boundary
- Stages contain SQL strings generated by `sql/stage_sql_gen.rs`, with `ExchangeRead` placeholders rendered as table references (e.g., `__exchange_0`)
- Execution follows topological order (currently sequential; parallel execution designed but not implemented)

**Key Files**:
- `ldp/planner/annotate.rs` - Bottom-up distribution annotation
- `ldp/planner/requirements.rs` - Operator-specific distribution requirements
- `ldp/planner/exchange.rs` - Exchange selection logic (baseline-first)
- `ldp/planner/cut.rs` - Stage cutting at exchange boundaries
- `ldp/executor/coordinator.rs` - Topological stage execution
- `sql/stage_sql_gen.rs` - Converts LogicalPlan fragments back to executable SQL after stage cutting

**Statistics Confidence**:
- **Exact** (from ingestion metadata) → Enable broadcast optimization
- **Estimated** → Apply 2x safety factor
- **Unknown** → Conservative fallback to shuffle

## Key Design Patterns

### Baseline-First Exchange Strategy

When planning distributed exchanges, **shuffle (HashPartition) is the default**. Broadcast is only used when:
1. Statistics are exact (from ingestion metadata)
2. Data size is below threshold (256MB default)
3. Context is a join (not grouping)

This ensures correctness when statistics are uncertain.

### Statistics Confidence Levels

All table scans and intermediate results have a confidence level:
- **Exact**: From ingestion metadata, enables optimizations
- **Estimated**: Apply 2x safety factor
- **Unknown**: Conservative fallback to shuffle

### Resource Limits

All queries and stages execute within configurable bounds:
- Output limits (rows/bytes)
- Timeout (default 5 min)
- Memory budget (set via DuckDB `max_memory`)

`StageExecutionMonitor` enforces these limits at runtime and can cancel queries via `duckdb_interrupt()`.

## Testing Approach

### LDP E2E Tests

Tests in `src/worker-storage/tests/ldp_e2e_*.rs` use `TestCluster` to simulate distributed execution:
- `TestCluster::builder().workers(n).build().await` creates in-memory workers
- `load_distributed_data()` / `load_data_to_worker()` populates workers with partitioned data
- Tests verify plan generation, exchange insertion, and result correctness

### Engine Tests

Tests in `src/worker-storage/tests/engine_*.rs` validate DuckDB integration:
- Accuracy: Correctness of query results
- Concurrency: Connection pool behavior under load
- Error handling: Recovery and resource cleanup

### Testing Dimension Tables

For non-epoch data (products, customers), register as:
- Singleton distribution (on one worker) for small dimensions
- Replicated distribution for pre-loaded static data
- HashPartitioned for large dimension tables

## Common Development Tasks

### Adding a New Exchange Type

1. Add variant to `Exchange` enum in `ldp/types.rs`
2. Update `determine_exchange()` in `ldp/planner/exchange.rs`
3. Add execution logic in `ldp/executor/exchange.rs`
4. Update stage cutting logic in `ldp/planner/cut.rs`

### Adding a New Operator

1. Add requirement logic in `ldp/planner/requirements.rs`
2. Add output distribution computation in `ldp/planner/annotate.rs`
3. Update stage SQL generation in `sql/stage_sql_gen.rs` if the operator changes how SQL is regenerated from LogicalPlan

### Registering a New Dataset

```rust
transformer.register_dataset(RegisteredDataset::new(
    "table_name".into(),
    "dt".into(),  // time column
));
```

Ensure the corresponding macro function exists in DuckDB.

## Important Constraints

- **Nightly Rust Required**: Uses `nightly-2025-10-10` (see `rust-toolchain`)
- **Arrow Version Pinning**: All Arrow crates must use exactly `56.2.0` for ABI compatibility
- **DuckDB Native Extensions**: Uses bundled DuckDB 1.4.2 with Arrow appender features
- **Planning uses LogicalPlan, not Substrait**: The LDP planner operates on a custom `LogicalPlan` IR. Substrait-based single-node DuckDB execution (`from_substrait()`) exists but is not used in the distributed planning path.

## Code Locations Reference

### SQL Processing
- SQL parsing: `worker-storage/src/sql/parser.rs`
- SQL transformation: `worker-storage/src/sql/transformer.rs`
- Admission control: `worker-storage/src/sql/admission.rs`
- Boolean interval algebra: `worker-storage/src/sql/interval.rs`
- Stage SQL generation: `worker-storage/src/sql/stage_sql_gen.rs`

### DuckDB Integration
- Query engine: `worker-storage/src/engine/duckdb/query_engine_impl.rs`
- Storage engine: `worker-storage/src/engine/duckdb/storage_engine_impl.rs`
- Connection pool: `worker-storage/src/engine/duckdb/pool.rs`
- Substrait single-node execution (not used in distributed path): `worker-storage/src/engine/duckdb/substrait.rs`

### LDP Components
- Planner entry: `worker-storage/src/ldp/planner/mod.rs`
- Executor: `worker-storage/src/ldp/executor/coordinator.rs`
- Stage execution: `worker-storage/src/ldp/executor/stage.rs`
- Flight integration: `worker-storage/src/ldp/executor/flight.rs`

### Arrow Flight Service
- Main service: `worker-flight/src/service.rs`
- Server builder: `worker-flight/src/server.rs`
- Examples: `worker-flight/examples/`
- Integration test harness: `worker-flight/tests/common/harness.rs`

### Test Infrastructure (Reusable, lives in `src/` not `tests/`)
- TestCluster: `worker-storage/src/ldp/testing/cluster.rs`
- Test data generation: `worker-storage/src/ldp/testing/data_loader.rs`
- DuckDB macro setup: `worker-storage/src/ldp/testing/macro_helpers.rs`
- TPC-H benchmark data: `worker-storage/src/ldp/testing/tpch_data.rs`

### Protobuf Definitions
- Source proto files: `proto/` (workspace root — `common.proto`, `control_plane.proto`, `ldp.proto`)
- Generated Rust: `worker-proto/src/proto/` (do not edit directly)

## Release Profile Notes

The release profile uses aggressive optimizations:
- `lto = "fat"`: Full link-time optimization
- `codegen-units = 1`: Maximum optimization, slower compile
- `rustflags = ["-Ctarget-cpu=native"]`: CPU-specific instructions
- `panic = "abort"`: Smaller binary, no unwinding

These settings maximize runtime performance at the cost of compile time and binary portability.

## System Context

This worker is the **data plane** of a two-component distributed system:

- **Control Plane** (leibrix at git@github.com:pzhenzhou/leibrix.git): Cluster coordination, worker assignment, tenant management, epoch metadata registry
- **Data Plane** (this project): Query execution, in-memory storage, distributed query processing

Workers maintain persistent gRPC streams to the control plane for:
- Receiving tenant assignments (one worker per tenant for strict isolation)
- Reporting health and resource utilization
- Receiving dataset registration and epoch metadata
- Coordinating graceful shutdown and failover

The `worker-cp` crate contains minimal control plane communication logic. The `tenant_id` field appears throughout the codebase as workers are dedicated to specific tenants.

## Related Documentation

- LDP design: `docs/ldp_design.md` (comprehensive 1000+ line design document with algorithm details)
- SQL module: `docs/sql_module_design.md`
- Arrow Flight: `src/worker-flight/README.md`
- E2E test status: `docs/E2E_TEST_FINAL_STATUS.md`
- Control plane repository: git@github.com:pzhenzhou/leibrix.git

## Error Handling

- **Library boundaries** (`worker-storage` public API): Use `thiserror` with typed error enums and struct variants for pattern matching.
- **Internal implementation**: Use `anyhow` for flexibility. Always add context with `.context()` or `.with_context(|| ...)`.
- **Structured error variants**: Prefer `QueryTimeout { elapsed: Duration, limit: Duration }` over `QueryTimeout(String)`.
- **Logging levels**: `error!()` for system failures, `warn!()` for recoverable issues, `info!()` for milestones, `debug!()` for troubleshooting.

## Code Style Conventions

### Import Organization

Four groups, separated by blank lines:
1. `std` / `core`
2. External crates (`arrow`, `duckdb`, `tokio`, etc.)
3. Internal workspace crates (`worker_proto`, `worker_storage`)
4. Parent/sibling modules (`super::`, `crate::`)

### Logging

Use `tracing` with structured field syntax, not string formatting:
```rust
// Good
tracing::info!(worker_id = %id, stage = %stage_id, "stage completed");
// Bad
tracing::info!("stage {} completed on worker {}", stage_id, id);
```

### Async and Blocking

- Use `tokio::task::spawn_blocking()` for CPU-bound work (DuckDB queries, Substrait serialization).
- Use `.await` for I/O-bound work (Flight RPCs, network calls).
- Clone `Arc` (cheap ref count bump), never clone its contents.

## SQL Semantic Parity Invariant

Transformed queries **must produce identical results** to the original. Original predicates are always preserved as a final filter after macro expansion — the macro call is an optimization hint, not the source of truth:
```sql
-- Original
SELECT * FROM sales WHERE dt >= '2025-01-01'
-- Transformed (predicate kept)
SELECT * FROM scan_sales('2025-01-01', '2025-02-01') WHERE dt >= '2025-01-01'
```

## Design Principles

All code changes in this repository follow principles from **Effective Rust** and **A Philosophy of Software Design**. These are not aspirational — they are enforced during review.

### Type Safety (Effective Rust)

- **Newtype Pattern**: Wrap primitive types (`String`, `u32`) in newtypes (`WorkerId`, `StageId`, `ExchangeId`, `QueryId`) so the compiler prevents mixing semantically different values. A `WorkerId` can never be passed where a `QueryId` is expected.
- **Express Invariants in Types**: Encode domain constraints into types rather than runtime checks. Prefer struct variants (e.g., `WorkerUnavailable { worker_id: WorkerId, detail: String }`) over stringly-typed errors.
- **Implement Standard Traits Thoughtfully**: Newtypes derive `Clone`, `Copy` (for integer-based), `Hash`, `Eq`, `PartialEq`, `Debug`, and implement `Display`, `From`, `AsRef`. Make them ergonomic without leaking inner representation.
- **Respect Sealed Trait Boundaries**: When a trait is sealed (e.g., `tracing::Value`), use the idiomatic workaround (`%` Display formatting) instead of fighting the type system.
- **Flexible API Boundaries**: Use `impl AsRef<str>` / `impl Into<T>` for functions that should accept both the newtype and its underlying type.

### Complexity Management (A Philosophy of Software Design)

- **Deep Modules, Shallow Interfaces**: Each abstraction presents a minimal public interface while hiding internal representation. Callers don't need to know whether `StageId` wraps `u32` or `u64`.
- **Define Errors Out of Existence**: Structure error types so misuse is impossible. Don't allow an error message where an ID belongs.
- **Information Hiding / Leakage Prevention**: Confine conversion logic (e.g., proto ↔ domain) to a single file. No other module needs to know the inner representation for serialization.
- **Strategic vs. Tactical Programming**: Fix the design, not the symptoms. Change type signatures at the source and let the compiler reveal every site that needs updating, rather than patching call sites with ad-hoc conversions.
- **Single Point of Truth**: One canonical definition for each type, struct, or algorithm. No parallel copies that can drift.
- **Fight Complexity Continually**: Each shortcut (e.g., `type WorkerId = String`) seems harmless individually but collectively allows silent bugs. Targeted refactoring reduces this accumulated complexity.
- **Consistency Across the Codebase**: All similar types follow the same pattern — same derives, same trait impls, same conventions. Understanding one means understanding all.

### Architectural Principles

- **Baseline-First Correctness**: Propagate changes file-by-file with compiler verification at each step. No speculative refactoring.
- **Proto Boundary as Conversion Firewall**: Newtypes don't cross the protobuf boundary. Conversion happens exactly once in each direction, in one file (`proto_convert.rs`).
- **Test Infrastructure Follows Production Code**: Test helpers use the same type-safe APIs as production code, ensuring tests exercise real interfaces.
