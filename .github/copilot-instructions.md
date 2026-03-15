# Copilot Instructions for Leibrix Worker

## Build, Test, and Lint Commands

Use the pinned toolchain from `rust-toolchain`:

```bash
rustup toolchain install nightly-2025-10-10
cargo +nightly-2025-10-10 build
```

Common commands (workspace root):

```bash
# Build
cargo build
cargo build --release
cargo build -p worker-storage

# Test (full / per crate)
cargo test
cargo test -p worker-storage
cargo test -p worker-flight

# Single integration test file
cargo test -p worker-storage --test engine_accuracy_tests

# Single test function
cargo test -p worker-storage --test engine_accuracy_tests test_create_single_epoch_table -- --nocapture

# Lint / format / checks
cargo fmt --all
cargo clippy --workspace --all-targets
cargo check --workspace
```

## High-Level Architecture

This repository is a Rust workspace with five crates:

- `worker-cli`: process entrypoint (`liebrix-worker`) and runtime wiring.
- `worker-cp`: control-plane session, event dispatch, and CP domain/proto conversion boundaries.
- `worker-flight`: Arrow Flight gRPC service for SQL queries and distributed stage result transfer.
- `worker-storage`: core storage/query engine (DuckDB), SQL transformation, and LDP planning/execution.
- `worker-proto`: protobuf codegen from `proto/*.proto`.

Runtime flow (big picture):

1. `worker-cli/src/main.rs` creates `MemoryDuckDBEngine`, `DuckDBQueryEngine`, `SqlTransformer`, and `DataLoader`.
2. `ControlPlaneSession` (from `worker-cp`) starts a bidirectional gRPC session to master.
3. `WorkerRuntimeDispatcher` bridges CP commands to storage actions:
   - assignments -> `DataLoader::load_epoch`
   - evictions -> `StorageEngine::drop_epoch_table`
4. If query address is configured, `worker-flight` starts and serves Arrow Flight requests.
5. Query path in `worker-flight/service.rs`: tenant validation -> SQL transform -> DuckDB query engine -> Arrow `RecordBatch` stream.

Planning/execution flow in `worker-storage`:

- SQL is rewritten through `sql/transformer.rs` to macro-backed scans (epoch pruning).
- LDP planner (`ldp/planner/*`) annotates distributions, inserts exchanges, then cuts stages.
- Stage outputs are exchanged via Flight and cached/retrieved with stage tickets in `worker-flight`.

## Key Repository Conventions

### Multi-tenancy and request boundaries

- One worker is bound to one tenant.
- Validate `tenant_id` at request/control-plane boundaries (not only at startup).

### SQL transformation invariant (must hold)

- Keep semantic parity: transformed SQL must return the same rows as original SQL.
- Use the double-guard pattern: macro parameters are optimization hints; original predicates stay in the rewritten query.
- Logical datasets are rewritten to macro calls (for example `scan_{dataset}(start, end)`).

### LDP planning strategy

- Baseline-first exchange policy:
  - hash partition shuffle is the default safe path,
  - broadcast is only for join context with exact stats and size below policy threshold.
- Do not add query-shape special cases when property-driven enforcement already covers the case.

### DuckDB integration

- Run DuckDB work in `tokio::task::spawn_blocking`.
- Use pooled/thread-local connections; do not share one DuckDB connection across threads/tasks.
- Keep Arrow-based streaming path (avoid materializing full datasets in memory when a stream is available).

### Data/model naming and boundaries

- Epoch tables follow `{dataset_id}__{epoch_id}` naming.
- Protobuf source files live in `proto/`; generated code is under `worker-proto/src/proto/` (do not hand-edit generated files).
- Keep proto <-> domain conversion logic in dedicated conversion modules (`proto_convert.rs` style) instead of scattering conversions.

### Dependency and toolchain constraints

- Keep Arrow crate versions pinned to `56.2.0` across crates for ABI compatibility.
- DuckDB is used as bundled `1.4.2` with Arrow-related features enabled.
- Nightly toolchain is required (`nightly-2025-10-10`).
