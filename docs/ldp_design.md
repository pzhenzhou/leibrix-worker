# Leibrix Distributed Plan (LDP) Design Document

## 1. Problem Statement and Design Constraints

### 1.1 What Problem Does LDP Solve?

Leibrix-worker is a distributed in-memory analytics system where **temporally-partitioned data** (called **epochs**) is spread across multiple worker nodes. Each worker holds a subset of epochs for a dataset, sharded by ingestion time or event time.

Without a distributed query layer, the system would be limited to:

- **Single-worker queries only**: A query like `SELECT SUM(revenue) FROM sales WHERE dt >= '2025-01-01'` could not combine data from workers w1, w2, and w3, each holding different time ranges.
- **Client-side aggregation**: The client would need to query each worker separately and merge results, incurring massive network overhead and pushing complex logic to the client.
- **No cross-worker joins**: A fact-dimension join (`sales JOIN products`) where `sales` spans 4 workers and `products` lives on 1 worker would be impossible without manual data movement.

LDP solves these problems by:

1. **Pushing computation to data**: Partial queries execute where data lives, reducing network traffic.
2. **Minimizing data movement**: Only shuffle intermediate results when necessary; broadcast small tables instead of shuffling large ones.
3. **Enabling parallel execution**: Independent stages run concurrently across workers.

### 1.2 Design Constraints

| Constraint | Rationale |
|------------|-----------|
| **Epoch-native partitioning** | Data arrives in temporal segments. The planner must understand this natural partitioning to avoid unnecessary shuffles. |
| **Single-tenant isolation** | Each worker binds to exactly one tenant. No cross-tenant data sharing or resource contention. |
| **DuckDB as local engine** | Each worker runs an embedded DuckDB instance. LDP must generate standard SQL that DuckDB can execute — no proprietary IR. |
| **Arrow throughout** | All data movement uses Apache Arrow columnar format for zero-copy compatibility with DuckDB and Arrow Flight. |
| **Correctness over performance** | When statistics are uncertain, always choose the safe strategy (shuffle) over the optimized one (broadcast). |
| **No recursive CTEs** | Recursive CTEs imply unbounded computation. The admission controller rejects them. |
| **Date predicates required** | Epoch-partitioned tables require time-range predicates to enable epoch pruning. Queries without them are rejected at admission. |

### 1.3 Design Philosophy: SQL-Delegating Architecture

LDP follows the **SQL-delegating distributed system** pattern used by Citus, Vitess, and PolarDB-X:

> The planning layer decides **data movement** (exchanges). DuckDB on each node handles **local optimization and execution**.

This means LDP does **not** build a custom query optimizer. It builds a `LogicalPlan` from SQL, annotates it with distribution properties, cuts it into stages, and generates **standard SQL** for each stage. DuckDB then optimizes and executes that SQL locally.

The previous Substrait-based approach was abandoned when DuckDB 1.4.2+ dropped the `duckdb-substrait-extension`, breaking both `get_substrait()` and `from_substrait()`. The current implementation uses `sqlparser::ast` as the plan representation and `query_arrow(sql)` for execution, preserving all distribution planning algorithms.

---

## 2. Core Distributed Execution Algorithm

### 2.1 The Unified Algorithm: Key Insight

The LDP planning algorithm embodies one design principle:

> **A single recursive traversal driven by distribution property enforcement.**

Unlike traditional distributed query planners that use dozens of pattern-matching rules, LDP uses a **property-driven** approach:

1. **One recursive traversal** handles all query shapes (star schema, snowflake, self-joins, CTEs, etc.)
2. **Distribution requirements** are the **only** operator-specific knowledge
3. **Exchange decisions** emerge from comparing `actual` vs. `required` distribution properties
4. **No special cases** for different query patterns

### 2.2 Algorithm: Annotate and Enforce (Single Pass)

The entire planning happens in one bottom-up traversal of the `LogicalPlan` tree:

```
annotate_and_enforce(plan_node):

    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 1: RECURSE (bottom-up)                                     │
    │   children = [annotate_and_enforce(child) for child in node]    │
    │   // Now each child carries its output distribution annotation  │
    └─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 2: GET REQUIREMENTS (the only operator-specific code)      │
    │   requirements = get_requirements(plan_node)                    │
    │                                                                 │
    │   Examples:                                                     │
    │     Sort        → [Singleton]                                   │
    │     Join        → [HashPartitioned(L_keys),                     │
    │                    HashPartitioned(R_keys)]                     │
    │     Aggregate   → [HashPartitioned(group_keys)]                 │
    │     Filter      → [Any]                                         │
    └─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 3: ENFORCE (generic comparison — no operator knowledge)    │
    │   for (child, required) in zip(children, requirements):         │
    │       actual = child.annotation.distribution                    │
    │       if NOT required.is_satisfied_by(actual):                  │
    │           exchange = determine_exchange(actual, required, ...)  │
    │           child.exchange_before = exchange                      │
    │           child.annotation = post_exchange_annotation           │
    └─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 4: COMPUTE OUTPUT DISTRIBUTION                             │
    │   output = derive_from(operator_type, child_distributions)      │
    │   return AnnotatedPlan(plan_node, output, children)             │
    └─────────────────────────────────────────────────────────────────┘
```

**Why this works**: Each operator makes a **local** decision ("what distribution do I need from my inputs?"). These local decisions **compose** to produce a globally correct distributed plan. No global analysis or multi-pass optimization is needed.

### 2.3 Requirements: The Only Operator-Specific Knowledge

The `get_logical_plan_requirements()` function is the **single location** in the entire planner that inspects operator types. Everything else is generic.

| Operator | Input Count | Requirements | Rationale |
|----------|-------------|--------------|-----------|
| `Scan` | 0 | (none) | Leaf node — no inputs |
| `ExchangeRead` | 0 | (none) | Leaf node — reads from exchange temp table |
| `Filter` | 1 | `[Any]` | Distribution-preserving — does not change row placement |
| `Project` | 1 | `[Any]` | Distribution-preserving — column projection only |
| `Sort` | 1 | `[Singleton]` | Global ordering requires all data on one node |
| `Limit` | 1 | `[Singleton]` | Global LIMIT/OFFSET requires all data on one node |
| `Aggregate` (global) | 1 | `[Singleton]` | `COUNT(*)`, `SUM(x)` without GROUP BY produces one row |
| `Aggregate` (grouped) | 1 | `[HashPartitioned(group_keys)]` | Rows with same group key must be co-located |
| `Join` (equi) | 2 | `[HashPartitioned(L_keys), HashPartitioned(R_keys)]` | Matching rows must be on the same worker |
| `Join` (cross) | 2 | `[Any, Any]` | No join keys — handled by broadcast at enforce step |
| `SetOp` (UNION, etc.) | 2 | `[Any, Any]` | Combined at coordinator |
| `Window` | 1 | `[Singleton]` | Conservative — could be optimized by PARTITION BY keys |
| `SubqueryScan` | 1 | `[Any]` | Distribution-preserving wrapper |

**Source**: `src/worker-storage/src/ldp/planner/requirements.rs`

### 2.4 Satisfaction Check

The enforcement step uses a simple satisfaction check:

```
RequiredDistribution::is_satisfied_by(actual) → bool

    Any:
        return true                         // any distribution works

    Singleton:
        return actual is Singleton          // must already be gathered

    HashPartitioned(required_keys):
        match actual:
            HashPartitioned(actual_keys) →  keys match (case-insensitive)
            Replicated                   →  true  // replicated satisfies anything
            _                            →  false
```

This simple check is the **only** logic needed to decide whether an exchange must be inserted.

**Source**: `src/worker-storage/src/ldp/types.rs` (`RequiredDistribution::is_satisfied_by`)

### 2.5 Exchange Selection: Baseline-First Strategy

When `actual ≠ required`, the planner selects an exchange. The core principle is **shuffle is always correct; broadcast is an optimization**.

#### General Exchange Selection

```
determine_exchange(actual, annotation, required, policy, is_join, target_workers):

    if required is Singleton:
        if can_gather(est_rows, est_bytes, policy):
            return Gather(coordinator)
        else:
            return Reject(GatherTooLarge)

    if required is HashPartitioned(keys):
        // BROADCAST optimization — requires ALL THREE conditions:
        //   1. is_join_context = true (not GROUP BY)
        //   2. stats are Exact (from ingestion metadata)
        //   3. est_bytes ≤ broadcast_bytes_max (default 256MB)
        if policy.can_optimize_to_broadcast(est_bytes, stats_exact, is_join):
            return Broadcast(target_workers)

        // BASELINE: HashPartition is always correct
        if can_shuffle(est_bytes, policy):
            return HashPartition(keys, default_partitions)
        else:
            return Reject(ShuffleTooLarge)
```

#### Join-Specific Exchange Selection

For joins, the planner makes a **coordinated decision** for both sides:

```
determine_join_exchanges(left, right, left_keys, right_keys, policy):

    // Shortcut 1: Replicated build side → no exchanges needed
    if right is Replicated:
        return (None, None)

    // Shortcut 2: Co-partitioned on matching keys → no exchanges needed
    if both HashPartitioned on matching keys with same workers:
        return (None, None)

    // Broadcast strategy selection
    left_can_broadcast  = exact(left)  AND small(left)  AND is_join
    right_can_broadcast = exact(right) AND small(right) AND is_join

    match (left_can_broadcast, right_can_broadcast):
        (true, true)   → broadcast smaller side
        (true, false)  → broadcast left
        (false, true)  → broadcast right
        (false, false) → shuffle both sides (baseline)
```

**Source**: `src/worker-storage/src/ldp/planner/exchange.rs`

### 2.6 Statistics Confidence Propagation

Every annotated node carries a `StatsSource` indicating how trustworthy its statistics are:

| Level | Source | Effect on Exchange Selection |
|-------|--------|------------------------------|
| `Exact` | Epoch ingestion metadata | Enables broadcast optimization |
| `Estimated` | Heuristics (selectivity, cardinality estimates) | Safety factor applied (default 2.0x) |
| `Unknown` | No statistics available | Conservative baseline (always shuffle) |

**Propagation rules**:
- **Filter**: Inherits child's source; applies 50% selectivity heuristic to row/byte estimates
- **Join**: Takes the **minimum** confidence of both sides (Exact + Estimated → Estimated)
- **Aggregate**: Uses `sqrt(input_rows) * sqrt(num_keys)` cardinality estimate; degrades to Estimated
- **Project/SubqueryScan**: Inherits child's source unchanged

### 2.7 Output Distribution Computation

After enforcement, each operator computes its output distribution:

| Operator | Output Distribution | Notes |
|----------|---------------------|-------|
| `Scan` | `EpochPartitioned(epoch_workers)` | Natural distribution from data placement |
| `Scan` (single worker) | `Singleton(worker)` | Only one worker holds this table |
| `Filter` | Same as input | Distribution-preserving |
| `Project` | Same as input | Distribution-preserving |
| `Sort` | `Singleton(coordinator)` | After Gather |
| `Limit` | `Singleton(coordinator)` | After Gather |
| `Aggregate` (global) | `Singleton` | Single result row |
| `Aggregate` (grouped) | `HashPartitioned(group_keys)` | Partial results per partition |
| `Join` | Same as left (probe) side | Standard join convention |
| `SetOp` | Same as first input | Combined stream |
| `Window` | `Singleton(coordinator)` | After Gather |

### 2.8 Stage Cutting: Exchange = Stage Boundary

After annotation, the plan tree is cut into **stages** at every exchange boundary:

```
cut_into_stages(annotated_root):

    For each annotated node (recursive):
        for each child:
            if child.exchange_before is Some(exchange):
                // CUT HERE
                1. Create a new Stage for the child subtree
                2. Generate executable SQL for the child's plan fragment
                3. Create an ExchangeEdge connecting child_stage → parent_stage
                4. Replace child with ExchangeRead { alias: "__exchange_N" }
            else:
                // Inline child into current stage
                recurse into child

    Final stage = root's plan fragment
    Return LdpPlan(stages, edges, root_stage)
```

**Key details**:

- Each stage carries a `stage_sql` field containing **executable SQL** (not IR). DuckDB runs this SQL directly via `query_arrow()`.
- Exchange inputs are registered as temporary DuckDB tables named `__exchange_0`, `__exchange_1`, etc.
- CTEs from the original query are duplicated into each stage that references them. This is correct because DuckDB re-evaluates non-materialized CTEs.
- Worker assignment comes from the distribution annotations: stages reading from local catalog run on data-holding workers; exchange-only stages run on the coordinator.

**Source**: `src/worker-storage/src/ldp/planner/cut.rs`

### 2.9 Topological Execution

The final `LdpPlan` is a DAG of stages connected by exchange edges. The executor processes stages in topological order using Kahn's algorithm:

```
execute(plan):
    1. Compute topological order via Kahn's algorithm
    2. Group into execution levels (stages at same depth can run in parallel)
    3. For each level:
        a. For each stage in level (concurrently):
           - Resolve exchange inputs from upstream stages
           - Submit stage SQL to target workers
           - Store output tickets for downstream stages
    4. Fetch final output from root stage
    5. Return concatenated RecordBatches
```

**Exchange resolution** before each stage:
- **Gather**: Fetch data from all source workers, concatenate
- **Broadcast**: Fetch from source, replicate to all targets
- **HashPartition**: Fetch all data, hash-partition by column values, distribute to assigned workers

**Source**: `src/worker-storage/src/ldp/executor/coordinator.rs`

### 2.10 Node-Centric Execution View (Who does what on w1/w2/coordinator)

The same `LdpPlan` can be understood as a set of responsibilities split across nodes:

- **Coordinator node**:
  - Receives the client SQL.
  - Runs admission, SQL transform, parsing, distribution planning, and stage cutting.
  - Orchestrates stage execution order (topological levels).
  - Resolves exchanges (`Gather`, `Broadcast`, `HashPartition`) by moving Arrow batches between workers.
  - Returns final Arrow results to the client.

- **Worker nodes (`w1`, `w2`, ...)**:
  - Execute assigned `stage_sql` on local DuckDB.
  - Read local catalog data (epochs) when a stage has `LocalCatalog` input.
  - Read exchange temp tables (`__exchange_N`) when a stage depends on upstream data movement.
  - Produce stage outputs as Arrow tickets/streams for downstream consumption.

- **Important coordinator rule**:
  - Coordinator identity is **not hardcoded** by LDP.
  - It is assigned by the upper-level control system/policy.
  - Any worker node can be chosen as coordinator for a query (for example, `w1` in one query and `w2` in another).

#### Example timeline (2 workers + coordinator)

Assume workers `w1` and `w2` hold different `sales` epochs, and `w2` is selected as coordinator by the upper layer:

1. **Planning on coordinator (`w2`)**:
   - Build `LogicalPlan`.
   - Annotate distributions bottom-up.
   - Insert exchanges where requirements are not satisfied.
   - Cut into stages and assign target workers.

2. **Stage execution**:
   - `Stage 0` runs on `w1` and `w2` (local scans + partial compute).
   - Each worker emits Arrow output for `Exchange 0`.

3. **Exchange handling on coordinator (`w2`)**:
   - If `Exchange 0` is `Gather`, `w2` fetches from both `w1` and `w2`, concatenates, and registers `__exchange_0`.
   - If `Exchange 0` is `Broadcast`, `w2` replicates source batches to both workers.
   - If `Exchange 0` is `HashPartition`, `w2` repartitions rows by hash key and routes partitions to target workers.

4. **Downstream stage(s)**:
   - Stage reads `__exchange_0` (or later exchange tables) on whichever workers are assigned.
   - Root stage output is returned by coordinator (`w2`) to client.

This is why node labels (`w1`, `w2`, `coordinator`) are **roles for a query execution**, not fixed machine identities in the algorithm.

---

## 3. Core Abstractions and Code Structure

### 3.1 System Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Client                                      │
│                         (SQL Query + tenant_id)                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        LdpCoordinator                                    │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌───────────┐  ┌───────────────┐  │
│  │  Admission   │→ │     SQL      │→ │    LDP    │→ │    Stage      │  │
│  │  Control     │  │  Transform   │  │  Planner  │  │   Executor    │  │
│  └──────────────┘  └──────────────┘  └───────────┘  └───────────────┘  │
│                                                                          │
│  Admission: reject recursive CTEs, require date predicates               │
│  Transform: rewrite table names to epoch-pruning macros                  │
│  Planner:   SQL → LogicalPlan → annotate → cut → LdpPlan               │
│  Executor:  topological stage execution with exchange resolution         │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          │                         │                         │
          ▼                         ▼                         ▼
   ┌──────────────┐          ┌──────────────┐          ┌──────────────┐
   │   Worker 1   │          │   Worker 2   │          │   Worker N   │
   │  (epochs     │  ←────→  │  (epochs     │  ←────→  │  (epochs     │
   │   e1, e4)    │  Flight  │   e2, e5)    │  Flight  │   e3, e6)    │
   │              │          │              │          │              │
   │   DuckDB     │          │   DuckDB     │          │   DuckDB     │
   └──────────────┘          └──────────────┘          └──────────────┘
```

### 3.2 Query Processing Pipeline

```
SQL Query
    │
    ▼
┌─────────────────────────────────────┐
│ 1. Admission Control                │  Reject recursive CTEs, require date predicates
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 2. SQL Transformation               │  Rewrite table names to scan_table(start, end) macros
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 3. SQL Parse → LogicalPlan          │  sqlparser → AST → LogicalPlan tree
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 4. Annotate & Enforce               │  Bottom-up distribution annotation + exchange insertion
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 5. Stage Cutting                    │  Cut at exchange boundaries → stages with SQL fragments
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 6. Topological Execution            │  Execute stages in dependency order via DuckDB
└─────────────────────────────────────┘
    │
    ▼
Arrow RecordBatch Stream
```

### 3.3 Code Organization

```
worker-storage/src/
├── sql/                              # SQL processing layer
│   ├── logical_plan.rs               # LogicalPlan enum, ColumnRef, PlanContext
│   ├── plan_builder.rs               # SQL AST → LogicalPlan conversion
│   ├── stage_sql_gen.rs              # LogicalPlan → executable SQL generation
│   ├── transformer.rs                # Table name → macro rewriting
│   ├── admission.rs                  # Query admission control
│   ├── interval.rs                   # Boolean interval algebra for epoch pruning
│   ├── boolean_analyzer.rs           # Boolean expression analysis
│   ├── discovery.rs                  # Table reference discovery
│   └── parser.rs                     # SQL parsing utilities
│
├── ldp/                              # Distributed planning & execution
│   ├── types.rs                      # Distribution, Exchange, Stage, LdpPlan
│   ├── proto_convert.rs              # Protobuf serialization for Flight
│   │
│   ├── planner/                      # Plan generation
│   │   ├── pipeline.rs               # Entry point: plan_ldp()
│   │   ├── annotate.rs               # Core annotation algorithm
│   │   ├── requirements.rs           # Operator distribution requirements
│   │   ├── exchange.rs               # Exchange selection logic
│   │   ├── cut.rs                    # Stage cutting at exchange boundaries
│   │   ├── metadata.rs               # Metadata trait + InMemoryMetadata
│   │   ├── storage_metadata.rs       # Production metadata from StorageEngine
│   │   ├── policy.rs                 # PlannerPolicy configuration
│   │   └── inspect.rs                # Plan inspection utilities
│   │
│   ├── executor/                     # Execution runtime
│   │   ├── coordinator.rs            # End-to-end query coordinator
│   │   ├── stage.rs                  # StageExecutor trait + LocalStageExecutor
│   │   ├── exchange.rs               # ExchangeRuntime (local + distributed)
│   │   ├── flight.rs                 # FlightStageExecutor + WorkerConnectionPool
│   │   ├── result_store.rs           # TTL-based result caching
│   │   ├── performance.rs            # Performance monitoring & recommendations
│   │   └── skew.rs                   # Data skew detection & handling
│   │
│   └── testing/                      # Test infrastructure
│       ├── cluster.rs                # TestCluster (mock multi-worker environment)
│       ├── data_loader.rs            # Test data generation & epoch loading
│       ├── scenarios.rs              # Declarative test scenario framework
│       ├── verifier.rs               # Result verification (exact + approximate)
│       └── tpch_data.rs              # TPC-H benchmark data generators
│
├── engine/duckdb/                    # DuckDB integration
│   ├── pool.rs                       # Connection pool with SharedDatabase
│   ├── query_engine_impl.rs          # query_arrow() execution
│   ├── storage_engine_impl.rs        # Bulk data loading via Arrow appender
│   └── arrow_utils.rs               # Arrow ↔ DuckDB conversion utilities
│
└── loader/                           # Data loading from external sources
```

### 3.4 Key Data Structures

#### Distribution Properties

```rust
/// How data is distributed across workers.
enum Distribution {
    Singleton { worker: WorkerId },                         // All data on one node
    EpochPartitioned { workers: Vec<WorkerId> },            // Natural time-based sharding
    HashPartitioned { column_refs: Vec<ColumnRef>, workers: Vec<WorkerId> },  // Hash of columns
    Replicated { workers: Vec<WorkerId> },                  // Same data on all workers
}

/// What distribution an operator requires from its input.
enum RequiredDistribution {
    Any,                                                    // Accept anything
    Singleton,                                              // Must be on one node
    HashPartitioned { column_refs: Vec<ColumnRef> },        // Must be partitioned by these columns
}
```

#### Exchange Types

```rust
/// Data movement between stages.
enum Exchange {
    Gather { target: WorkerId },                            // Collect all → one node
    Broadcast { targets: Vec<WorkerId> },                   // Replicate one → all nodes
    HashPartition { column_refs: Vec<ColumnRef>, partitions: u32 },  // Redistribute by hash
}
```

#### LogicalPlan (Intermediate Representation)

```rust
/// Relational algebra tree. Each node carries sqlparser AST expressions
/// for two purposes:
///   - Distribution planning: group_keys, left_keys, right_keys
///   - SQL generation: predicate, items, order_by, group_by, join_op
enum LogicalPlan {
    Scan { table_name, alias, table_factor },               // Table scan (leaf)
    Filter { input, predicate },                            // σ (selection)
    Project { input, items },                               // π (projection)
    Aggregate { input, group_by, aggr_exprs, having, group_keys },  // γ (aggregation)
    Sort { input, order_by },                               // τ (sort)
    Limit { input, limit, offset },                         // LIMIT/OFFSET
    Join { left, right, join_op, left_keys, right_keys },   // ⋈ (join)
    SetOp { left, right, op, set_quantifier },              // ∪/∩/∖
    Window { input, window_exprs },                         // Window functions
    SubqueryScan { input, alias },                          // Derived table
    ExchangeRead { exchange_id, alias },                    // Exchange placeholder
}
```

The `LogicalPlan` serves a **dual purpose**: the planner reads `group_keys` / `left_keys` / `right_keys` for distribution decisions, while the SQL generator reads `predicate` / `items` / `order_by` / `join_op` for SQL reconstruction. These concerns don't interfere.

#### Stage and LdpPlan

```rust
/// A single execution unit with executable SQL.
struct Stage {
    stage_id: StageId,
    target_workers: Vec<WorkerId>,      // Workers that execute this stage
    inputs: Vec<StageInput>,            // LocalCatalog or ExchangeInput
    output: StageOutput,                // Stream or Partitioned
    stage_sql: String,                  // Executable SQL for DuckDB
    limits: StageLimits,                // Resource bounds
}

/// Complete distributed execution plan (DAG of stages).
struct LdpPlan {
    query_id: QueryId,
    coordinator: WorkerId,
    stages: Vec<Stage>,                 // All stages in topological order
    edges: Vec<ExchangeEdge>,           // Exchange connections between stages
    root_stage: StageId,                // Final stage producing query result
}
```

#### AnnotatedPlan (Intermediate Planning Structure)

```rust
/// LogicalPlan node annotated with distribution information.
struct AnnotatedPlan {
    plan: LogicalPlan,                  // Original plan node
    annotation: DistributionAnnotation, // Distribution + stats
    exchange_before: Option<Exchange>,  // Exchange to insert before this node
    children: Vec<AnnotatedPlan>,       // Annotated children
}

/// Statistics annotation attached to each plan node.
struct DistributionAnnotation {
    distribution: Distribution,         // How data is distributed
    est_rows: u64,                      // Estimated row count
    est_bytes: u64,                     // Estimated byte size
    stats_source: StatsSource,          // Exact | Estimated | Unknown
}
```

### 3.5 Key Traits and Interfaces

#### Metadata Trait

```rust
/// Abstract metadata access for planning decisions.
trait Metadata {
    fn get_epoch_stats(&self, epoch_id: &str) -> Option<EpochStats>;
    fn get_epoch_worker(&self, epoch_id: &str) -> Option<WorkerId>;
    fn get_epochs_for_table(&self, table: &str, start: u64, end: u64) -> Vec<(String, WorkerId)>;
    fn get_table_scan_stats(&self, table: &str) -> TableScanStats;
}
```

Two implementations:
- `InMemoryMetadata`: In-memory implementation for testing
- `StorageEngineMetadata<E>`: Production implementation wrapping the storage engine

#### StageExecutor Trait

```rust
/// Backend for executing stages on workers.
trait StageExecutor {
    async fn submit_stage(&self, ...) -> Result<StageTickets>;
    async fn submit_stage_streaming(&self, ...) -> Result<RecordBatchStream>;
    async fn fetch_output(&self, ticket: &StageTicket) -> Result<Vec<RecordBatch>>;
}
```

Two implementations:
- `LocalStageExecutor`: Executes stages via local DuckDB connections
- `FlightStageExecutor`: Submits stages to remote workers via Arrow Flight

### 3.6 PlannerPolicy Configuration

```rust
struct PlannerPolicy {
    broadcast_bytes_max: u64,       // 256 MB — max bytes for broadcast exchange
    shuffle_bytes_max: u64,         // 2 GB — max bytes for shuffle exchange
    gather_rows_max: u64,           // 50M — max rows for gather exchange
    gather_bytes_max: u64,          // 5 GB — max bytes for gather exchange
    default_partitions: u32,        // 16 — hash partition count
    coordinator: String,            // Coordinator worker ID
    safety_factor: f64,             // 2.0 — multiplier for uncertain stats
}
```

**Presets**:
- `conservative()`: Disable broadcast (set threshold to 0), safety factor 3.0
- `aggressive()`: 1GB broadcast threshold, safety factor 1.1
- `for_testing()`: Enable broadcast, no safety factor

---

## 4. Execution Strategies with Examples

### 4.1 Example 1: Simple Aggregation with GROUP BY

**Query**:
```sql
SELECT region, SUM(revenue) FROM sales WHERE dt >= '2025-01-01' GROUP BY region
```

**Setup**: `sales` has epochs on workers w1, w2.

**Step 1: SQL Transformation**
```sql
SELECT region, SUM(revenue) FROM scan_sales('2025-01-01', '9999-12-31')
WHERE dt >= '2025-01-01' GROUP BY region
```

**Step 2: LogicalPlan**
```
Aggregate(group_keys=[region], aggr=[SUM(revenue)])
    └── Filter(dt >= '2025-01-01')
            └── Scan(sales)
```

**Step 3: Annotate & Enforce**
```
Aggregate
    requirement: HashPartitioned([region])
    actual child: EpochPartitioned([w1, w2])
    → NOT satisfied → insert Gather(coordinator)
    output: Singleton(coordinator)

Filter
    requirement: Any
    → satisfied (no exchange)
    output: EpochPartitioned([w1, w2])

Scan(sales)
    output: EpochPartitioned([w1, w2])   ← from metadata
```

> **Note**: The planner requires `Singleton` for global aggregation when the actual distribution is `EpochPartitioned`, because a grouped aggregate on non-co-located data requires gathering first (the planner does not currently generate two-phase partial/final aggregation).

**Step 4: Stage Cutting**
```
Stage 0 (workers: [w1, w2]):
    SQL: SELECT region, SUM(revenue) FROM scan_sales('2025-01-01', '9999-12-31')
         WHERE dt >= '2025-01-01' GROUP BY region

Exchange 0: Gather → coordinator

Stage 1 (workers: [coordinator]):
    SQL: SELECT region, SUM(revenue) FROM __exchange_0 GROUP BY region
```

**Step 5: Execution**
1. Stage 0 runs on w1 and w2 concurrently — each produces partial results
2. Gather exchange: coordinator fetches partial results from w1 and w2, concatenates
3. Stage 1 runs on coordinator: re-aggregates the partial results
4. Return final result

---

### 4.2 Example 2: Multi-Table Join (Star Schema)

**Query**:
```sql
SELECT p.category, SUM(s.revenue)
FROM sales s
JOIN products p ON s.product_id = p.product_id
WHERE s.dt >= '2025-01-01'
GROUP BY p.category
```

**Setup**:
- `sales`: 100M rows across workers w1, w2, w3, w4 (EpochPartitioned)
- `products`: 10K rows on w1 only (Singleton), exact stats (500KB)

**Planning with Exact Stats (Broadcast Optimization)**:

```
Join
    left (sales):  EpochPartitioned([w1,w2,w3,w4])
    right (products): Singleton(w1)
    requirements: [HashPartitioned([s.product_id]), HashPartitioned([p.product_id])]

    Left:  EpochPartitioned ≠ HashPartitioned([product_id]) → needs exchange
    Right: Singleton ≠ HashPartitioned([product_id]) → needs exchange

    Join exchange selection:
        left_can_broadcast  = exact(sales) AND small(sales)     → false (100M rows)
        right_can_broadcast = exact(products) AND small(products) → true (500KB < 256MB)
        → Broadcast products to all workers

Result:
    Left exchange:  None (sales stays on its workers)
    Right exchange: Broadcast(products → [w1,w2,w3,w4])
```

**Stages**:
```
Stage 0 (workers: [w1]):
    SQL: SELECT * FROM products
    → Produces products data from w1

Exchange 0: Broadcast → [w1,w2,w3,w4]

Stage 1 (workers: [w1,w2,w3,w4]):
    SQL: SELECT p.category, SUM(s.revenue)
         FROM scan_sales('2025-01-01', '9999-12-31') s
         JOIN __exchange_0 p ON s.product_id = p.product_id
         WHERE s.dt >= '2025-01-01'
         GROUP BY p.category
    → Each worker joins local sales data with broadcast products

Exchange 1: Gather → coordinator

Stage 2 (workers: [coordinator]):
    SQL: SELECT category, SUM(revenue) FROM __exchange_1 GROUP BY category
    → Re-aggregate partial results
```

**Data movement**: Only 500KB (products) broadcast to 4 workers = 2MB total. Without broadcast optimization, all 100M sales rows would need to be shuffled.

**Planning without Exact Stats (Baseline)**:

If products had `Unknown` stats, the planner would fall back to:
```
Stage 0: Scan sales → HashPartition(product_id, 16 partitions)
Stage 1: Scan products → HashPartition(product_id, 16 partitions)
Stage 2: Hash Join (both sides shuffled)

Data movement: ~100M rows + 10K rows shuffled
```

This is always correct but significantly more expensive.

---

### 4.3 Example 3: Multi-Way Join (Snowflake Schema)

**Query**:
```sql
SELECT c.name, p.category, SUM(o.amount)
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN products p ON o.product_id = p.product_id
WHERE o.dt >= '2025-01-01'
GROUP BY c.name, p.category
```

**Setup**:
- `orders`: EpochPartitioned([w1,w2]) — large fact table
- `customers`: Singleton(w1) — 50K rows, exact stats, 2MB
- `products`: Singleton(w1) — 10K rows, exact stats, 500KB

**LogicalPlan** (as built by `plan_builder.rs`):
```
Aggregate(group_keys=[c.name, p.category])
    └── Join(o.product_id = p.product_id)
            ├── Join(o.customer_id = c.customer_id)
            │       ├── Filter(o.dt >= '2025-01-01')
            │       │       └── Scan(orders)
            │       └── Scan(customers)
            └── Scan(products)
```

**Annotation** (bottom-up):
1. `Scan(orders)` → `EpochPartitioned([w1,w2])`
2. `Scan(customers)` → `Singleton(w1)`, exact 2MB
3. `Scan(products)` → `Singleton(w1)`, exact 500KB
4. Inner `Join(orders ⋈ customers)`:
   - Left: `EpochPartitioned([w1,w2])` ≠ `HashPartitioned([customer_id])`
   - Right: `Singleton(w1)` ≠ `HashPartitioned([customer_id])`
   - Customers can broadcast (2MB < 256MB, exact stats, join context) → `Broadcast(customers → [w1,w2])`
   - Output: `EpochPartitioned([w1,w2])` (follows left side)
5. Outer `Join(... ⋈ products)`:
   - Left: `EpochPartitioned([w1,w2])` ≠ `HashPartitioned([product_id])`
   - Right: `Singleton(w1)` ≠ `HashPartitioned([product_id])`
   - Products can broadcast (500KB < 256MB, exact stats, join context) → `Broadcast(products → [w1,w2])`
   - Output: `EpochPartitioned([w1,w2])` (follows left side)
6. `Aggregate(group_keys=[c.name, p.category])`:
   - Requirement: `HashPartitioned([c.name, p.category])`
   - Actual: `EpochPartitioned([w1,w2])` → not satisfied
   - Insert `Gather(coordinator)`

**Resulting stages**: Both dimension tables broadcast (total ~2.5MB moved), orders stay in place. The massive fact table is never shuffled.

---

### 4.4 Example 4: Common Table Expressions (CTEs)

**Query**:
```sql
WITH top_products AS (
    SELECT product_id, SUM(revenue) as total_revenue
    FROM sales
    WHERE dt >= '2025-01-01'
    GROUP BY product_id
    ORDER BY total_revenue DESC
    LIMIT 10
)
SELECT p.name, tp.total_revenue
FROM top_products tp
JOIN products p ON tp.product_id = p.product_id
```

**How CTEs are handled**:

1. **Admission control**: Non-recursive CTEs are allowed. Recursive CTEs (`WITH RECURSIVE`) are rejected.

2. **Plan building** (`plan_builder.rs`): The CTE body is expanded if it contains a FROM clause, building a full `LogicalPlan` subtree. The CTE definition is stored in `PlanContext.cte_definitions` for SQL regeneration.

3. **Annotation**: The expanded CTE subplan is annotated like any other subtree. The `Sort` + `Limit` inside the CTE forces a `Gather` exchange, creating a stage boundary.

4. **Stage cutting**: When cutting stages, CTEs are **duplicated** into each stage that references them. This is correct because DuckDB re-evaluates non-materialized CTEs. The stage cutter uses `filter_ctes_for_stage()` to determine which CTEs to include in each stage's SQL.

5. **SQL generation**: The `generate_stage_sql()` function prepends `WITH cte_name AS (...)` to stages that reference CTEs, preserving the original CTE definitions from the sqlparser AST.

**Resulting plan**:
```
Stage 0 (workers: [w1, w2]):
    SQL: SELECT product_id, SUM(revenue) as total_revenue
         FROM scan_sales('2025-01-01', '9999-12-31')
         WHERE dt >= '2025-01-01'
         GROUP BY product_id

Exchange 0: Gather → coordinator

Stage 1 (workers: [coordinator]):
    SQL: WITH top_products AS (
             SELECT product_id, SUM(total_revenue) as total_revenue
             FROM __exchange_0
             GROUP BY product_id
             ORDER BY total_revenue DESC
             LIMIT 10
         )
         SELECT p.name, tp.total_revenue
         FROM top_products tp
         JOIN products p ON tp.product_id = p.product_id
```

**Key insight**: The CTE's `ORDER BY ... LIMIT 10` forces a `Singleton` requirement (all data on one node), which creates a Gather exchange. After gathering, the coordinator runs both the CTE finalization and the subsequent join locally, since `top_products` is now a small (10 rows) intermediate result.

---

### 4.5 Unsupported Use Cases

LDP explicitly does **not** support the following scenarios, with clear error handling:

| Unsupported Feature | Rejection Point | Error | Reason |
|---------------------|----------------|-------|--------|
| **Recursive CTEs** (`WITH RECURSIVE`) | Admission control | `AdmissionError::RecursiveCteNotSupported` | Implies unbounded computation incompatible with resource limits |
| **Queries without date predicates** on epoch-partitioned tables | Admission control | `AdmissionError::MissingTimeRangePredicate` | Cannot determine which epochs to scan without time bounds |
| **INSERT / UPDATE / DELETE** | Plan builder | `PlanBuildError::UnsupportedStatement` | LDP is read-only analytics |
| **LATERAL JOINs** | Plan builder | `PlanBuildError::UnsupportedFeature` | Correlated subqueries not supported |
| **Data exceeding gather limits** | Planner (exchange selection) | `PlanningError::Rejected(GatherTooLarge)` | Default: 50M rows / 5GB. Add aggregation or filters to reduce data. |
| **Data exceeding shuffle limits** | Planner (exchange selection) | `PlanningError::Rejected(ShuffleTooLarge)` | Default: 2GB per shuffle. Add filters to reduce data. |
| **Two-phase partial/final aggregation** | Not implemented | N/A | The planner gathers all data for aggregation rather than pushing down partial aggregates. This is a future optimization. |
| **Window function partitioning** | Planner (conservative) | Uses Gather (Singleton) | Could be optimized to partition by PARTITION BY keys. Current implementation gathers all data. |
| **Parallel independent stages** | Executor | Stages at the same DAG level execute sequentially within a level | Parallel execution within levels is implemented via `compute_execution_levels()` but concurrent stage submission is level-based, not fully concurrent. |

---

## 5. Exchange Semantics

### 5.1 Gather Exchange

Collects all data to a single target worker (coordinator):

```
Source Workers: [w1, w2, w3]
    │    │    │
    └────┼────┘
         │  All data flows to coordinator
         ▼
Target: coordinator

Input:  EpochPartitioned([w1, w2, w3])  or  HashPartitioned(...)
Output: Singleton(coordinator)
```

**Use cases**: ORDER BY, LIMIT/OFFSET, scalar aggregates, window functions.

### 5.2 Broadcast Exchange

Replicates data from one source to all target workers:

```
Source: w1 (e.g., small dimension table)
         │
    ┌────┼────┐
    │    │    │  Same data copied to all
    ▼    ▼    ▼
Targets: [w1, w2, w3]

Input:  Singleton(w1)
Output: Replicated([w1, w2, w3])
```

**Conditions** (all three required):
1. Join context (not GROUP BY)
2. Exact statistics from ingestion metadata
3. Data size ≤ `broadcast_bytes_max` (default 256MB)

**Use case**: Small dimension tables joined with large fact tables.

### 5.3 HashPartition Exchange

Redistributes data by hash of key columns:

```
Source Workers: [w1, w2]
    Each row hashed by key columns → assigned to partition

    ┌──────────────────────────────────────────┐
    │ w1 data:  hash(row) % 16 → partition 0-15│
    │ w2 data:  hash(row) % 16 → partition 0-15│
    └──────────────────────────────────────────┘
                    │
    Partitions assigned round-robin to workers:
    w1 gets partitions 0,2,4,6,...
    w2 gets partitions 1,3,5,7,...

Input:  EpochPartitioned([w1, w2])
Output: HashPartitioned(column_refs, [w1, w2])
```

**Use cases**: Distributed GROUP BY, equi-join when both sides are large.

**Skew handling**: The `SkewHandler` detects data skew by analyzing key frequency distribution (coefficient of variation > threshold). For skewed keys, round-robin distribution replaces hash partitioning to avoid hot partitions.

---

## 6. Technical Trade-offs

### 6.1 SQL-Native vs. Custom IR

| Aspect | SQL-Native (current) | Custom IR |
|--------|---------------------|-----------|
| **DuckDB compatibility** | Always compatible — standard SQL | Requires IR → SQL translation |
| **Maintenance** | Leverages sqlparser ecosystem | Custom parser + optimizer needed |
| **Optimization** | Delegates to DuckDB's optimizer | Can apply custom optimizations |
| **Expressiveness** | Limited to SQL constructs | Can represent custom operators |
| **Debugging** | Stage SQL is human-readable | IR requires separate debugging tools |

**Decision**: SQL-native approach. The planning layer handles distribution; DuckDB handles local optimization. This avoids the maintenance burden of a custom optimizer while still achieving correct distributed execution.

### 6.2 Single-Pass vs. Multi-Pass Planning

| Aspect | Single-Pass (current) | Multi-Pass (Cascades-style) |
|--------|----------------------|----------------------------|
| **Complexity** | O(n) traversal | Exponential search space |
| **Plan quality** | Good for most patterns | Optimal for all patterns |
| **Implementation** | ~500 lines of core logic | Thousands of lines + rule engine |
| **Correctness** | Easy to verify | Complex interaction between rules |

**Decision**: Single-pass. The property-driven approach is simple, correct, and sufficient for the target workload (temporal analytics with star/snowflake schemas). Multi-pass optimization would only help for complex queries that are outside the typical use case.

### 6.3 Baseline-First vs. Cost-Based Exchange Selection

| Aspect | Baseline-First (current) | Full Cost-Based |
|--------|-------------------------|-----------------|
| **Correctness** | Always correct (shuffle is safe default) | Depends on cost model accuracy |
| **Simplicity** | Three conditions for broadcast | Complex cost functions |
| **Risk** | May over-shuffle when stats are unknown | May under-estimate costs → OOM |
| **Statistics dependency** | Degrades gracefully with unknown stats | Requires accurate stats |

**Decision**: Baseline-first. When statistics are uncertain, the system defaults to shuffle (which is always correct but may be slower). Broadcast optimization is only enabled when all three safety conditions are met.

### 6.4 Epoch-Based vs. Hash-Based Natural Distribution

| Aspect | Epoch-Based (current) | Hash-Based |
|--------|----------------------|------------|
| **Pruning** | Time-range queries skip entire epochs | Requires secondary indexing |
| **Load balance** | May be skewed by time | Even distribution by hash |
| **Ingestion** | Append-only to current epoch | Hash routing required |
| **Query pattern** | Optimized for temporal analytics | Optimized for point lookups |

**Decision**: Epoch-based. The system is designed for temporal analytics workloads where data arrives in time-ordered segments. Epoch pruning eliminates entire data segments from queries, reducing I/O dramatically.

---

## 7. Dimension Table Handling

Dimension tables (products, customers, regions) differ from fact tables:
- No natural time-based epochs
- Relatively static, updated infrequently
- Small enough to fit on a single worker or be replicated
- Joined with fact tables using non-time keys

### Registration Patterns

**Pattern 1: Singleton + Broadcast (Small Dimensions)**
```
Register "products" on one worker with exact stats:
    worker: w1, stats: Exact(10K rows, 500KB)
    distribution: Singleton(w1)

At query time:
    products: Singleton(w1) → Broadcast([w1,w2,w3,w4])
    sales: EpochPartitioned → stays in place
    → Local join on each worker (zero fact table movement)
```

**Pattern 2: Replicated (Pre-loaded on All Workers)**
```
Load "products" onto ALL workers during startup:
    distribution: Replicated([w1,w2,w3,w4])

At query time:
    products: Replicated → requirement already satisfied
    sales: EpochPartitioned → stays in place
    → Local join on each worker (zero data movement, best case)
```

**Pattern 3: Hash-Partitioned (Large Dimensions)**
```
For large dimension tables (e.g., 100M customer records):
    Pre-partition by join key across workers
    distribution: HashPartitioned([customer_id], [w1,w2,w3,w4])

At query time:
    If fact table is also HashPartitioned on same key:
    → Co-partitioned join (zero data movement)
    Otherwise:
    → Shuffle fact table to match dimension partitioning
```

**Recommendation**: Always provide exact statistics for dimension tables during registration to enable broadcast optimization.

---

## 8. Resource Limits and Safety

### Stage Execution Limits

```rust
StageLimits {
    max_bytes_output: 5 GB,       // Maximum output bytes per stage
    max_rows_output: 100M,        // Maximum output rows per stage
    timeout_ms: 300_000,          // 5-minute execution timeout
    memory_bytes: 2 GB,           // Memory budget per stage
}
```

The `StageExecutionMonitor` enforces these limits at runtime:
- Atomic tracking of `rows_scanned`, `rows_produced`, `bytes_produced`
- Periodic timeout checking (100ms intervals)
- Cancellation via `duckdb_interrupt()` for immediate termination
- Integration with DuckDB `max_memory` setting

### Query-Level Protection

| Protection | Mechanism | Default |
|-----------|-----------|---------|
| Recursive CTE rejection | Admission control | Always |
| Date predicate requirement | Admission control | Always for epoch-partitioned tables |
| Gather size limit | Planner rejection | 50M rows / 5GB |
| Shuffle size limit | Planner rejection | 2GB |
| Broadcast size limit | Planner threshold | 256MB |
| Stage timeout | Runtime monitor | 5 minutes |
| Memory budget | DuckDB `max_memory` | 2GB |
| Query timeout | Coordinator | 5 minutes |
| Stage retries | Coordinator | 3 retries |

---

## 9. Distributed Execution via Arrow Flight

### Stage Submission Flow

```
Coordinator                          Worker
    │                                   │
    │  DoAction("submit_stage")         │
    │  ─────────────────────────────→   │
    │  [stage_sql + exchange inputs     │
    │   serialized as Arrow IPC]        │
    │                                   │
    │   ←─────────────────────────────  │
    │  StageTicket(query_id, stage_id)  │
    │                                   │
    │  DoGet(ticket)                    │
    │  ─────────────────────────────→   │
    │                                   │
    │   ←─────────────────────────────  │
    │  Stream<RecordBatch>              │
    │                                   │
```

### Component Roles

- **`FlightStageExecutor`**: Implements `StageExecutor` trait. Serializes stage definitions via `proto_convert.rs`, sends via Flight `DoAction`, caches tickets for result retrieval.
- **`WorkerConnectionPool`**: Manages Flight connections to workers. Lazy connection creation, cached per worker ID.
- **`LdpFlightClient`**: High-level Flight client for health checks, stage submission, and result fetching.
- **`DistributedExchangeRuntime`**: Handles remote exchange execution. Fetches data from remote workers, applies hash partitioning, manages ticket registration.
- **`StageResultStore`**: Worker-side result caching with TTL-based eviction (default 5 minutes). Results stored by `(tenant_id, query_id, stage_id)` key.

---

## Appendix A: Configuration Reference

### PlannerPolicy Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broadcast_bytes_max` | 256MB | Max bytes for broadcast exchange |
| `shuffle_bytes_max` | 2GB | Max bytes for shuffle exchange |
| `gather_rows_max` | 50M | Max rows for gather exchange |
| `gather_bytes_max` | 5GB | Max bytes for gather exchange |
| `default_partitions` | 16 | Hash partition count |
| `coordinator` | (required) | Coordinator worker ID |
| `safety_factor` | 2.0 | Applied to uncertain stats |

### CoordinatorConfig Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `tenant_id` | (required) | Tenant identifier |
| `query_timeout` | 5 min | Maximum query execution time |
| `max_concurrent_stages` | 16 | Parallelism limit |
| `max_stage_retries` | 3 | Retry count per failed stage |
| `distributed` | false | Enable Flight-based distribution |

### DuckDbPoolConfig Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `max_size` | 32 | Maximum connections in the pool |
| `initial_size` | 4 | Initial connections to establish |
| `connection_timeout` | 30s | Connection timeout |
| `statement_timeout` | 60s | Statement timeout |
| `memory_limit_mb` | 1024 | Memory limit per connection (MB) |

## Appendix B: Error Handling Reference

### Planning Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `Rejected(ShuffleTooLarge)` | Shuffle exceeds `shuffle_bytes_max` | Add filters to reduce data |
| `Rejected(GatherTooLarge)` | Gather exceeds row/byte limits | Add aggregation before gather |
| `MissingMetadata(table)` | Table not found in metadata | Register table with coordinator |
| `InvalidPlan(msg)` | Malformed plan structure | Check SQL validity |

### Pipeline Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `Parse(SqlTransformError)` | SQL parsing failed | Fix SQL syntax |
| `PlanBuild(PlanBuildError)` | Unsupported SQL feature | Check supported features |
| `Planning(PlanningError)` | Distribution planning failed | Check metadata registration |

### Execution Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `StageNotFound` | Stage ID mismatch | Internal error |
| `StageFailed` | Worker execution error | Check worker logs |
| `ExchangeFailed` | Network/Flight error | Retry or check connectivity |
| `WorkerUnavailable` | Worker offline | Remove from pool |

---

## Appendix C: Glossary

| Term | Definition |
|------|------------|
| **Epoch** | A bounded segment of data partitioned by time. Each epoch lives on one worker. |
| **LDP** | Leibrix Distributed Plan — the distributed execution plan produced by the planner. |
| **Stage** | An independent execution unit containing SQL that runs on one or more workers. |
| **Exchange** | Data movement between stages (Gather, Broadcast, HashPartition). |
| **Distribution** | A property describing how data is spread across workers. |
| **Coordinator** | The worker that orchestrates query execution and receives the final result. |
| **Annotation** | Distribution metadata attached to each LogicalPlan node during planning. |
| **Safety Factor** | Multiplier (default 2.0) applied to uncertain statistics before making exchange decisions. |
| **Admission Control** | Pre-planning validation that rejects queries incompatible with distributed execution. |

---

**Revision History**:
- 2025-01-11: Initial design document
- 2026-01-17: Updated implementation status
- 2026-02-15: Complete rewrite reflecting SQL-native architecture (Substrait removed), actual code implementation, execution examples for multi-table joins, CTEs, and unsupported use cases
