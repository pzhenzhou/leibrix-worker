# Leibrix Distributed Plan (LDP) Design Document

## 1. Purpose and Necessity

### 1.1 Business Context

Leibrix-worker is a distributed analytics system that processes **temporally-partitioned data** stored as **epochs** - bounded segments of data spread across multiple worker nodes. Each worker holds a subset of epochs for a dataset, with data naturally sharded by ingestion time or event time.

The key business requirements driving LDP are:

1. **Time-Range Analytics**: Users need to query weeks or months of data spanning multiple workers
2. **Low-Latency Joins**: Analytics often combine fact tables with dimension tables across nodes
3. **Resource Efficiency**: In-memory analytics require careful memory management to avoid OOM
4. **Multi-Tenant Isolation**: Each worker binds to exactly one tenant with no cross-tenant data sharing

### 1.2 Technical Drivers

Without distributed query capability, the system would be limited to:
- Single-worker queries only
- Client-side data aggregation (massive network overhead)
- Sequential queries across workers (unacceptable latency)

LDP addresses these limitations by:
- **Pushing computation to data**: Execute partial queries where data lives
- **Minimizing data movement**: Only shuffle intermediate results when necessary
- **Enabling parallel execution**: Run stages concurrently across workers

### 1.3 Design Philosophy

LDP follows a "**Substrait-first**" approach:

> Work directly with Substrait as the logical plan representation. No custom IR - traverse and annotate Substrait directly.

This eliminates the complexity of maintaining a separate intermediate representation while leveraging DuckDB's proven SQL-to-Substrait compilation.

---

## 2. Architectural Design

### 2.1 System Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                              Client                                      │
│                         (SQL Query + tenant_id)                          │
└─────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           LDP Coordinator                                │
│  ┌──────────────┐  ┌─────────────┐  ┌───────────┐  ┌─────────────────┐ │
│  │  Admission   │→ │    SQL      │→ │   LDP     │→ │    Stage        │ │
│  │  Control     │  │ Transform   │  │  Planner  │  │   Executor      │ │
│  └──────────────┘  └─────────────┘  └───────────┘  └─────────────────┘ │
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

### 2.2 Component Relationships

The LDP system spans two main components:

- **Worker-Storage**: Contains the LDP planner, executor, and DuckDB integration. Responsible for plan generation and local stage execution.
- **Worker-Flight**: Provides the Arrow Flight transport layer for distributed communication between workers.

These components interact through well-defined interfaces: the planner produces `LdpPlan` structures, and the executor coordinates stage submission across workers using Flight for data movement.

### 2.3 Query Processing Pipeline

```
SQL Query
    │
    ▼
┌─────────────────────────────────────┐
│ 1. Admission Control                │ ← Reject recursive CTEs, require date predicates
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 2. SQL Transformation               │ ← Rewrite for epoch pruning
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 3. SQL → Substrait                  │ ← DuckDB compiles to Substrait plan
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 4. Annotate & Enforce               │ ← Distribution annotation + exchange insertion
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 5. Stage Cutting                    │ ← Cut at exchange boundaries into stages
└─────────────────────────────────────┘
    │
    ▼
┌─────────────────────────────────────┐
│ 6. Topological Execution            │ ← Execute stages in dependency order
└─────────────────────────────────────┘
    │
    ▼
Arrow RecordBatch Stream
```

---

## 3. Technical Trade-offs

### 3.1 Baseline-First Exchange Strategy

**Decision**: Use shuffle (HashPartition) as the default exchange, not broadcast.

**Rationale**:
- Shuffle is **always correct** regardless of data size
- Broadcast is an **optimization** that requires exact statistics and join context
- When stats are uncertain, the 2x safety factor prevents optimization

**Trade-off Table**:

| Strategy | When Applied | Risk | Benefit |
|----------|--------------|------|---------|
| Shuffle (baseline) | Stats unknown/uncertain | None | Always correct |
| Broadcast | Stats exact + join context + small data | Wrong size estimate → network flood | Reduced shuffle overhead |
| Gather | Singleton required | Too much data → OOM | Enables global operations |

**Enforcement**: Broadcast optimization requires all three conditions: join context, exact statistics, and data size within threshold.

### 3.2 Substrait-First vs Custom IR

**Decision**: Work directly with Substrait rather than defining a custom intermediate representation.

**Advantages**:
- No translation overhead between representations
- DuckDB can execute Substrait directly (`from_substrait()`)
- Industry-standard format enables future interoperability

**Trade-offs**:
- Limited to Substrait's operator set
- Must handle Substrait's protobuf complexity
- Annotation metadata stored separately (not embedded in Substrait)

### 3.3 Statistics Confidence Levels

**Decision**: Implement three-tier statistics confidence.

| Level | Source | Use |
|-------|--------|-----|
| `Exact` | Epoch ingestion metadata | Enable optimizations |
| `Estimated` | Sampling/heuristics | Apply safety factor |
| `Unknown` | No stats available | Conservative baseline |

**Trade-off**: Conservative approach may over-estimate data movement, but never under-estimates (which could cause OOM or incorrect results).

### 3.4 Epoch-Based vs Hash-Based Natural Distribution

**Decision**: Data is naturally `EpochPartitioned` on read, not randomly distributed.

**Advantages**:
- Queries with time predicates can prune entire epochs
- Workers don't need to coordinate which partitions they own
- Natural alignment with temporal analytics workloads

**Trade-offs**:
- Hash-partitioned tables require explicit epoch assignment strategy
- Time-based skew may cause load imbalance

### 3.5 Single Coordinator Model

**Decision**: One worker acts as coordinator for a query, receiving the final gathered result.

**Advantages**:
- Simplified execution model
- No distributed consensus needed for result assembly
- Natural fit for Flight's request-response model

**Trade-offs**:
- Coordinator becomes bottleneck for result-heavy queries
- Memory pressure on coordinator for large result sets

### 3.6 Connection Pool Sizing

The system uses connection pooling to balance parallelism against memory pressure. Higher pool sizes increase concurrent query capacity but risk memory exhaustion under load.

### 3.7 Optimization Path: When Statistics Are Sufficient

The LDP system exhibits significant performance improvements when statistics are exact and complete. This "happy path" demonstrates how the system optimizes common analytics patterns.

#### The Three Conditions for Broadcast Optimization

```
can_optimize_to_broadcast:
    1. is_join_context = true      // This is a JOIN, not GROUP BY
    2. stats_source = Exact        // Statistics come from ingestion metadata
    3. estimated_bytes <= 256MB    // Small enough to broadcast
```

When ALL three conditions are met, the planner selects Broadcast over HashPartition shuffle.

#### Happy Path: Fact-Dimension Join with Exact Stats

Consider a typical star-schema query:

```sql
SELECT p.category, SUM(s.revenue)
FROM sales s
JOIN products p ON s.product_id = p.product_id
WHERE s.dt >= '2025-01-01'
GROUP BY p.category
```

**Scenario**: Sales (fact table) has 100M rows across 4 workers, Products (dimension) has 10K rows with exact stats.

```
Without Optimization (stats unknown):
┌─────────────────────────────────────────────────────────────┐
│  Stage 0: Scan sales → HashPartition(product_id)    │
│  Stage 1: Scan products → HashPartition(product_id) │
│  Stage 2: Hash Join (both sides shuffled)             │
│                                                       │
│  Data Movement: ~100M rows shuffled + 10K rows shuffled│
└─────────────────────────────────────────────────────────────┘

With Optimization (exact stats, small dimension):
┌─────────────────────────────────────────────────────────────┐
│  Stage 0: Scan products → Gather to coordinator       │
│  Stage 1: Broadcast products to all workers           │
│  Stage 2: Scan sales + Local Join (no sales shuffle!) │
│                                                       │
│  Data Movement: 10K rows broadcast to 4 workers        │
│  Savings: ~99.99% reduction in data movement           │
└─────────────────────────────────────────────────────────────┘
```

#### Decision Flow for Join Exchange Selection

```
determine_join_exchanges(left_stats, right_stats):
    
    left_can_broadcast  = exact(left)  AND small(left)
    right_can_broadcast = exact(right) AND small(right)
    
    match (left_can_broadcast, right_can_broadcast):
        (true, true)   → broadcast smaller side
        (true, false)  → broadcast left
        (false, true)  → broadcast right
        (false, false) → shuffle both sides (baseline)
```

#### Performance Impact Summary

| Scenario | Stats Quality | Exchange Type | Data Movement |
|----------|---------------|---------------|---------------|
| Small dim join | Exact | Broadcast | Minimal (dim only) |
| Small dim join | Unknown | Shuffle both | High (both tables) |
| Large table join | Any | Shuffle both | High (both tables) |
| Co-partitioned join | Any | None | Zero |

### 3.8 Handling Dimension Tables (Non-Epoch Data)

In typical analytics workloads, **fact tables** are epoch-partitioned (transactions, events by time), while **dimension tables** (products, customers, regions) often lack temporal partitioning.

#### The Dimension Table Challenge

Dimension tables differ from fact tables:
- No natural time-based epochs
- Relatively static data (updated infrequently)
- Small enough to fit on a single worker or be replicated
- Joined with fact tables using non-time keys

#### Current System Behavior

The metadata system is primarily designed for epoch-based data. For dimension tables without epochs:

1. **Singleton Registration**: Register the dimension table as a single "epoch" on one worker
   ```
   Dimension table "products" → Single epoch on worker w1
   TableScanStats:
       workers: [w1]
       distribution: Singleton(w1)
       stats: Exact(10000 rows, 500KB)
   ```

2. **Broadcast Join Path**: When joined with epoch-partitioned fact tables:
   - Dimension (Singleton) scanned on w1
   - If small + exact stats → Broadcast to all fact workers
   - Each worker joins locally with its fact data partition

#### Recommended Patterns for Dimension Tables

**Pattern 1: Singleton + Broadcast (Small Dimensions)**
```
Dimension Table Registration:
    dataset_id: "products"
    epoch_id: "singleton"           // Single "epoch" covering all time
    time_range: (0, MAX)            // Matches any time query
    worker: w1                      // Lives on one node
    stats: Exact(10K rows, 500KB)   // Enable broadcast optimization
    
Query: sales JOIN products ON product_id
    → Products: Singleton(w1) → Broadcast(all_workers)
    → Sales: EpochPartitioned(workers) → No shuffle needed
    → Join locally on each worker
```

**Pattern 2: Replicated Dimension (Pre-loaded)**
```
Dimension Table Registration:
    Load products onto ALL workers during startup
    Each worker holds identical copy
    
    distribution: Replicated([w1, w2, w3, w4])
    
Query: sales JOIN products ON product_id
    → Products: Already Replicated → No exchange needed
    → Sales: EpochPartitioned(workers) → No shuffle needed
    → Join locally (best case - zero data movement)
```

**Pattern 3: Hash-Partitioned Dimension (Large Dimensions)**
```
For large dimension tables (e.g., 100M customer records):
    Pre-partition by join key (customer_id)
    Register each partition as an "epoch" on different workers
    
    distribution: HashPartitioned([customer_id], workers)
    
Query: transactions JOIN customers ON customer_id
    → Both sides hash-partitioned by customer_id
    → Co-located join (if fact table also hash-partitioned by same key)
```

#### Statistics Source for Dimension Tables

| Source | Confidence | Optimization Enabled |
|--------|------------|---------------------|
| Ingestion metadata (row/byte count) | Exact | Broadcast for small dims |
| Catalog registration | Exact | Full optimization |
| External system sync | Estimated | Safety factor applied |
| Unknown | Unknown | Conservative shuffle |

**Recommendation**: Always provide exact statistics for dimension tables during registration to enable broadcast join optimization.

---

## 4. Algorithm Process

### 4.1 The Unified Algorithm: Core Insight

The LDP planning algorithm embodies a key design principle:

> **Single unified algorithm driven by distribution property enforcement.**

Unlike traditional distributed query planners that use pattern-matching or rule-based rewriting (e.g., "if join with small table, use broadcast"), LDP uses a **property-driven** approach where:

1. **One recursive traversal** handles all query shapes
2. **Distribution requirements** are the **only** operator-specific knowledge
3. **Exchange decisions** emerge naturally from comparing actual vs. required properties
4. **No special cases** for different query patterns (star schema, snowflake, etc.)

#### Why This Matters

Traditional approaches require:
- Dozens of transformation rules for different patterns
- Complex rule ordering and interaction management
- Separate handling for joins, aggregations, sorts, etc.

The unified algorithm requires only:
- A requirements function per operator type
- A single comparison: `actual.satisfies(required)?`
- A single exchange selection function

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    UNIFIED ALGORITHM FLOW                               │
│                                                                         │
│   For ANY query shape, the same algorithm applies:                      │
│                                                                         │
│   ┌──────────────┐      ┌──────────────┐      ┌──────────────┐         │
│   │  Annotate    │ ───▶ │   Enforce    │ ───▶ │    Cut       │         │
│   │  (bottom-up) │      │ (local check)│      │  (at edges)  │         │
│   └──────────────┘      └──────────────┘      └──────────────┘         │
│         │                      │                     │                  │
│         ▼                      ▼                     ▼                  │
│   Compute output        actual ≠ required?      Exchange =             │
│   distribution          Insert exchange         Stage boundary         │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### 4.2 The Single-Pass Algorithm

The entire planning happens in **one recursive traversal** of the Substrait tree:

```
annotate_and_enforce(rel):
    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 1: RECURSE                                                 │
    │   children = [annotate_and_enforce(child) for child in rel]     │
    │                                                                 │
    │   // Now we know each child's output distribution               │
    └─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 2: GET REQUIREMENTS (only operator-specific code)          │
    │   requirements = get_requirements(rel)                          │
    │                                                                 │
    │   // e.g., Sort → [Singleton]                                   │
    │   //       Join → [HashPartitioned(left_keys),                  │
    │   //               HashPartitioned(right_keys)]                 │
    └─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 3: ENFORCE (generic comparison logic)                      │
    │   for (child, required) in zip(children, requirements):         │
    │       actual = child.distribution                               │
    │       if NOT required.is_satisfied_by(actual):                  │
    │           exchange = determine_exchange(actual, required)       │
    │           child.exchange_before = exchange                      │
    │           child.distribution = post_exchange_distribution       │
    └─────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
    ┌─────────────────────────────────────────────────────────────────┐
    │ STEP 4: COMPUTE OUTPUT                                          │
    │   output_distribution = compute_output(rel, child_distributions)│
    │                                                                 │
    │   return AnnotatedRel(rel, output_distribution, children)       │
    └─────────────────────────────────────────────────────────────────┘
```

#### The Key Insight: Local Decisions, Global Correctness

Each operator makes **local** decisions:
- "What distribution do I need from my inputs?"
- "Does my input satisfy that requirement?"
- "If not, what exchange fixes it?"

These local decisions **compose** to produce a globally correct distributed plan. No global analysis or multi-pass optimization is needed.

### 4.3 Requirements: The Only Operator-Specific Knowledge

The `get_requirements()` function is the **single location** containing operator-specific logic:

```
get_requirements(rel) → Vec<RequiredDistribution>

// This is literally the ONLY place that knows:
//   - Sort needs Singleton
//   - Join needs HashPartitioned
//   - Filter needs Any
```

| Operator | Requirements | Rationale |
|----------|--------------|----------|
| Read | `[]` (leaf) | No inputs |
| Filter | `[Any]` | Distribution-preserving |
| Project | `[Any]` | Distribution-preserving |
| Sort | `[Singleton]` | Global ordering |
| Fetch | `[Singleton]` | Global LIMIT/OFFSET |
| Aggregate (global) | `[Singleton]` | Single result row |
| Aggregate (grouped) | `[HashPartitioned(group_keys)]` | Correctness |
| Join (equi) | `[HashPartitioned(L), HashPartitioned(R)]` | Correctness |
| Cross Join | `[Any, Any]` | Broadcast handled separately |
| Window | `[Singleton]` | Conservative |
| Set (UNION) | `[Any, Any, ...]` | Combined at coordinator |

### 4.4 Satisfaction Check: The Core Logic

The enforcement step uses a simple satisfaction check:

```
RequiredDistribution::is_satisfied_by(actual: Distribution) → bool

    Any:
        return true  // Any distribution works
    
    Singleton:
        return actual is Singleton
    
    HashPartitioned(required_keys):
        match actual:
            HashPartitioned(actual_keys) → required_keys == actual_keys
            Replicated → true  // Replicated satisfies any partitioning
            _ → false
```

This simple check determines whether an exchange is needed—no complex pattern matching.

### 4.5 Exchange Selection

When `actual ≠ required`, the algorithm selects an exchange:

```
determine_exchange(actual, required, stats, policy, context):
    
    if required is Singleton:
        if can_gather(stats, policy):
            return Gather(coordinator)
        else:
            return Reject(GatherTooLarge)
    
    if required is HashPartitioned(keys):
        // Baseline-first: shuffle is always correct
        // Broadcast is an optimization with strict conditions
        
        if is_join_context AND stats.is_exact AND stats.bytes ≤ broadcast_max:
            return Broadcast(target_workers)
        
        if can_shuffle(stats, policy):
            return HashPartition(keys, num_partitions)
        else:
            return Reject(ShuffleTooLarge)
```

**Join-Specific Optimization**:

For joins, both sides may need exchanges. A coordinated decision picks the best strategy:

```
determine_join_exchanges(left_stats, right_stats, left_keys, right_keys):
    
    left_can_broadcast  = exact_stats AND small(left)
    right_can_broadcast = exact_stats AND small(right)
    
    match (left_can_broadcast, right_can_broadcast):
        (true, true)  → broadcast smaller side
        (true, false) → broadcast left
        (false, true) → broadcast right
        (false, false) → shuffle both sides by their keys
```

### 4.6 Stage Cutting: Exchange = Boundary

After annotation, exchanges mark where to cut into stages:

```
cut_into_stages(annotated_root):
    stages = []
    edges = []
    
    traverse(node):
        for child in node.children:
            if child.exchange_before is not None:
                // CUT HERE
                child_stage = create_stage(child)
                stages.append(child_stage)
                
                // Create edge for data flow
                edge = ExchangeEdge(
                    exchange = child.exchange_before,
                    from_stage = child_stage.id,
                    to_stage = current_stage.id
                )
                edges.append(edge)
                
                // Replace child with placeholder
                replace_with_read_placeholder(child, edge.id)
            else:
                // Inline child into current stage
                traverse(child)
    
    root_stage = create_stage(annotated_root)
    stages.append(root_stage)
    
    return LdpPlan(stages, edges, root_stage)
```

### 4.7 Metadata Integration

The algorithm queries metadata lazily during annotation:

```
For Read(table_name) operators:
    stats = metadata.get_table_scan_stats(table_name)
    
    TableScanStats:
        rows: u64         // Sum across epochs
        bytes: u64        // Sum across epochs  
        workers: Vec<ID>  // Workers holding epochs
        stats_source: Exact | Estimated | Unknown
```

Statistics confidence propagates through the tree:
- Filter/Project: inherit child's confidence
- Join: take minimum confidence of both sides
- Unknown stats → safety factor applied → baseline (shuffle) used

### 4.8 Execution: Topological Stage Ordering

Once planning produces an LdpPlan with stages connected by exchange edges, execution follows a **dependency-driven** order:

#### The Topological Ordering Principle

Stages form a **Directed Acyclic Graph (DAG)** where:
- Each stage is a node
- Each exchange edge represents a data dependency (from → to)
- A stage cannot execute until all its upstream stages complete

```
Example DAG:

  Stage 0 (scan workers w1,w2)          Stage 1 (scan workers w3,w4)
       \                                    /
        \      Gather                     /  Gather
         \      ↓                        /    ↓
          └─────→ Stage 2 (join) ←──────┘
                       │
                       │ Gather
                       ↓
                 Stage 3 (aggregate)

Execution Order: [0, 1] → 2 → 3
              (0,1 can run in parallel)
```

#### Kahn's Algorithm for Ordering

The executor uses Kahn's algorithm to determine execution order:

```
topological_order(stages, edges):
    in_degree[stage] = count of incoming edges
    queue = stages where in_degree == 0  // Leaf stages
    
    order = []
    while queue not empty:
        stage = queue.pop()
        order.append(stage)
        
        for downstream in stage.downstream_stages:
            in_degree[downstream] -= 1
            if in_degree[downstream] == 0:
                queue.push(downstream)
    
    return order
```

This guarantees:
1. **Correctness**: No stage executes before its inputs are ready
2. **Parallelism**: Independent stages (same level in DAG) can run concurrently
3. **Determinism**: Same plan always produces same execution order

#### Execution Loop

The executor processes stages in topological order:

```
execute(plan):
    1. Compute topological order of stages
    2. For each stage in order:
        a. Resolve inputs from upstream exchanges
        b. Submit stage to target workers
        c. Store output tickets for downstream stages
    3. Fetch final output from root stage
    4. Return concatenated RecordBatches
```

**Exchange Resolution Process**:

Before executing a stage, the executor resolves its exchange inputs:

1. **Identify Dependencies**: Find all exchange edges pointing to this stage
2. **Fetch Upstream Data**: For each exchange, retrieve data from source workers
3. **Apply Exchange Logic**:
   - **Gather**: Concatenate all source data
   - **Broadcast**: Return same data for all targets
   - **HashPartition**: Filter to partitions assigned to this worker
4. **Register as Tables**: Make exchange data available as virtual tables

### 4.9 Output Distribution Computation

After enforcement, each operator computes its output distribution based on operator semantics:

| Operator | Output Distribution | Notes |
|----------|---------------------|-------|
| Read | EpochPartitioned(epoch workers) | Natural distribution from data placement |
| Filter | Same as input | Distribution-preserving |
| Project | Same as input | Distribution-preserving |
| Sort | Singleton(coordinator) | After Gather |
| Fetch | Singleton(coordinator) | After Gather |
| Aggregate (global) | Singleton | Single result row |
| Aggregate (grouped) | HashPartitioned(group keys) | Partial results per partition |
| Join | Same as left (probe) side | Standard join convention |
| Cross/Nested Loop | Same as left side | — |
| Set (UNION) | Same as first input | Combined stream |

---

## 5. Data Structures and Exchange Semantics

### 5.1 Distribution Properties

Every node in the annotated plan carries a **distribution property** describing how its output data is spread across workers:

| Distribution | Meaning | Typical Source |
|--------------|---------|----------------|
| **Singleton** | All data on one worker | After Gather, scalar aggregates |
| **EpochPartitioned** | Natural partitioning by epochs | Table scans |
| **HashPartitioned** | Partitioned by hash of columns | After shuffle |
| **Replicated** | Same data on all workers | After Broadcast |

Additionally, each node carries **statistics** (row count, byte size) and a **confidence level** (Exact, Estimated, Unknown) that influence exchange selection.

### 5.2 Exchange Semantics

#### Gather Exchange

Collects all data to a single target (coordinator):

```
Source Workers: [w1, w2, w3]
    │    │    │
    └────┼────┘
         │ All data flows to coordinator
         ▼
Target Worker: coordinator

Input Distribution:  EpochPartitioned([w1, w2, w3])
Output Distribution: Singleton(coordinator)
```

**Semantics**:
- All source worker data collected at one destination
- Output distribution becomes Singleton
- Used when global ordering or scalar aggregation required

#### Broadcast Exchange

Replicates data to all target workers:

```
Source Worker: w1 (small dimension table)
         │
    ┌────┼────┐
    │    │    │ Same data copied to all
    ▼    ▼    ▼
Targets: [w1, w2, w3]

Input Distribution:  Singleton(w1)
Output Distribution: Replicated([w1, w2, w3])
```

**Semantics**:
- Single source's data replicated to all targets
- Output distribution becomes Replicated
- Enables local joins without shuffle on receiving workers

#### HashPartition Exchange

Redistributes data by hash of key columns:

```
Source Workers: [w1, w2]
    ┌───────┬───────┐
    │ hash=0│ hash=1│ hash=2│ hash=3│
    └───────┴───────┴───────┴───────┘
              │   │   │   │
    ┌─────────┼───┼───┼───┼─────────┐
    │  p0,p2  │   │   │  p1,p3      │
    ▼         │   │   │             ▼
   w1         │   │   │            w2

Input Distribution:  EpochPartitioned([w1, w2])
Output Distribution: HashPartitioned(field_refs, [w1, w2])
```

**Semantics**:
- Each row hashed by key columns to determine target partition
- Each worker receives subset of partitions assigned to it
- Output distribution becomes HashPartitioned
- Ensures co-location of rows with same key values

### 5.3 Stage Representation

An **LdpPlan** represents the complete distributed execution plan:

- **Stages**: Independent execution units, each containing a Substrait fragment
- **Edges**: Data flow connections between stages via exchanges
- **Root Stage**: The final stage producing query results
- **Coordinator**: The worker receiving the final gathered result

Each **Stage** encapsulates:

- **Target Workers**: Which workers execute this stage
- **Inputs**: Either local catalog tables or exchange inputs from upstream stages
- **Substrait Fragment**: The serialized query plan for this stage
- **Resource Limits**: Timeout, memory budget, output limits

### 5.4 Substrait Fragment Extraction

When cutting stages, the annotated tree is converted to executable Substrait:

1. **Placeholder Insertion**: Exchange children replaced with `ReadRel(__exchange_N)`
2. **Reconstruction**: Parent operators rebuilt with new children
3. **Serialization**: Each stage's fragment serialized as standalone Substrait bytes

```
Original Tree:           Cut Tree (Stage 1):     Cut Tree (Stage 0):
    Sort                      Sort                   Read(sales)
      │                         │                        │
   Gather                  Read(__exchange_0)       Filter
      │
    Filter
      │
  Read(sales)
```

### 5.5 Distributed Execution via Arrow Flight

In distributed mode, stage submission and result retrieval use Arrow Flight:

**Stage Submission**:
1. Coordinator serializes stage definition and input data
2. Each target worker receives its portion via Flight DoPut
3. Worker executes the Substrait fragment locally using DuckDB
4. Worker stores results and returns a ticket for later retrieval

**Result Retrieval**:
1. Coordinator requests results using the ticket via Flight DoGet
2. Worker streams RecordBatches back
3. Results cached until retrieval, then cleaned up

### 5.6 Resource Limits

Each stage operates within configurable resource bounds:

- **Output Limits**: Maximum rows and bytes a stage can produce
- **Timeout**: Maximum execution time before cancellation
- **Memory Budget**: Memory ceiling for stage execution

**Admission Control** at query entry:
- Reject recursive CTEs (unbounded computation)
- Require date predicates for epoch pruning
- Fail fast on syntax errors

---

## 6. Example Query Flow

Consider: `SELECT region, SUM(revenue) FROM sales WHERE dt >= '2025-01-01' GROUP BY region`

### Step 1: SQL Transformation

```sql
-- Original
SELECT region, SUM(revenue) FROM sales WHERE dt >= '2025-01-01' GROUP BY region

-- Transformed (with epoch pruning)
SELECT region, SUM(revenue) FROM sales_macro('2025-01-01', '9999-12-31')
WHERE dt >= '2025-01-01' GROUP BY region
```

### Step 2: Substrait Generation

DuckDB produces:
```
Aggregate(group=[region], agg=[SUM(revenue)])
    └── Filter(dt >= '2025-01-01')
            └── Read(sales)
```

### Step 3: Distribution Annotation

Assuming sales epochs on workers w1, w2:

```
Aggregate (output: Singleton)     ← requires: HashPartitioned([region])
    │                                 actual: EpochPartitioned([w1, w2])
    │                                 → Insert Gather exchange
    │
    └── Filter (output: EpochPartitioned([w1, w2]))  ← requires: Any
            │
            └── Read (output: EpochPartitioned([w1, w2]))
```

### Step 4: Stage Cutting

```
Stage 0 (workers: [w1, w2]):
    Filter(dt >= '2025-01-01')
        └── Read(sales)

Exchange 0: Gather(coordinator)

Stage 1 (workers: [coordinator]):
    Aggregate(group=[region], agg=[SUM(revenue)])
        └── Read(__exchange_0)
```

### Step 5: Execution

1. Execute Stage 0 on w1 and w2 in parallel
2. Execute Gather exchange: coordinator fetches from w1, w2
3. Execute Stage 1 on coordinator with gathered data
4. Return aggregated result

---

## 7. Key Design Principles Summary

1. **Substrait-First**: No custom IR, work directly with Substrait
2. **Baseline-First**: Shuffle is always correct; broadcast is an optimization
3. **Conservative Stats**: Unknown/estimated stats trigger safety factors
4. **Epoch-Native**: Natural time-based data partitioning
5. **Zero-Copy Pipeline**: Arrow throughout (DuckDB → Flight → Client)
6. **Trait-Based Abstraction**: `StageExecutor`, `Metadata` enable testing and extension
7. **Multi-Tenant Isolation**: `tenant_id` in all control plane messages
8. **Resource Limits**: Configurable bounds prevent runaway queries

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
| `safety_factor` | 2.0 | Applied to uncertain stats |

### CoordinatorConfig Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `query_timeout` | 5 min | Maximum query execution time |
| `max_concurrent_stages` | 16 | Parallelism limit |
| `max_stage_retries` | 3 | Retry count per failed stage |
| `distributed` | false | Enable Flight-based distribution |

---

## Appendix B: Error Handling

### Planning Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `Rejected(ShuffleTooLarge)` | Shuffle exceeds `shuffle_bytes_max` | Add filters to reduce data |
| `Rejected(GatherTooLarge)` | Gather exceeds limits | Add aggregation before gather |
| `MissingMetadata` | Table not found in metadata | Register table with coordinator |
| `InvalidPlan` | Malformed Substrait | Check SQL validity |

### Execution Errors

| Error | Cause | Resolution |
|-------|-------|------------|
| `StageNotFound` | Stage ID mismatch | Internal error |
| `StageFailed` | Worker execution error | Check worker logs |
| `ExchangeFailed` | Network/Flight error | Retry or check connectivity |
| `WorkerUnavailable` | Worker offline | Remove from pool |

---

**Revision History**:
- 2025-01-11: Initial design document

