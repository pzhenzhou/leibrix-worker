# SQL Module Design

## 1. Purpose

The SQL module provides **logical dataset abstraction** over physical epoch tables. It transforms client SQL queries from logical table names to DuckDB table macro calls, enabling:

- **Client Transparency**: Users query logical datasets without knowing about physical partitioning
- **Epoch Pruning**: Automatic optimization via macro parameters derived from query predicates
- **Semantic Parity**: Transformed queries produce identical results to original queries

## 2. Architecture

```
Client SQL → Parser → Discovery → Analysis → Transformation → DuckDB SQL
                         ↓           ↓            ↓
                    Table Refs   Date Ranges   Macro Calls
```

The module is a **pure transformation layer** above the query engine. It performs no query execution.

## 3. Core Abstractions

### 3.1 DateBound
Represents date interval endpoints:
- `Literal(String)`: Concrete date like `"2025-01-15"`
- `Parameter(String)`: SQL parameter like `"?p1"`

**Design Decision**: Implements total ordering (`Ord`) for use in `min`/`max` operations. Literals sort before Parameters; Parameters sort lexicographically.

### 3.2 Interval
A half-open date range `[start, end)` using interval algebra:
- `start: Option<DateBound>`: Inclusive lower bound (`None` = -∞)
- `end: Option<DateBound>`: Exclusive upper bound (`None` = +∞)

**Operations**:
- `intersect(A, B)`: Returns tightest range satisfying both (for `AND`)
- `union(A, B)`: Returns widest range covering both (for `OR`)

**Design Decision**: Half-open intervals simplify date arithmetic and avoid off-by-one errors.

### 3.3 ScopeId & TableReference
**ScopeId**: Unique identifier for query context (Main, CTE, Subquery)

**TableReference**: Tracks logical table usage within a scope:
- Table name and alias
- `ScopeId` for disambiguation
- Used to attribute predicates correctly

**Design Decision**: Explicit scope tracking prevents predicate misattribution in nested queries.

### 3.4 RegisteredDataset
Configuration for a logical dataset:
- `logical_name`: Client-facing table name
- `macro_name`: DuckDB table macro function name
- `date_column`: Column name for date filtering

## 4. SQL Rewriting Algorithm

### Phase 1: Parse
Uses `sqlparser-rs` to convert SQL strings into Abstract Syntax Trees (ASTs). Provides round-trip guarantee: SQL → AST → SQL.

### Phase 2: Discovery
**Goal**: Find all references to registered logical tables.

**Process**:
1. Traverse AST recursively
2. For each table reference, check if it matches a registered dataset
3. Record table name, alias, and `ScopeId`
4. Build `Vec<TableReference>` for next phase

**Edge Cases**:
- Aliased tables (`SELECT * FROM sales AS s`)
- CTEs (`WITH tmp AS (...) SELECT * FROM tmp`)
- Subqueries in `FROM`, `WHERE`, `JOIN`

### Phase 3: Analysis (Interval Algebra)
**Goal**: Extract date range for each logical table.

**Algorithm** (`BooleanAnalyzer`):
1. For each registered table, initialize interval as `Universe` (-∞, +∞)
2. Recursively traverse `WHERE` and `JOIN ON` clauses
3. For each comparison (`dt >= '2025-01-01'`):
   - Extract date bound
   - Determine which table the predicate applies to
   - Create an `Interval` from the predicate
4. Combine intervals per boolean logic:
   - `AND`: `current.intersect(new_interval)`
   - `OR`: `current.union(new_interval)`
5. For multi-table predicates in `OR`: conservatively assign `Universe` to all tables

**Predicate Attribution**:
- Track all table references in current scope
- Match column qualifiers (`sales.dt`) to table aliases
- For unqualified columns, infer table if unambiguous

**Semantic Guarantees**:
- Conservative extraction: never narrow bounds incorrectly
- `is_empty()` only returns `true` for provably empty intervals (literal dates)
- Parameters default to `Universe` in `to_macro_params()`

### Phase 4: Transformation
**Goal**: Rewrite AST to replace logical tables with macro calls.

**Process**:
1. For each `TableReference`, replace table name with macro call:
   ```
   sales → sales_macro('2025-01-01', '2025-12-31')
   ```
2. Derive macro parameters from the table's `Interval`:
   - `start`: Defaults to `'1970-01-01'` if `None` or `Parameter`
   - `end`: Defaults to `'9999-12-31'` if `None` or `Parameter`
3. Preserve original `WHERE` and `JOIN ON` clauses **unchanged**

**Double-Guard Strategy**:
- Macro parameters: Performance optimization hints for epoch pruning
- Original predicates: Correctness guarantee at row level
- Result: Safe even if interval analysis is overly conservative

## 5. Key Design Decisions

### 5.1 Non-Destructive Transformation
**Rationale**: Query correctness takes precedence over optimization. By preserving original predicates, the system cannot return incorrect results, even if the interval analysis is imperfect.

**Trade-off**: Slight redundancy (predicate checked twice: macro filter + WHERE clause), but guarantees semantic parity.

### 5.2 Conservative Interval Extraction
**Rationale**: When uncertain about predicate meaning (e.g., parameters, complex expressions), default to widest interval (`Universe`).

**Trade-off**: Potential over-scanning of epochs, but never under-scanning (which would cause missing data).

### 5.3 Total Ordering for DateBound
**Rationale**: Rust's `min`/`max` require `Ord`. To use these functions in interval operations, `DateBound` must define consistent ordering for all variants, including non-comparable parameters.

**Trade-off**: Lexicographic ordering of parameters is arbitrary but deterministic, ensuring reproducible behavior.

### 5.4 Scope-Aware Analysis
**Rationale**: SQL queries can have multiple tables with the same name in different scopes (e.g., CTE named `sales` and base table `sales`). Without scope tracking, predicates would be misattributed.

**Trade-off**: Added complexity in discovery and analysis phases, but necessary for correctness.

### 5.5 Interval Algebra over Ad-Hoc Merging
**Rationale**: Initial implementation used simple `DateRange` with ad-hoc merging, which failed for complex boolean logic. Interval algebra provides formal semantics for `AND`/`OR` and handles edge cases (empty, universe, parameters) uniformly.

**Trade-off**: More upfront design, but cleaner code and fewer bugs.

## 6. Error Handling

**Strategy**: Fail fast and propagate errors up the stack.

**Error Types**:
- `ParseError`: Invalid SQL syntax
- `TransformError`: Transformation failure (should be rare)
- Fatal errors abort the query; engine remains operational

## 7. Performance Characteristics

- **Parse/Transform Overhead**: O(AST nodes), typically < 1ms for queries < 1000 lines
- **Memory**: Single AST in memory, minimal cloning
- **Epoch Pruning Benefit**: Reduces I/O by 10-100x for time-range queries on large datasets

## 8. Testing Strategy

**Unit Tests** (69 tests covering):
- Interval algebra operations (intersection, union, empty)
- DateBound ordering (literals, parameters, mixed)
- Discovery (CTEs, subqueries, joins, aliases)
- Analysis (AND/OR, cross-table predicates, scope isolation)
- Transformation (macro calls, semantic parity, round-trip)

**Property**: `original_query_result == transformed_query_result`

## 9. Future Extensions

### 9.1 Partition Pruning
Extend interval extraction to support multi-dimensional predicates (e.g., `region = 'US'`) for finer-grained pruning.

### 9.2 Query Hints
Allow users to provide explicit date ranges via SQL comments: `/* epoch_hint: 2025-01-01 to 2025-12-31 */`.

### 9.3 Statistics-Based Optimization
Track actual epoch scan costs to refine macro parameter generation.

## 10. Module Dependencies

```
sql/
├── mod.rs              # Public API, exports
├── types.rs            # Core data structures
├── error.rs            # Error types
├── parser.rs           # SQL ↔ AST conversion
├── discovery.rs        # Table reference extraction
├── analyzer.rs         # Legacy date range extraction (deprecated)
├── boolean_analyzer.rs # Interval algebra-based analysis
├── interval.rs         # Interval algebra implementation
└── transformer.rs      # SQL rewriting orchestration
```

**External Dependencies**:
- `sqlparser = "0.60"`: AST manipulation
- `chrono = "0.4"`: Date arithmetic

## 11. Guarantees

1. **Semantic Parity**: `∀ query Q, result(Q) = result(transform(Q))`
2. **Idempotency**: `transform(transform(Q)) = transform(Q)` (no-op if already transformed)
3. **Conservative Pruning**: Macro parameters never exclude relevant epochs
4. **Scope Isolation**: Predicates in CTEs don't affect outer query intervals

---

**Revision History**:
- 2025-12-28: Initial design document

