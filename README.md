# Leibrix Worker
⚠️ **Active development:** not production-ready yet.

**High-performance, memory-centric data plane for the Leibrix distributed acceleration layer.**

## What is Leibrix Worker?

Leibrix Worker is the Data Plane component of a distributed in-memory acceleration system for interactive analytics. It
serves as the **high-performance execution engine** that loads, stores, and queries immutable data epochs entirely from
memory, delivering predictable millisecond-level query latency with strict multi-tenant isolation.

## Role & Responsibilities

- **Data Loading**: Pulls immutable data epochs from source systems (Iceberg, StarRocks, JDBC) via Arrow Flight
- **Memory Management**: Maintains in-memory datasets within strict per-tenant memory quotas
- **Query Execution**: Executes bounded SQL queries using embedded DuckDB with vectorized processing
- **Resource Enforcement**: Enforces per-query CPU, memory, and row scan limits for predictability
- **Control Plane Integration**: Maintains persistent gRPC stream with Master for coordination and health reporting
- **Zero-Copy Processing**: Leverages Apache Arrow for efficient data movement and processing

## Core Features

### Memory-Centric Storage

- In-memory execution engine using embedded DuckDB for vectorized analytics
- Immutable epoch tables with automatic macro-based query routing
- Arrow-native data processing for zero-copy ingestion and result streaming
- Per-tenant memory quota enforcement with benefit-driven eviction

### Bounded Query Execution

- Configurable per-query resource limits (rows scanned, memory, CPU time)
- Automatic query rejection when bounds are exceeded
- P99 latency predictability through strict resource accounting
- Fallback coordination with Gateway for complex queries

### Zero-Copy Data Pipeline

- Apache Arrow Flight as primary data loading protocol
- Direct Parquet reading for Iceberg tables (no intermediate serialization)
- DuckDB's native Arrow integration for ingestion without copying
- Arrow IPC streaming for query results back to Gateway

### Multi-Tenant Isolation

- Exclusive worker-to-tenant assignment (no cross-tenant interference)
- Hard isolation via dedicated DuckDB instance per tenant
- Per-tenant concurrency caps and memory budgets
- Independent failure domains for tenant workloads

### Intelligent Lifecycle Management

- Epoch-based data organization with automatic table macro updates
- Dynamic epoch addition/removal without query interruption
- LRU or benefit-score driven epoch eviction under memory pressure
- Graceful shutdown with in-flight query coordination

## Architecture

Leibrix Worker is a Rust-based service leveraging high-performance libraries:

- **Data Loading**: Arrow Flight client + Parquet reader + JDBC adapter
- **Storage Engine**: Embedded DuckDB with Arrow C Data Interface
- **Query Service**: gRPC SQL service with Arrow streaming results
- **Control Plane**: Bidirectional gRPC stream to Master for coordination
- **Runtime**: Tokio async runtime + blocking thread pools for CPU-bound work

## Technical Stack

- **Language**: Rust (for memory safety and predictable performance)
- **Query Engine**: DuckDB (vectorized, Arrow-native OLAP engine)
- **Data Format**: Apache Arrow (zero-copy columnar memory format)
- **Network Protocol**: gRPC (control plane) + Arrow Flight (data loading)
- **Concurrency**: Tokio async runtime for I/O, thread pools for compute

## Related Components

- **[leibrix](https://github.com/pzhenzhou/leibrix)**: Master control plane for cluster coordination
- **leibrix-gateway** (future): Unified query routing layer with MySQL protocol support

## Limitations

- Not a system of record—serves cached data from upstream sources only
- Requires sufficient RAM to hold working dataset in memory
- Query complexity bounded by configured resource limits
- Schema evolution per epoch (incompatible changes require new dataset versions)
