# Arrow Flight Query Service Design for Leibrix-Worker

## 1. Arrow Flight Protocol Overview

Apache Arrow Flight is a high-performance RPC framework built on gRPC for transferring large datasets using the Arrow columnar memory format.

### Core Concepts

| Concept | Description |
|---------|-------------|
| **FlightData** | The fundamental unit of transfer - serialized Arrow RecordBatches |
| **FlightDescriptor** | Identifies a dataset (by path or command) |
| **FlightInfo** | Metadata about a dataset: schema, endpoints, total records/bytes |
| **FlightEndpoint** | Location(s) where data can be fetched |
| **Ticket** | Opaque token to retrieve specific data partitions |

### Key RPCs

| RPC | Signature | Purpose |
|-----|-----------|---------|
| `GetFlightInfo` | `FlightDescriptor → FlightInfo` | Returns metadata + endpoints for a query/table |
| `DoGet` | `Ticket → Stream<FlightData>` | Streams Arrow RecordBatches to client |
| `DoPut` | `Stream<FlightData> → Stream<PutResult>` | Ingests Arrow data from client |
| `DoAction` | `Action → Stream<Result>` | Custom actions (cancel, health, etc.) |
| `ListFlights` | `Criteria → Stream<FlightInfo>` | Enumerate available datasets |

### Why Arrow Flight for Leibrix-Worker

1. **Zero-copy transfer**: Arrow IPC format means data stays in columnar format end-to-end
2. **Streaming**: Handles large result sets without loading everything into memory
3. **Interoperability**: Native clients in Python, Java, C++, Rust, Go
4. **Architecture alignment**: `QueryEngine.execute_query()` returns `QueryResultStream` (Arrow RecordBatches)

---

## 2. Architecture Design

```
┌─────────────────────────────────────────────────────────────────┐
│                     worker-flight (new crate)                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │             FlightQueryService<Q>                         │   │
│  │   impl FlightService for FlightQueryService<Q>            │   │
│  │                                                           │   │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────────┐   │   │
│  │  │ GetFlightInfo│  │   DoGet    │  │  DoAction       │   │   │
│  │  │ (SQL→plan)  │  │ (execute)  │  │ (cancel/health) │   │   │
│  │  └──────┬──────┘  └──────┬──────┘  └─────────────────┘   │   │
│  │         │                │                                │   │
│  │         ▼                ▼                                │   │
│  │  ┌─────────────────────────────────────────────────────┐ │   │
│  │  │        Q: QueryEngine (trait bound)                  │ │   │
│  │  │  • get_table_schema()                                │ │   │
│  │  │  • get_table_metadata()                              │ │   │
│  │  │  • execute_query() → QueryResultStream               │ │   │
│  │  └─────────────────────────────────────────────────────┘ │   │
│  └──────────────────────────────────────────────────────────┘   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Directory Structure

```
src/
├── worker-flight/             # NEW crate (Arrow Flight service for workers)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs
│       ├── config.rs          # FlightServerConfig
│       ├── service.rs         # FlightQueryService implementation
│       ├── ticket.rs          # Ticket encoding/decoding
│       ├── error.rs           # Flight-specific error mapping
│       └── server.rs          # Server startup, shutdown
```

**Naming Rationale:**
- `worker-flight` emphasizes Arrow Flight protocol (the distinctive technology)
- Concise and follows Rust naming conventions
- Parallel to `worker-storage` naming pattern
- Clear that it's worker-specific infrastructure
- Avoids confusion with generic "query" services (gRPC, HTTP, etc.)

---

## 3. Core Data Structures

### Design Decision: Generic Type Parameter vs Trait Objects

The `FlightQueryService` uses a **generic type parameter `Q: QueryEngine`** instead of a trait object (`Arc<dyn QueryEngine>`). This decision follows Rust best practices for several reasons:

#### Why Not Trait Objects (`Arc<dyn QueryEngine>`)?

The `QueryEngine` trait uses **RPITIT** (Return Position Impl Trait In Traits), which was stabilized in Rust 1.75:

```rust
pub trait QueryEngine: Send + Sync {
    fn execute_query(&self, sql: &str, ...) 
        -> impl Future<Output = Result<QueryResultStream, QueryError>> + Send;
    //     ^^^^ RPITIT - requires concrete type at compile time
}
```

**RPITIT traits are NOT object-safe** because:
- The compiler needs to know the concrete `Future` type for monomorphization
- Dynamic dispatch requires a uniform size, but different implementations may return different future types
- This would require `Pin<Box<dyn Future>>` indirection, negating performance benefits

#### Why Not GATs (Generic Associated Types)?

While GATs could make the trait object-safe:

```rust
pub trait QueryEngine: Send + Sync {
    type ExecuteQueryFuture<'a>: Future<Output = Result<QueryResultStream, QueryError>> + Send + 'a
    where Self: 'a;
    
    fn execute_query<'a>(&'a self, sql: &str, ...) -> Self::ExecuteQueryFuture<'a>;
}
```

**This is NOT recommended** because:
- ❌ More verbose and complex for implementers
- ❌ Worse ergonomics compared to RPITIT
- ❌ Goes against modern Rust idioms (RPITIT is preferred since 1.75)
- ❌ Still requires `Pin<Box<dyn Future>>` for dynamic dispatch, losing performance
- ❌ No real benefit over generics for this use case

#### Why Generics (`FlightQueryService<Q: QueryEngine>`)? ✅

**This is the Rust best practice** for this scenario:

**Advantages:**
1. **Zero-Cost Abstraction** (Static Dispatch)
   - Monomorphization at compile time
   - No vtable indirection overhead
   - Better inlining and optimization opportunities
   - Aligns with Rust's core principle: "zero-cost abstractions"

2. **RPITIT Compatibility**
   - Works seamlessly with the modern trait design
   - No need to refactor traits to GATs
   - Maintains clean, readable trait definitions

3. **Type Safety**
   - Compile-time type verification
   - Better error messages
   - Catches errors earlier in the development cycle

4. **Testability**
   - Easy to create mock implementations:
     ```rust
     #[cfg(test)]
     struct MockQueryEngine { /* ... */ }
     
     #[test]
     fn test_flight_service() {
         let mock_engine = Arc::new(MockQueryEngine::new());
         let service = FlightQueryService::new(mock_engine, ...);
         // test service behavior
     }
     ```

5. **Flexibility Without Runtime Cost**
   - Can swap implementations at compile time
   - No performance penalty
   - Configuration via generics instead of dynamic dispatch

**Trade-offs:**
- Binary contains monomorphized code for each concrete type (slightly larger binary)
- Cannot switch implementations at runtime (but this is not needed in our architecture)

#### Conclusion

Given that:
1. The `QueryEngine` trait uses RPITIT (modern, ergonomic, performant)
2. The Worker is deployed with a single concrete implementation (`DuckDBQueryEngine`)
3. Zero-cost abstractions are a core Rust principle
4. Testability is easily achieved with generics

**The generic type parameter approach is the correct Rust best practice** and aligns with the existing codebase design philosophy.

---

## 4. Core Data Structures (Continued)

### QueryTicket

```rust
pub struct QueryTicket {
    pub sql: String,
    pub tenant_id: String,
    pub memory_limit_mb: Option<usize>,
    pub timeout_secs: Option<u64>,
}
```

### FlightQueryService

```rust
/// Arrow Flight query service that provides SQL query execution over Arrow Flight protocol.
///
/// Generic over `Q: QueryEngine` to enable:
/// - Zero-cost abstraction (static dispatch)
/// - Easy testing with mock implementations
/// - Type safety without runtime overhead
///
/// This follows Rust best practices by using generics instead of trait objects,
/// since `QueryEngine` uses RPITIT (Return Position Impl Trait In Traits) which
/// is not object-safe.
pub struct FlightQueryService<Q>
where
    Q: QueryEngine,
{
    query_engine: Arc<Q>,
    sql_transformer: Arc<SqlTransformer>,  // Transforms logical tables → macro calls
    tenant_id: String,  // Bound at startup (shared-nothing architecture)
}
```

### FlightServerConfig

```rust
use std::net::SocketAddr;
use tonic::transport::ServerTlsConfig;

/// Configuration for the Arrow Flight server.
/// 
/// Uses standard `tonic` types for TLS configuration to avoid reinventing the wheel.
pub struct FlightServerConfig {
    /// Address to bind the Flight server to (e.g., "0.0.0.0:8815")
    pub bind_addr: SocketAddr,
    
    /// Optional TLS configuration using tonic's built-in support.
    /// 
    /// Use `ServerTlsConfig::new()` and configure with:
    /// - `.identity()` for server certificate and key
    /// - `.client_ca_root()` for mTLS client verification
    /// 
    /// Example:
    /// ```rust
    /// use tonic::transport::{ServerTlsConfig, Identity};
    /// 
    /// let cert = std::fs::read("server.crt")?;
    /// let key = std::fs::read("server.key")?;
    /// let identity = Identity::from_pem(cert, key);
    /// 
    /// let tls_config = ServerTlsConfig::new().identity(identity);
    /// ```
    pub tls_config: Option<ServerTlsConfig>,
    
    /// Maximum message size in bytes (default: 16MB)
    /// Prevents OOM from malicious/malformed large messages
    pub max_message_size: usize,
    
    /// Maximum number of concurrent connections (default: 1000)
    /// Protects against connection exhaustion attacks
    pub concurrency_limit: usize,
}

impl Default for FlightServerConfig {
    fn default() -> Self {
        Self {
            bind_addr: "0.0.0.0:8815".parse().unwrap(),
            tls_config: None,  // TLS disabled by default
            max_message_size: 16 * 1024 * 1024,  // 16 MB
            concurrency_limit: 1000,
        }
    }
}
```

---

## 5. Implementation Tasks

### Phase 1: Foundation (Core Service)

| Task | Description |
|------|-------------|
| 1.1 | Create `worker-flight` crate with dependencies |
| 1.2 | Define `FlightQueryService<Q>` struct holding `Arc<Q: QueryEngine>` |
| 1.3 | Implement `QueryTicket` serialization (JSON) |
| 1.4 | Map `QueryError` → `arrow_flight::error::FlightError` |

### Phase 2: Core RPCs

| Task | Description |
|------|-------------|
| 2.1 | `get_flight_info`: Transform SQL (logical → macro), validate, return schema + metadata |
| 2.2 | `do_get`: Decode ticket, transform SQL, call `execute_query()`, stream `RecordBatch` → `FlightData` |
| 2.3 | `list_flights`: Use `SqlTransformer::registered_dataset_ids()` to enumerate logical datasets |

### Phase 3: Production Features

| Task | Description |
|------|-------------|
| 3.1 | Implement `do_action` for `cancel_query`, `health_check` |
| 3.2 | Add TLS support via `tonic::transport::ServerTlsConfig` (no custom TLS type needed) |
| 3.3 | Integrate with existing tracing for observability |
| 3.4 | Wire into `worker-cli` as runtime component |

---

## 6. FlightService Trait Implementation

```rust
#[tonic::async_trait]
impl<Q> FlightService for FlightQueryService<Q>
where
    Q: QueryEngine + 'static,
{
    type HandshakeStream = ...;
    type ListFlightsStream = ...;
    type DoGetStream = ...;
    type DoPutStream = ...;
    type DoActionStream = ...;
    type ListActionsStream = ...;
    type DoExchangeStream = ...;

    // Required for query execution
    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status>;

    // Required for catalog discovery
    async fn list_flights(
        &self,
        request: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status>;

    // Optional but recommended
    async fn do_action(
        &self,
        request: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status>;

    async fn list_actions(
        &self,
        request: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status>;
}
```

---

## 7. Dependencies

### worker-flight/Cargo.toml

```toml
[package]
name = "worker-flight"
version.workspace = true
edition.workspace = true

[dependencies]
arrow-flight = { version = "=56.2.0", features = ["flight-sql"] }
arrow = { version = "=56.2.0" }
arrow-schema = "=56.2.0"
arrow-ipc = "=56.2.0"

# tonic requires both 'transport' and a TLS backend feature for ServerTlsConfig
# - transport: enables Server builder and transport layer
# - tls-webpki-roots: enables TLS via rustls with Mozilla CA certificates (RECOMMENDED)
#   Alternative: tls-native-roots (uses OS certificate store)
tonic = { version = "0.13", features = ["transport", "tls-webpki-roots"] }

tokio = { workspace = true }
futures-util = { workspace = true }
tracing = { workspace = true }
thiserror = { workspace = true }
anyhow = { workspace = true }
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Internal dependency
worker-storage = { path = "../worker-storage" }

# SQL transformation dependencies (re-exported from worker-storage)
# These are already included via worker-storage, listed here for clarity:
# - sqlparser (for SQL AST manipulation)
# - chrono (for date arithmetic)
```

---

## 8. Error Mapping

| QueryError | FlightError / Status |
|------------|---------------------|
| `Timeout` | `Status::deadline_exceeded()` |
| `DuckDB` | `Status::internal()` |
| `Sql` | `Status::invalid_argument()` |
| `TableNotFound` | `Status::not_found()` |
| `TenantValidation` | `Status::permission_denied()` |
| `Internal` | `Status::internal()` |

---

## 9. Integration Points

### With Existing DuckDBQueryEngine

```rust
impl<Q> FlightQueryService<Q>
where
    Q: QueryEngine,
{
    pub fn new(
        query_engine: Arc<Q>,
        sql_transformer: Arc<SqlTransformer>,
        tenant_id: String,
    ) -> Self {
        Self { 
            query_engine, 
            sql_transformer,
            tenant_id 
        }
    }
}
```

### With worker-cli

```rust
// In worker-cli main.rs
use tonic::transport::{Server, ServerTlsConfig, Identity};
use arrow_flight::flight_service_server::FlightServiceServer;

let shared_db = Arc::new(SharedDatabase::new(&config)?);
let query_engine = Arc::new(DuckDBQueryEngine::new(shared_db.clone(), max_concurrent));

// Create SQL transformer and register logical datasets
let mut sql_transformer = SqlTransformer::new();
sql_transformer.register_dataset(RegisteredDataset::new(
    "sales_data".to_string(),
    "dt".to_string(),
));
let sql_transformer = Arc::new(sql_transformer);

// Note: FlightQueryService is generic over Q: QueryEngine
// The concrete type is DuckDBQueryEngine
let flight_service = FlightQueryService::new(query_engine, sql_transformer, tenant_id);

// Build tonic server with optional TLS
let mut server_builder = Server::builder();

if let Some(tls_cert_path) = config.flight_tls_cert {
    let cert = tokio::fs::read(&tls_cert_path).await?;
    let key = tokio::fs::read(&config.flight_tls_key.unwrap()).await?;
    let identity = Identity::from_pem(cert, key);
    
    let tls_config = ServerTlsConfig::new().identity(identity);
    server_builder = server_builder
        .tls_config(tls_config)?
        .tcp_nodelay(true);
}

// Configure limits
server_builder = server_builder
    .max_frame_size(Some(16 * 1024 * 1024))  // 16 MB
    .concurrency_limit_per_connection(256);

// Start Flight server
let flight_server = FlightServiceServer::new(flight_service);
server_builder
    .add_service(flight_server)
    .serve(config.flight_bind_addr)
    .await?;
```

**Key Points:**
- Use `tonic::transport::ServerTlsConfig` directly (no custom types)
- TLS is optional - can run without it for development/internal networks
- `Identity::from_pem()` loads certificate and private key
- Apply limits via `Server::builder()` methods
- Arrow Flight service is wrapped with `FlightServiceServer::new()`


---

## 10. Logical-to-Physical Table Mapping

### The Mapping Problem

The Worker architecture implements a clear separation between logical and physical data layers:

- **Logical Layer**: Client applications interact with logical dataset names (e.g., `sales_data`)
- **Physical Layer**: Multiple epoch tables stored in DuckDB (e.g., `sales_data__epoch_20250101`, `sales_data__epoch_20250102`, ...)
- **Mapping Mechanism**: DuckDB table macros (e.g., `scan_sales_data(start_dt, end_excl)`)

### Why This Mapping Exists

1. **Client Transparency**: Clients query a single logical table (`sales_data`) without needing to understand epoch partitioning
2. **Epoch Pruning**: Table macros enable DuckDB to prune irrelevant epochs based on date range predicates
3. **Lifecycle Autonomy**: Worker nodes can add/remove epoch tables (TTL, eviction) without breaking client queries
4. **Performance Optimization**: DuckDB's optimizer can push down predicates and perform constant folding on epoch ranges

### Table Macro Structure

For a logical dataset `sales_data` with time column `dt`:

```sql
CREATE OR REPLACE MACRO scan_sales_data(start_dt DATE, end_excl DATE) AS TABLE (
  SELECT * FROM sales_data__epoch_20250101
  WHERE (DATE '2025-01-01' >= start_dt AND DATE '2025-01-01' < end_excl)
    AND dt = DATE '2025-01-01'
UNION ALL
  SELECT * FROM sales_data__epoch_20250102
  WHERE (DATE '2025-01-02' >= start_dt AND DATE '2025-01-02' < end_excl)
    AND dt = DATE '2025-01-02'
  ...
);
```

### Query Transformation

Client applications query logical tables, but the actual SQL execution uses table macros:

**Client Query**:
```sql
SELECT * FROM sales_data WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'
```

**Transformed Query** (by `SqlTransformer`):
```sql
SELECT * FROM scan_sales_data(DATE '2025-01-01', DATE '2025-02-01')
WHERE dt BETWEEN '2025-01-01' AND '2025-01-31'
```

**Key Points**:
- Original WHERE predicates are preserved for semantic correctness
- Macro parameters act as "hints" for epoch pruning
- DuckDB optimizer eliminates epochs outside the date range via constant folding

### Impact on Arrow Flight Service

For the `FlightQueryService` implementation:

1. **`ListFlights` RPC**:
   - Returns **logical dataset names** (e.g., `sales_data`), not physical epoch tables
   - Clients discover queryable datasets via registered logical names
   - Implementation: Use `LogicalDatasetManager::list_datasets()` instead of `DuckDBQueryEngine::list_tables()`

2. **`GetFlightInfo` RPC**:
   - Accepts queries against **logical table names** (e.g., `SELECT * FROM sales_data`)
   - Must transform SQL using `SqlTransformer` before analyzing schema/metadata
   - Return schema from the logical dataset (all epochs share the same schema)

3. **`DoGet` RPC**:
   - Ticket contains SQL against **logical table names**
   - Transform SQL using `SqlTransformer` before execution
   - Execute transformed SQL via `DuckDBQueryEngine::execute_query()`

### SQL Transformation Module

The `worker-storage/src/sql/` module provides the transformation logic:

- **`SqlTransformer`**: Main entry point for query transformation
- **`RegisteredDataset`**: Tracks logical dataset metadata (name, time column, macro name)
- **Discovery Phase**: Identifies logical table references in SQL AST
- **Analysis Phase**: Extracts date range predicates from WHERE clauses
- **Transformation Phase**: Replaces logical tables with macro calls

**Example Usage**:
```rust
let mut transformer = SqlTransformer::new();
transformer.register_dataset(RegisteredDataset::new(
    "sales_data".to_string(),
    "dt".to_string(),
));

let result = transformer.transform(client_sql)?;
let transformed_sql = result.transformed_sql;

// Execute transformed SQL
let stream = query_engine.execute_query(&transformed_sql, memory_limit, timeout).await?;
```

### Design Implication for Flight Service

The `FlightQueryService` **must be aware of the logical-to-physical mapping**:

```rust
pub struct FlightQueryService {
    query_engine: Arc<DuckDBQueryEngine>,
    sql_transformer: Arc<SqlTransformer>,  // NEW: SQL transformation layer
    tenant_id: String,
}

impl FlightQueryService {
    async fn do_get(&self, ticket: Ticket) -> Result<Stream<FlightData>, Status> {
        let query_ticket = deserialize_ticket(&ticket)?;
        
        // Validate tenant
        if query_ticket.tenant_id != self.tenant_id {
            return Err(Status::permission_denied("Tenant ID mismatch"));
        }
        
        // Transform SQL: logical tables → macro calls
        let transform_result = self.sql_transformer
            .transform(&query_ticket.sql)
            .map_err(|e| Status::invalid_argument(format!("SQL transformation failed: {}", e)))?;
        
        // Execute transformed SQL
        let stream = self.query_engine
            .execute_query(
                &transform_result.transformed_sql,
                query_ticket.memory_limit_mb,
                query_ticket.timeout_secs.map(Duration::from_secs),
            )
            .await
            .map_err(|e| map_query_error_to_status(e))?;
        
        // Convert Arrow RecordBatch stream to FlightData stream
        Ok(arrow_stream_to_flight_data(stream))
    }
}
```

### Summary

- **Physical Tables**: `{dataset_id}__{epoch_id}` (returned by `DuckDBQueryEngine::list_tables()`)
- **Logical Tables**: `{dataset_id}` (what clients query)
- **Mapping Mechanism**: Table macros `scan_{dataset_id}(start_dt, end_excl)` that UNION physical epoch tables
- **Transformation**: `SqlTransformer` rewrites client SQL from logical → macro calls
- **Flight Service Role**: Accept logical table queries, transform to macro calls, execute, return results

This mapping is **essential for client transparency** and enables the Worker to autonomously manage epoch lifecycles (add/remove/evict) without breaking client applications.

---

## 11. Request Flow

```
Client                          FlightQueryService                DuckDBQueryEngine
  │                                    │                                 │
  │─── GetFlightInfo(SQL) ────────────►│                                 │
  │   (logical table: sales_data)      │                                 │
  │                                    │── transform SQL ────────────────│
  │                                    │   (logical → macro call)        │
  │                                    │◄── get_table_metadata() ────────│
  │◄── FlightInfo(schema, endpoints) ──│                                 │
  │                                    │                                 │
  │─── DoGet(Ticket) ─────────────────►│                                 │
  │                                    │── decode ticket ────────────────│
  │                                    │── transform SQL ────────────────│
  │                                    │── execute_query(transformed) ──►│
  │                                    │◄── QueryResultStream ───────────│
  │◄── Stream<FlightData> ─────────────│                                 │
  │◄── Stream<FlightData> ─────────────│                                 │
  │◄── (stream complete) ──────────────│                                 │
```
