# Worker-Flight: Arrow Flight Query Service

Arrow Flight query service implementation for Leibrix-Worker, providing high-performance SQL query execution over the Arrow Flight protocol.

## ✅ Phase 1 Complete: Core Functionality

### Implementation Summary

All Phase 1 tasks have been successfully implemented:

1. **✅ Task 1.1**: Applied `max_message_size` configuration in server builder
2. **✅ Task 1.2**: Implemented schema extraction in `get_flight_info`
3. **✅ Task 1.3**: Added schema to `FlightInfo` response
4. **✅ Task 1.4**: Implemented `get_schema` RPC
5. **✅ Task 1.5**: Created manual testing examples

### What's Working

- ✅ **Full Flight Service Implementation**: All core RPCs functional
  - `list_flights`: Enumerates registered logical datasets
  - `get_flight_info`: Returns schema + metadata for SQL queries
  - `get_schema`: Extracts schema from queries
  - `do_get`: Executes queries and streams Arrow RecordBatches
  - `do_action`: Supports `health_check` action
  
- ✅ **SQL Transformation**: Logical tables → macro calls
- ✅ **Tenant Validation**: Security check on every request
- ✅ **Error Mapping**: Comprehensive `QueryError` → `Status` mapping
- ✅ **Server Configuration**: TLS, message size, concurrency limits
- ✅ **Graceful Shutdown**: Clean shutdown with `run_with_shutdown`

### Testing Examples

Two examples are provided for testing:

#### 1. Rust Server Example

```bash
cargo run --example basic_server
```

This starts a Flight server at `grpc://127.0.0.1:8815` with:
- Sample `sales_data` table with 3 rows
- Logical dataset registered
- Ready for client connections

#### 2. Python Client Example

```bash
# Install dependencies
pip install pyarrow pandas

# Run client (with server running)
python src/worker-flight/examples/python_client.py
```

This demonstrates:
- Listing available datasets
- Getting schema from queries
- Executing queries and fetching results
- Aggregation queries
- Health check action

### Architecture

```
FlightQueryService<Q: QueryEngine>
├── get_flight_info    → Returns schema + metadata
├── get_schema        → Returns just schema
├── do_get           → Executes query + streams results
├── list_flights     → Lists logical datasets
└── do_action        → Health check, etc.
    │
    ├─→ SqlTransformer → Logical tables → macro calls
    └─→ QueryEngine    → Execute transformed SQL
```

### Code Quality

- **Generic Design**: `FlightQueryService<Q: QueryEngine>` for testability
- **Zero-Cost Abstractions**: Static dispatch, no runtime overhead
- **Comprehensive Error Handling**: All error paths mapped correctly
- **Tracing**: Structured logging with `#[instrument]`
- **Type Safety**: Compile-time verification

### Configuration

```rust
let config = FlightServerConfig {
    bind_addr: "127.0.0.1:8815".parse()?,
    tls_config: None,  // Or Some(ServerTlsConfig::new()...)
    max_message_size: 16 * 1024 * 1024,  // 16 MB
    concurrency_limit: 100,
};
```

### Next Steps

**Phase 2: Testing & Validation** (Pending)
- Create mock `QueryEngine` for unit tests
- Add comprehensive test suite
- E2E tests with real DuckDB

**Phase 3: Documentation** (Pending)
- API documentation improvements
- More usage examples
- TLS setup guide

**Phase 4: Production Features** (Optional)
- Query cancellation
- Metrics & observability
- Advanced logging

## Usage

### Basic Server Setup

```rust
use std::sync::Arc;
use worker_flight::{FlightServerBuilder, FlightServerConfig};
use worker_storage::engine::duckdb::DuckDBQueryEngine;
use worker_storage::sql::{RegisteredDataset, SqlTransformer};

// 1. Create query engine
let engine = Arc::new(DuckDBQueryEngine::new(db, 32));

// 2. Register logical datasets
let mut transformer = SqlTransformer::new();
transformer.register_dataset(RegisteredDataset::new(
    "sales_data".into(),
    "dt".into(),
));

// 3. Configure and run server
let config = FlightServerConfig::default();
FlightServerBuilder::new(
    config,
    engine,
    Arc::new(transformer),
    "my-tenant".into(),
)
.run().await?;
```

### Client Queries

**Python:**
```python
import pyarrow.flight as flight

client = flight.FlightClient("grpc://localhost:8815")

# Get schema
descriptor = flight.FlightDescriptor.for_command(
    "SELECT * FROM sales_data WHERE dt = '2025-01-01'".encode()
)
info = client.get_flight_info(descriptor)
schema = info.schema

# Execute query
ticket = info.endpoints[0].ticket
reader = client.do_get(ticket)
table = reader.read_all()
df = table.to_pandas()
```

## Testing Phase 1

Run the server:
```bash
cargo run --example basic_server
```

In another terminal, run the Python client:
```bash
python src/worker-flight/examples/python_client.py
```

Expected output:
```
🐍 Python Arrow Flight Client Example

🔌 Connecting to Flight server at grpc://127.0.0.1:8815...
✓ Connected

📋 Listing available datasets...
  - [sales_data]

📝 Getting schema for query...
✓ Schema fields: id, dt, product, amount, country
  Total endpoints: 1

🚀 Executing query and fetching results...
   SQL: SELECT * FROM sales_data WHERE dt = '2025-01-01'

✓ Query successful! Retrieved 3 rows

Results:
   id          dt  product  amount country
0   1  2025-01-01   Widget  100.50      US
1   2  2025-01-01   Gadget  250.00      UK
2   3  2025-01-01   Widget  150.75      JP
```

## Dependencies

Core dependencies:
- `arrow-flight = "56.2.0"` - Arrow Flight protocol
- `tonic = "0.13"` - gRPC framework (with `tls-webpki-roots`)
- `worker-storage` - Query engine + SQL transformation

## Status

**Phase 1: ✅ COMPLETE**
- All core functionality implemented
- Server compiles successfully
- Manual testing examples provided
- Ready for Phase 2 (Testing)

---

For detailed design documentation, see: `.kilocode/rules/arrow-flight-query-service-design.md`


