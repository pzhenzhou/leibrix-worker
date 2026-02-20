#![allow(dead_code)]

//! `DataSeeder` — deterministic epoch-table + macro + dataset seeding.
//!
//! All data is defined as fixed literal SQL VALUES — no RNG, fully deterministic.
//! Each `seed_*` method is safe to call on a fresh database (one call per harness).
//!
//! # Fixture overview
//!
//! | Fixture | Tables created | Macro | Dataset registered |
//! |---------|---------------|-------|-------------------|
//! | `seed_standard` | `orders__epoch_20250101`, `customers` | `scan_orders` | `orders` |
//! | `seed_orders_single_epoch` | `orders__epoch_20250101` | `scan_orders` | `orders` |
//! | `seed_orders_multi_epoch` | `orders__epoch_20250101/20250201` | `scan_orders` | `orders` |
//! | `seed_customers` | `customers` | — | — |
//! | `seed_regions` | `regions` | — | — |

use std::sync::Arc;

use tokio::sync::RwLock;

use worker_storage::engine::duckdb::SharedDatabase;
use worker_storage::ldp::testing::macro_helpers::create_epoch_table_macro;
use worker_storage::sql::{RegisteredDataset, SqlTransformer};

/// Seeds deterministic test data into a `SharedDatabase`.
///
/// Construct from a `FlightTestHarness` using its `shared_db` and
/// `sql_transformer` fields, then call the appropriate `seed_*` methods
/// before running test assertions.
pub struct DataSeeder {
    shared_db: Arc<SharedDatabase>,
    sql_transformer: Arc<RwLock<SqlTransformer>>,
}

impl DataSeeder {
    pub fn new(
        shared_db: Arc<SharedDatabase>,
        sql_transformer: Arc<RwLock<SqlTransformer>>,
    ) -> Self {
        Self {
            shared_db,
            sql_transformer,
        }
    }

    /// Seed the standard fixture: orders (single Jan-2025 epoch) + customers.
    ///
    /// This is the fixture used by most tests. Registers the `orders` dataset.
    pub async fn seed_standard(&self) {
        self.seed_orders_single_epoch().await;
        self.seed_customers();
    }

    /// Create `orders__epoch_20250101` (20 fixed rows, all in Jan 2025) and
    /// register the `orders` dataset in the SQL transformer.
    ///
    /// Queries must include `WHERE order_date >= '2025-01-01'` (or similar
    /// date predicate) to pass the transformer's admission control.
    pub async fn seed_orders_single_epoch(&self) {
        let conn = self.shared_db.get().expect("shared_db.get");

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS orders__epoch_20250101 (
               order_id    INTEGER NOT NULL,
               customer_id INTEGER NOT NULL,
               amount      DOUBLE  NOT NULL,
               order_date  DATE    NOT NULL
             );",
        )
        .expect("create orders__epoch_20250101");

        conn.execute_batch(
            "INSERT INTO orders__epoch_20250101
             SELECT order_id, customer_id, amount, CAST(order_date AS DATE)
             FROM (VALUES
               (1,  101, 150.00, '2025-01-05'),
               (2,  102, 230.50, '2025-01-08'),
               (3,  103,  89.99, '2025-01-12'),
               (4,  104, 410.00, '2025-01-15'),
               (5,  105, 175.25, '2025-01-18'),
               (6,  101, 320.00, '2025-01-20'),
               (7,  102,  55.00, '2025-01-22'),
               (8,  103, 640.75, '2025-01-25'),
               (9,  104, 210.00, '2025-01-27'),
               (10, 105, 380.00, '2025-01-30'),
               (11, 101, 120.00, '2025-01-03'),
               (12, 102, 495.50, '2025-01-06'),
               (13, 103,  67.00, '2025-01-09'),
               (14, 104, 310.00, '2025-01-11'),
               (15, 105, 220.75, '2025-01-14'),
               (16, 101, 580.00, '2025-01-16'),
               (17, 102, 145.00, '2025-01-19'),
               (18, 103, 730.25, '2025-01-21'),
               (19, 104, 290.00, '2025-01-23'),
               (20, 105, 460.00, '2025-01-28')
             ) t(order_id, customer_id, amount, order_date);",
        )
        .expect("insert orders__epoch_20250101");

        create_epoch_table_macro(
            &conn,
            "orders",
            "order_date",
            &["orders__epoch_20250101".to_string()],
        )
        .expect("create scan_orders macro (single epoch)");

        // Schema-discovery view: enables `DESCRIBE "orders"` for get_flight_info / get_schema.
        conn.execute_batch(
            r#"CREATE OR REPLACE VIEW "orders" AS SELECT * FROM "orders__epoch_20250101""#,
        )
        .expect("create orders schema view (single epoch)");

        self.sql_transformer.write().await.register_dataset(
            RegisteredDataset::new("orders".to_string(), "order_date".to_string()),
        );
    }

    /// Create two epoch tables (`20250101` + `20250201`, 10 rows each) and
    /// register the `orders` dataset. Use for cross-epoch range query tests.
    pub async fn seed_orders_multi_epoch(&self) {
        let conn = self.shared_db.get().expect("shared_db.get");

        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS orders__epoch_20250101 (
               order_id INTEGER NOT NULL, customer_id INTEGER NOT NULL,
               amount DOUBLE NOT NULL, order_date DATE NOT NULL
             );
             INSERT INTO orders__epoch_20250101
             SELECT order_id, customer_id, amount, CAST(order_date AS DATE)
             FROM (VALUES
               (1,  101, 150.00, '2025-01-05'),
               (2,  102, 230.50, '2025-01-08'),
               (3,  103,  89.99, '2025-01-12'),
               (4,  104, 410.00, '2025-01-15'),
               (5,  105, 175.25, '2025-01-18'),
               (6,  101, 320.00, '2025-01-20'),
               (7,  102,  55.00, '2025-01-22'),
               (8,  103, 640.75, '2025-01-25'),
               (9,  104, 210.00, '2025-01-27'),
               (10, 105, 380.00, '2025-01-30')
             ) t(order_id, customer_id, amount, order_date);

             CREATE TABLE IF NOT EXISTS orders__epoch_20250201 (
               order_id INTEGER NOT NULL, customer_id INTEGER NOT NULL,
               amount DOUBLE NOT NULL, order_date DATE NOT NULL
             );
             INSERT INTO orders__epoch_20250201
             SELECT order_id, customer_id, amount, CAST(order_date AS DATE)
             FROM (VALUES
               (11, 101, 100.00, '2025-02-03'),
               (12, 102, 290.00, '2025-02-05'),
               (13, 103, 450.00, '2025-02-07'),
               (14, 104, 120.50, '2025-02-10'),
               (15, 105, 330.00, '2025-02-12'),
               (16, 101, 210.00, '2025-02-15'),
               (17, 102,  75.00, '2025-02-18'),
               (18, 103, 500.00, '2025-02-20'),
               (19, 104, 180.00, '2025-02-22'),
               (20, 105, 420.00, '2025-02-25')
             ) t(order_id, customer_id, amount, order_date);",
        )
        .expect("seed multi-epoch orders DDL");

        create_epoch_table_macro(
            &conn,
            "orders",
            "order_date",
            &[
                "orders__epoch_20250101".to_string(),
                "orders__epoch_20250201".to_string(),
            ],
        )
        .expect("create scan_orders macro (multi epoch)");

        // Schema-discovery view: enables `DESCRIBE "orders"` for get_flight_info / get_schema.
        conn.execute_batch(
            r#"CREATE OR REPLACE VIEW "orders" AS
               SELECT * FROM "orders__epoch_20250101"
               UNION ALL
               SELECT * FROM "orders__epoch_20250201""#,
        )
        .expect("create orders schema view (multi epoch)");

        self.sql_transformer.write().await.register_dataset(
            RegisteredDataset::new("orders".to_string(), "order_date".to_string()),
        );
    }

    /// Create `customers` dimension table (5 rows, no epoch / no macro).
    pub fn seed_customers(&self) {
        let conn = self.shared_db.get().expect("shared_db.get");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS customers (
               customer_id   INTEGER NOT NULL,
               customer_name VARCHAR NOT NULL,
               region        VARCHAR NOT NULL
             );
             INSERT INTO customers VALUES
               (101, 'Alice',   'US'),
               (102, 'Bob',     'EU'),
               (103, 'Charlie', 'US'),
               (104, 'Diana',   'APAC'),
               (105, 'Eve',     'EU');",
        )
        .expect("seed customers");
    }

    /// Create `regions` lookup table (5 rows).
    pub fn seed_regions(&self) {
        let conn = self.shared_db.get().expect("shared_db.get");
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS regions (
               region_name VARCHAR NOT NULL,
               continent   VARCHAR NOT NULL
             );
             INSERT INTO regions VALUES
               ('US',   'Americas'),
               ('EU',   'Europe'),
               ('APAC', 'Asia-Pacific'),
               ('LATAM','Americas'),
               ('MEA',  'Middle East & Africa');",
        )
        .expect("seed regions");
    }
}
