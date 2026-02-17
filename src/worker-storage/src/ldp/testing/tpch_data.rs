//! TPC-H data generators for realistic benchmark testing.
//!
//! Provides generators for core TPC-H 2.18 tables with proper schemas
//! and realistic data distributions. Supports epoch-based time partitioning
//! for fact tables.

use arrow::array::{
    ArrayRef, Date32Array, Float64Array, Int32Array, Int64Array, StringArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use chrono::NaiveDate;
use rand::distributions::Alphanumeric;
use rand::seq::SliceRandom;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use std::sync::Arc;

use super::data_loader::EpochSpec;

/// TPC-H data generator with configurable scale factor.
///
/// Scale factor determines the approximate database size:
/// - SF 0.01 = 10MB
/// - SF 0.1 = 100MB
/// - SF 1.0 = 1GB
/// - SF 10.0 = 10GB
pub struct TpchDataGenerator {
    scale_factor: f64,
    rng: StdRng,
}

impl TpchDataGenerator {
    /// Create a new TPC-H data generator with the given scale factor.
    ///
    /// Use a fixed seed for reproducibility in tests.
    pub fn new(scale_factor: f64) -> Self {
        Self {
            scale_factor,
            rng: StdRng::seed_from_u64(42),
        }
    }

    /// Create generator with custom seed for repeatability.
    pub fn with_seed(scale_factor: f64, seed: u64) -> Self {
        Self {
            scale_factor,
            rng: StdRng::seed_from_u64(seed),
        }
    }

    /// Generate LINEITEM table data across multiple epochs.
    ///
    /// LINEITEM is the largest fact table in TPC-H, containing order line items.
    /// Each epoch contains data for a specific time range based on l_shipdate.
    ///
    /// # Schema (simplified from TPC-H 2.18):
    /// - l_orderkey: INT64 (foreign key to ORDERS)
    /// - l_partkey: INT32 (foreign key to PART)
    /// - l_suppkey: INT32 (foreign key to SUPPLIER)
    /// - l_linenumber: INT32
    /// - l_quantity: FLOAT64
    /// - l_extendedprice: FLOAT64
    /// - l_discount: FLOAT64
    /// - l_tax: FLOAT64
    /// - l_returnflag: STRING ('R', 'A', 'N')
    /// - l_linestatus: STRING ('O', 'F')
    /// - l_shipdate: DATE32
    /// - l_commitdate: DATE32
    /// - l_receiptdate: DATE32
    /// - l_shipinstruct: STRING
    /// - l_shipmode: STRING
    /// - l_comment: STRING
    pub fn generate_lineitem_epochs(&mut self, epochs: &[EpochSpec]) -> Vec<(EpochSpec, Vec<RecordBatch>)> {
        epochs
            .iter()
            .map(|epoch| {
                let batches = self.generate_lineitem_for_epoch(epoch);
                (epoch.clone(), batches)
            })
            .collect()
    }

    /// Generate LINEITEM data for a single epoch.
    fn generate_lineitem_for_epoch(&mut self, epoch: &EpochSpec) -> Vec<RecordBatch> {
        let row_count = epoch.row_count;
        if row_count == 0 {
            return vec![];
        }

        // Generate data in batches of 1024 rows
        const BATCH_SIZE: usize = 1024;
        let num_batches = row_count.div_ceil(BATCH_SIZE);

        let mut batches = Vec::with_capacity(num_batches);
        let mut remaining = row_count;

        for _ in 0..num_batches {
            let batch_rows = remaining.min(BATCH_SIZE);
            if batch_rows == 0 {
                break;
            }

            let batch = self.generate_lineitem_batch(batch_rows, &epoch.start_date, &epoch.end_date);
            batches.push(batch);
            remaining -= batch_rows;
        }

        batches
    }

    /// Generate a single batch of LINEITEM rows.
    fn generate_lineitem_batch(
        &mut self,
        row_count: usize,
        start_date: &NaiveDate,
        end_date: &NaiveDate,
    ) -> RecordBatch {
        let mut l_orderkey = Vec::with_capacity(row_count);
        let mut l_partkey = Vec::with_capacity(row_count);
        let mut l_suppkey = Vec::with_capacity(row_count);
        let mut l_linenumber = Vec::with_capacity(row_count);
        let mut l_quantity = Vec::with_capacity(row_count);
        let mut l_extendedprice = Vec::with_capacity(row_count);
        let mut l_discount = Vec::with_capacity(row_count);
        let mut l_tax = Vec::with_capacity(row_count);
        let mut l_returnflag = Vec::with_capacity(row_count);
        let mut l_linestatus = Vec::with_capacity(row_count);
        let mut l_shipdate = Vec::with_capacity(row_count);
        let mut l_commitdate = Vec::with_capacity(row_count);
        let mut l_receiptdate = Vec::with_capacity(row_count);
        let mut l_shipinstruct = Vec::with_capacity(row_count);
        let mut l_shipmode = Vec::with_capacity(row_count);
        let mut l_comment = Vec::with_capacity(row_count);

        let returnflags = ["R", "A", "N"];
        let linestatuses = ["O", "F"];
        let shipinstructs = ["DELIVER IN PERSON", "COLLECT COD", "NONE", "TAKE BACK RETURN"];
        let shipmodes = ["TRUCK", "MAIL", "REG AIR", "SHIP", "FOB", "AIR", "RAIL"];

        // Calculate date range in days
        let date_range_days = (*end_date - *start_date).num_days() as i32;

        for i in 0..row_count {
            // Generate realistic order key (based on scale factor)
            l_orderkey.push((self.rng.gen::<u32>() as i64 % (1500000.0 * self.scale_factor) as i64) + 1);

            // Part key (1 to 200,000 * SF)
            l_partkey.push((self.rng.gen::<u32>() % (200000.0 * self.scale_factor) as u32) as i32 + 1);

            // Supplier key (1 to 10,000 * SF)
            l_suppkey.push((self.rng.gen::<u32>() % (10000.0 * self.scale_factor) as u32) as i32 + 1);

            // Line number (1-7 per order)
            l_linenumber.push((i % 7) as i32 + 1);

            // Quantity (1-50)
            l_quantity.push(self.rng.gen_range(1.0..=50.0));

            // Extended price (quantity * part_price, price 900-100,000)
            let part_price = self.rng.gen_range(900.0..=100000.0);
            l_extendedprice.push(l_quantity[i] * part_price);

            // Discount (0.00-0.10)
            l_discount.push(self.rng.gen_range(0.0..=0.10));

            // Tax (0.00-0.08)
            l_tax.push(self.rng.gen_range(0.0..=0.08));

            // Return flag
            l_returnflag.push(returnflags.choose(&mut self.rng).unwrap().to_string());

            // Line status
            l_linestatus.push(linestatuses.choose(&mut self.rng).unwrap().to_string());

            // Ship date (within epoch range)
            let days_offset = if date_range_days > 0 {
                self.rng.gen_range(0..date_range_days)
            } else {
                0
            };
            let ship_date = *start_date + chrono::Duration::days(days_offset as i64);
            l_shipdate.push(date_to_arrow(&ship_date));

            // Commit date (7-30 days before ship date)
            let commit_offset = self.rng.gen_range(7..=30);
            let commit_date = ship_date - chrono::Duration::days(commit_offset);
            l_commitdate.push(date_to_arrow(&commit_date));

            // Receipt date (1-10 days after ship date)
            let receipt_offset = self.rng.gen_range(1..=10);
            let receipt_date = ship_date + chrono::Duration::days(receipt_offset);
            l_receiptdate.push(date_to_arrow(&receipt_date));

            // Ship instructions
            l_shipinstruct.push(shipinstructs.choose(&mut self.rng).unwrap().to_string());

            // Ship mode
            l_shipmode.push(shipmodes.choose(&mut self.rng).unwrap().to_string());

            // Comment (random text 10-43 chars)
            let comment_len = self.rng.gen_range(10..=43);
            l_comment.push(random_string(&mut self.rng, comment_len));
        }

        let schema = Arc::new(lineitem_schema());

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(l_orderkey)) as ArrayRef,
                Arc::new(Int32Array::from(l_partkey)),
                Arc::new(Int32Array::from(l_suppkey)),
                Arc::new(Int32Array::from(l_linenumber)),
                Arc::new(Float64Array::from(l_quantity)),
                Arc::new(Float64Array::from(l_extendedprice)),
                Arc::new(Float64Array::from(l_discount)),
                Arc::new(Float64Array::from(l_tax)),
                Arc::new(StringArray::from(l_returnflag)),
                Arc::new(StringArray::from(l_linestatus)),
                Arc::new(Date32Array::from(l_shipdate)),
                Arc::new(Date32Array::from(l_commitdate)),
                Arc::new(Date32Array::from(l_receiptdate)),
                Arc::new(StringArray::from(l_shipinstruct)),
                Arc::new(StringArray::from(l_shipmode)),
                Arc::new(StringArray::from(l_comment)),
            ],
        )
        .unwrap()
    }

    /// Generate PART table data.
    ///
    /// PART is a dimension table containing part information.
    /// Typically small enough to be replicated or broadcast.
    ///
    /// # Schema (simplified):
    /// - p_partkey: INT32 (primary key)
    /// - p_name: STRING
    /// - p_mfgr: STRING (manufacturer)
    /// - p_brand: STRING
    /// - p_type: STRING
    /// - p_size: INT32
    /// - p_container: STRING
    /// - p_retailprice: FLOAT64
    /// - p_comment: STRING
    pub fn generate_part(&mut self, row_count: usize) -> Vec<RecordBatch> {
        if row_count == 0 {
            return vec![];
        }

        const BATCH_SIZE: usize = 1024;
        let num_batches = row_count.div_ceil(BATCH_SIZE);

        let mut batches = Vec::with_capacity(num_batches);
        let mut remaining = row_count;
        let mut key_offset = 0;

        for _ in 0..num_batches {
            let batch_rows = remaining.min(BATCH_SIZE);
            if batch_rows == 0 {
                break;
            }

            let batch = self.generate_part_batch(batch_rows, key_offset);
            batches.push(batch);
            remaining -= batch_rows;
            key_offset += batch_rows;
        }

        batches
    }

    fn generate_part_batch(&mut self, row_count: usize, key_offset: usize) -> RecordBatch {
        let mut p_partkey = Vec::with_capacity(row_count);
        let mut p_name = Vec::with_capacity(row_count);
        let mut p_mfgr = Vec::with_capacity(row_count);
        let mut p_brand = Vec::with_capacity(row_count);
        let mut p_type = Vec::with_capacity(row_count);
        let mut p_size = Vec::with_capacity(row_count);
        let mut p_container = Vec::with_capacity(row_count);
        let mut p_retailprice = Vec::with_capacity(row_count);
        let mut p_comment = Vec::with_capacity(row_count);

        let mfgrs = ["Manufacturer#1", "Manufacturer#2", "Manufacturer#3", "Manufacturer#4", "Manufacturer#5"];
        let brands = ["Brand#11", "Brand#12", "Brand#13", "Brand#21", "Brand#22", "Brand#23"];
        let types = ["STANDARD POLISHED TIN", "SMALL PLATED BRASS", "MEDIUM BURNISHED COPPER"];
        let containers = ["SM CASE", "SM BOX", "SM PACK", "LG CASE", "LG BOX", "LG PACK"];

        for i in 0..row_count {
            p_partkey.push((key_offset + i) as i32 + 1);
            p_name.push(format!("Part {}", key_offset + i + 1));
            p_mfgr.push(mfgrs.choose(&mut self.rng).unwrap().to_string());
            p_brand.push(brands.choose(&mut self.rng).unwrap().to_string());
            p_type.push(types.choose(&mut self.rng).unwrap().to_string());
            p_size.push(self.rng.gen_range(1..=50));
            p_container.push(containers.choose(&mut self.rng).unwrap().to_string());
            p_retailprice.push(self.rng.gen_range(900.0..=2000.0));
            let comment_len = self.rng.gen_range(10..=23);
            p_comment.push(random_string(&mut self.rng, comment_len));
        }

        let schema = Arc::new(part_schema());

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(p_partkey)) as ArrayRef,
                Arc::new(StringArray::from(p_name)),
                Arc::new(StringArray::from(p_mfgr)),
                Arc::new(StringArray::from(p_brand)),
                Arc::new(StringArray::from(p_type)),
                Arc::new(Int32Array::from(p_size)),
                Arc::new(StringArray::from(p_container)),
                Arc::new(Float64Array::from(p_retailprice)),
                Arc::new(StringArray::from(p_comment)),
            ],
        )
        .unwrap()
    }

    /// Generate SUPPLIER table data.
    ///
    /// SUPPLIER is a dimension table containing supplier information.
    ///
    /// # Schema (simplified):
    /// - s_suppkey: INT32 (primary key)
    /// - s_name: STRING
    /// - s_address: STRING
    /// - s_nationkey: INT32
    /// - s_phone: STRING
    /// - s_acctbal: FLOAT64
    /// - s_comment: STRING
    pub fn generate_supplier(&mut self, row_count: usize) -> Vec<RecordBatch> {
        if row_count == 0 {
            return vec![];
        }

        const BATCH_SIZE: usize = 1024;
        let num_batches = row_count.div_ceil(BATCH_SIZE);

        let mut batches = Vec::with_capacity(num_batches);
        let mut remaining = row_count;
        let mut key_offset = 0;

        for _ in 0..num_batches {
            let batch_rows = remaining.min(BATCH_SIZE);
            if batch_rows == 0 {
                break;
            }

            let batch = self.generate_supplier_batch(batch_rows, key_offset);
            batches.push(batch);
            remaining -= batch_rows;
            key_offset += batch_rows;
        }

        batches
    }

    fn generate_supplier_batch(&mut self, row_count: usize, key_offset: usize) -> RecordBatch {
        let mut s_suppkey = Vec::with_capacity(row_count);
        let mut s_name = Vec::with_capacity(row_count);
        let mut s_address = Vec::with_capacity(row_count);
        let mut s_nationkey = Vec::with_capacity(row_count);
        let mut s_phone = Vec::with_capacity(row_count);
        let mut s_acctbal = Vec::with_capacity(row_count);
        let mut s_comment = Vec::with_capacity(row_count);

        for i in 0..row_count {
            s_suppkey.push((key_offset + i) as i32 + 1);
            s_name.push(format!("Supplier#{:09}", key_offset + i + 1));
            let address_len = self.rng.gen_range(15..=40);
            s_address.push(random_string(&mut self.rng, address_len));
            s_nationkey.push(self.rng.gen_range(0..25)); // 25 nations in TPC-H
            s_phone.push(format!(
                "{:02}-{:03}-{:03}-{:04}",
                self.rng.gen_range(10..=34),
                self.rng.gen_range(100..=999),
                self.rng.gen_range(100..=999),
                self.rng.gen_range(1000..=9999)
            ));
            s_acctbal.push(self.rng.gen_range(-999.99..=9999.99));
            let comment_len = self.rng.gen_range(10..=25);
            s_comment.push(random_string(&mut self.rng, comment_len));
        }

        let schema = Arc::new(supplier_schema());

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(s_suppkey)) as ArrayRef,
                Arc::new(StringArray::from(s_name)),
                Arc::new(StringArray::from(s_address)),
                Arc::new(Int32Array::from(s_nationkey)),
                Arc::new(StringArray::from(s_phone)),
                Arc::new(Float64Array::from(s_acctbal)),
                Arc::new(StringArray::from(s_comment)),
            ],
        )
        .unwrap()
    }

    /// Generate CUSTOMER table data.
    ///
    /// CUSTOMER is a dimension table containing customer information.
    ///
    /// # Schema (simplified):
    /// - c_custkey: INT32 (primary key)
    /// - c_name: STRING
    /// - c_address: STRING
    /// - c_nationkey: INT32
    /// - c_phone: STRING
    /// - c_acctbal: FLOAT64
    /// - c_mktsegment: STRING
    /// - c_comment: STRING
    pub fn generate_customer(&mut self, row_count: usize) -> Vec<RecordBatch> {
        if row_count == 0 {
            return vec![];
        }

        const BATCH_SIZE: usize = 1024;
        let num_batches = row_count.div_ceil(BATCH_SIZE);

        let mut batches = Vec::with_capacity(num_batches);
        let mut remaining = row_count;
        let mut key_offset = 0;

        for _ in 0..num_batches {
            let batch_rows = remaining.min(BATCH_SIZE);
            if batch_rows == 0 {
                break;
            }

            let batch = self.generate_customer_batch(batch_rows, key_offset);
            batches.push(batch);
            remaining -= batch_rows;
            key_offset += batch_rows;
        }

        batches
    }

    fn generate_customer_batch(&mut self, row_count: usize, key_offset: usize) -> RecordBatch {
        let mut c_custkey = Vec::with_capacity(row_count);
        let mut c_name = Vec::with_capacity(row_count);
        let mut c_address = Vec::with_capacity(row_count);
        let mut c_nationkey = Vec::with_capacity(row_count);
        let mut c_phone = Vec::with_capacity(row_count);
        let mut c_acctbal = Vec::with_capacity(row_count);
        let mut c_mktsegment = Vec::with_capacity(row_count);
        let mut c_comment = Vec::with_capacity(row_count);

        let segments = ["AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"];

        for i in 0..row_count {
            c_custkey.push((key_offset + i) as i32 + 1);
            c_name.push(format!("Customer#{:09}", key_offset + i + 1));
            let address_len = self.rng.gen_range(15..=40);
            c_address.push(random_string(&mut self.rng, address_len));
            c_nationkey.push(self.rng.gen_range(0..25));
            c_phone.push(format!(
                "{:02}-{:03}-{:03}-{:04}",
                self.rng.gen_range(10..=34),
                self.rng.gen_range(100..=999),
                self.rng.gen_range(100..=999),
                self.rng.gen_range(1000..=9999)
            ));
            c_acctbal.push(self.rng.gen_range(-999.99..=9999.99));
            c_mktsegment.push(segments.choose(&mut self.rng).unwrap().to_string());
            let comment_len = self.rng.gen_range(10..=29);
            c_comment.push(random_string(&mut self.rng, comment_len));
        }

        let schema = Arc::new(customer_schema());

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(c_custkey)) as ArrayRef,
                Arc::new(StringArray::from(c_name)),
                Arc::new(StringArray::from(c_address)),
                Arc::new(Int32Array::from(c_nationkey)),
                Arc::new(StringArray::from(c_phone)),
                Arc::new(Float64Array::from(c_acctbal)),
                Arc::new(StringArray::from(c_mktsegment)),
                Arc::new(StringArray::from(c_comment)),
            ],
        )
        .unwrap()
    }

    /// Generate ORDERS table data across multiple epochs.
    ///
    /// ORDERS is a fact table containing order information.
    /// Each epoch contains data for a specific time range based on o_orderdate.
    ///
    /// # Schema (simplified):
    /// - o_orderkey: INT64 (primary key)
    /// - o_custkey: INT32 (foreign key to CUSTOMER)
    /// - o_orderstatus: STRING ('O', 'F', 'P')
    /// - o_totalprice: FLOAT64
    /// - o_orderdate: DATE32
    /// - o_orderpriority: STRING
    /// - o_clerk: STRING
    /// - o_shippriority: INT32
    /// - o_comment: STRING
    pub fn generate_orders_epochs(&mut self, epochs: &[EpochSpec]) -> Vec<(EpochSpec, Vec<RecordBatch>)> {
        epochs
            .iter()
            .map(|epoch| {
                let batches = self.generate_orders_for_epoch(epoch);
                (epoch.clone(), batches)
            })
            .collect()
    }

    fn generate_orders_for_epoch(&mut self, epoch: &EpochSpec) -> Vec<RecordBatch> {
        let row_count = epoch.row_count;
        if row_count == 0 {
            return vec![];
        }

        const BATCH_SIZE: usize = 1024;
        let num_batches = row_count.div_ceil(BATCH_SIZE);

        let mut batches = Vec::with_capacity(num_batches);
        let mut remaining = row_count;

        for _ in 0..num_batches {
            let batch_rows = remaining.min(BATCH_SIZE);
            if batch_rows == 0 {
                break;
            }

            let batch = self.generate_orders_batch(batch_rows, &epoch.start_date, &epoch.end_date);
            batches.push(batch);
            remaining -= batch_rows;
        }

        batches
    }

    fn generate_orders_batch(
        &mut self,
        row_count: usize,
        start_date: &NaiveDate,
        end_date: &NaiveDate,
    ) -> RecordBatch {
        let mut o_orderkey = Vec::with_capacity(row_count);
        let mut o_custkey = Vec::with_capacity(row_count);
        let mut o_orderstatus = Vec::with_capacity(row_count);
        let mut o_totalprice = Vec::with_capacity(row_count);
        let mut o_orderdate = Vec::with_capacity(row_count);
        let mut o_orderpriority = Vec::with_capacity(row_count);
        let mut o_clerk = Vec::with_capacity(row_count);
        let mut o_shippriority = Vec::with_capacity(row_count);
        let mut o_comment = Vec::with_capacity(row_count);

        let statuses = ["O", "F", "P"];
        let priorities = ["1-URGENT", "2-HIGH", "3-MEDIUM", "4-NOT SPECIFIED", "5-LOW"];

        let date_range_days = (*end_date - *start_date).num_days() as i32;

        for i in 0..row_count {
            o_orderkey.push((self.rng.gen::<u32>() as i64 % (1500000.0 * self.scale_factor) as i64) + 1);
            o_custkey.push((self.rng.gen::<u32>() % (150000.0 * self.scale_factor) as u32) as i32 + 1);
            o_orderstatus.push(statuses.choose(&mut self.rng).unwrap().to_string());
            o_totalprice.push(self.rng.gen_range(1000.0..=500000.0));

            let days_offset = if date_range_days > 0 {
                self.rng.gen_range(0..date_range_days)
            } else {
                0
            };
            let order_date = *start_date + chrono::Duration::days(days_offset as i64);
            o_orderdate.push(date_to_arrow(&order_date));

            o_orderpriority.push(priorities.choose(&mut self.rng).unwrap().to_string());
            o_clerk.push(format!("Clerk#{:09}", (i % 1000) + 1));
            o_shippriority.push(self.rng.gen_range(0..=1));
            let comment_len = self.rng.gen_range(10..=39);
            o_comment.push(random_string(&mut self.rng, comment_len));
        }

        let schema = Arc::new(orders_schema());

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(o_orderkey)) as ArrayRef,
                Arc::new(Int32Array::from(o_custkey)),
                Arc::new(StringArray::from(o_orderstatus)),
                Arc::new(Float64Array::from(o_totalprice)),
                Arc::new(Date32Array::from(o_orderdate)),
                Arc::new(StringArray::from(o_orderpriority)),
                Arc::new(StringArray::from(o_clerk)),
                Arc::new(Int32Array::from(o_shippriority)),
                Arc::new(StringArray::from(o_comment)),
            ],
        )
        .unwrap()
    }
}

/// Convert NaiveDate to Arrow Date32 (days since Unix epoch)
fn date_to_arrow(date: &NaiveDate) -> i32 {
    let epoch = NaiveDate::from_ymd_opt(1970, 1, 1).unwrap();
    (*date - epoch).num_days() as i32
}

/// Generate random alphanumeric string
fn random_string<R: Rng>(rng: &mut R, len: usize) -> String {
    rng.sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}

/// TPC-H LINEITEM schema
fn lineitem_schema() -> Schema {
    Schema::new(vec![
        Field::new("l_orderkey", DataType::Int64, false),
        Field::new("l_partkey", DataType::Int32, false),
        Field::new("l_suppkey", DataType::Int32, false),
        Field::new("l_linenumber", DataType::Int32, false),
        Field::new("l_quantity", DataType::Float64, false),
        Field::new("l_extendedprice", DataType::Float64, false),
        Field::new("l_discount", DataType::Float64, false),
        Field::new("l_tax", DataType::Float64, false),
        Field::new("l_returnflag", DataType::Utf8, false),
        Field::new("l_linestatus", DataType::Utf8, false),
        Field::new("l_shipdate", DataType::Date32, false),
        Field::new("l_commitdate", DataType::Date32, false),
        Field::new("l_receiptdate", DataType::Date32, false),
        Field::new("l_shipinstruct", DataType::Utf8, false),Field::new("l_shipmode", DataType::Utf8, false),
        Field::new("l_comment", DataType::Utf8, false),
    ])
}

/// TPC-H PART schema
fn part_schema() -> Schema {
    Schema::new(vec![
        Field::new("p_partkey", DataType::Int32, false),
        Field::new("p_name", DataType::Utf8, false),
        Field::new("p_mfgr", DataType::Utf8, false),
        Field::new("p_brand", DataType::Utf8, false),
        Field::new("p_type", DataType::Utf8, false),
        Field::new("p_size", DataType::Int32, false),
        Field::new("p_container", DataType::Utf8, false),
        Field::new("p_retailprice", DataType::Float64, false),
        Field::new("p_comment", DataType::Utf8, false),
    ])
}

/// TPC-H SUPPLIER schema
fn supplier_schema() -> Schema {
    Schema::new(vec![
        Field::new("s_suppkey", DataType::Int32, false),
        Field::new("s_name", DataType::Utf8, false),
        Field::new("s_address", DataType::Utf8, false),
        Field::new("s_nationkey", DataType::Int32, false),
        Field::new("s_phone", DataType::Utf8, false),
        Field::new("s_acctbal", DataType::Float64, false),
        Field::new("s_comment", DataType::Utf8, false),
    ])
}

/// TPC-H CUSTOMER schema
fn customer_schema() -> Schema {
    Schema::new(vec![
        Field::new("c_custkey", DataType::Int32, false),
        Field::new("c_name", DataType::Utf8, false),
        Field::new("c_address", DataType::Utf8, false),
        Field::new("c_nationkey", DataType::Int32, false),
        Field::new("c_phone", DataType::Utf8, false),
        Field::new("c_acctbal", DataType::Float64, false),
        Field::new("c_mktsegment", DataType::Utf8, false),
        Field::new("c_comment", DataType::Utf8, false),
    ])
}

/// TPC-H ORDERS schema
fn orders_schema() -> Schema {
    Schema::new(vec![
        Field::new("o_orderkey", DataType::Int64, false),
        Field::new("o_custkey", DataType::Int32, false),
        Field::new("o_orderstatus", DataType::Utf8, false),
        Field::new("o_totalprice", DataType::Float64, false),
        Field::new("o_orderdate", DataType::Date32, false),
        Field::new("o_orderpriority", DataType::Utf8, false),
        Field::new("o_clerk", DataType::Utf8, false),
        Field::new("o_shippriority", DataType::Int32, false),
        Field::new("o_comment", DataType::Utf8, false),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;

    #[test]
    fn test_tpch_generator_scale_factor() {
        let gen = TpchDataGenerator::new(0.1);
        assert_eq!(gen.scale_factor, 0.1);
    }

    #[test]
    fn test_generate_lineitem_schema() {
        let mut gen = TpchDataGenerator::new(0.01);
        let epoch = EpochSpec {
            epoch_id: "e1".to_string(),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
            worker_id: "w1".to_string(),
            row_count: 100,
        };

        let batches = gen.generate_lineitem_for_epoch(&epoch);
        assert!(!batches.is_empty());

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 16);
        assert_eq!(batch.schema().field(0).name(), "l_orderkey");
        assert_eq!(batch.schema().field(10).name(), "l_shipdate");
    }

    #[test]
    fn test_generate_part_schema() {
        let mut gen = TpchDataGenerator::new(0.01);
        let batches = gen.generate_part(50);
        assert!(!batches.is_empty());

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 9);
        assert_eq!(batch.schema().field(0).name(), "p_partkey");
    }

    #[test]
    fn test_generate_supplier_schema() {
        let mut gen = TpchDataGenerator::new(0.01);
        let batches = gen.generate_supplier(50);
        assert!(!batches.is_empty());

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 7);
        assert_eq!(batch.schema().field(0).name(), "s_suppkey");
    }

    #[test]
    fn test_generate_customer_schema() {
        let mut gen = TpchDataGenerator::new(0.01);
        let batches = gen.generate_customer(100);
        assert!(!batches.is_empty());

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 8);
        assert_eq!(batch.schema().field(0).name(), "c_custkey");
    }

    #[test]
    fn test_generate_orders_schema() {
        let mut gen = TpchDataGenerator::new(0.01);
        let epoch = EpochSpec {
            epoch_id: "e1".to_string(),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
            worker_id: "w1".to_string(),
            row_count: 100,
        };

        let batches = gen.generate_orders_for_epoch(&epoch);
        assert!(!batches.is_empty());

        let batch = &batches[0];
        assert_eq!(batch.num_columns(), 9);
        assert_eq!(batch.schema().field(0).name(), "o_orderkey");
        assert_eq!(batch.schema().field(4).name(), "o_orderdate");
    }

    #[test]
    fn test_lineitem_date_range() {
        let mut gen = TpchDataGenerator::new(0.01);
        let epoch = EpochSpec {
            epoch_id: "e1".to_string(),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 10).unwrap(),
            worker_id: "w1".to_string(),
            row_count: 50,
        };

        let batches = gen.generate_lineitem_for_epoch(&epoch);
        let batch = &batches[0];

        // Verify all ship dates are within epoch range
        let ship_dates = batch
            .column(10)
            .as_any()
            .downcast_ref::<Date32Array>()
            .unwrap();

        let epoch_start_days = date_to_arrow(&epoch.start_date);
        let epoch_end_days = date_to_arrow(&epoch.end_date);

        for i in 0..ship_dates.len() {
            let ship_date = ship_dates.value(i);
            assert!(
                ship_date >= epoch_start_days && ship_date < epoch_end_days,
                "Ship date {} not in range [{}, {})",
                ship_date,
                epoch_start_days,
                epoch_end_days
            );
        }
    }

    #[test]
    fn test_epoch_row_count_exact() {
        let mut gen = TpchDataGenerator::new(0.01);
        let epoch = EpochSpec {
            epoch_id: "e1".to_string(),
            start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
            worker_id: "w1".to_string(),
            row_count: 2500, // Non-batch-aligned
        };

        let batches = gen.generate_lineitem_for_epoch(&epoch);
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2500);
    }

    #[test]
    fn test_reproducible_with_seed() {
        let mut gen1 = TpchDataGenerator::with_seed(0.01, 12345);
        let mut gen2 = TpchDataGenerator::with_seed(0.01, 12345);

        let batches1 = gen1.generate_part(10);
        let batches2 = gen2.generate_part(10);

        // Same seed should produce identical data
        assert_eq!(batches1.len(), batches2.len());

        for (b1, b2) in batches1.iter().zip(batches2.iter()) {
            assert_eq!(b1.num_rows(), b2.num_rows());
            assert_eq!(b1.num_columns(), b2.num_columns());
        }
    }
}
