//! Basic integration test for TPC-H data generators.
//!
//! This test verifies that the TPC-H data generators compile and produce
//! valid Arrow RecordBatches with the correct schemas.

use chrono::NaiveDate;
use worker_storage::ldp::testing::data_loader::EpochSpec;
use worker_storage::ldp::testing::tpch_data::TpchDataGenerator;

#[test]
fn test_tpch_generator_produces_valid_lineitem() {
    let mut gen = TpchDataGenerator::new(0.01);

    let epoch = EpochSpec {
        epoch_id: "e1".to_string(),
        start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
        worker_id: "w1".to_string(),
        row_count: 100,
    };

    let batches = gen.generate_lineitem_epochs(&[epoch]);

    assert_eq!(batches.len(), 1);
    assert!(!batches[0].1.is_empty());

    let batch = &batches[0].1[0];
    assert_eq!(batch.num_columns(), 16);
    assert!(batch.num_rows() > 0);
}

#[test]
fn test_tpch_generator_produces_valid_part() {
    let mut gen = TpchDataGenerator::new(0.01);
    let batches = gen.generate_part(50);

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 9);
    assert!(batch.num_rows() > 0);
}

#[test]
fn test_tpch_generator_produces_valid_supplier() {
    let mut gen = TpchDataGenerator::new(0.01);
    let batches = gen.generate_supplier(50);

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 7);
    assert!(batch.num_rows() > 0);
}

#[test]
fn test_tpch_generator_produces_valid_customer() {
    let mut gen = TpchDataGenerator::new(0.01);
    let batches = gen.generate_customer(100);

    assert!(!batches.is_empty());
    let batch = &batches[0];
    assert_eq!(batch.num_columns(), 8);
    assert!(batch.num_rows() > 0);
}

#[test]
fn test_tpch_generator_produces_valid_orders() {
    let mut gen = TpchDataGenerator::new(0.01);

    let epoch = EpochSpec {
        epoch_id: "e1".to_string(),
        start_date: NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
        end_date: NaiveDate::from_ymd_opt(2025, 1, 31).unwrap(),
        worker_id: "w1".to_string(),
        row_count: 100,
    };

    let batches = gen.generate_orders_epochs(&[epoch]);

    assert_eq!(batches.len(), 1);
    assert!(!batches[0].1.is_empty());

    let batch = &batches[0].1[0];
    assert_eq!(batch.num_columns(), 9);
    assert!(batch.num_rows() > 0);
}
