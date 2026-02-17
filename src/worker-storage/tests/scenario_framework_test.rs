//! Basic integration test for the test scenario framework.
//!
//! This test verifies that the scenario builder API works correctly
//! and that scenarios can be constructed with tables, queries, and expectations.

use worker_storage::ldp::testing::scenarios::*;

#[test]
fn test_scenario_builder_basic() {
    let scenario = TestScenario::builder()
        .name("Basic test scenario")
        .table(
            TableSetup::new("orders")
                .distribution(DistributionSetup::EpochPartitioned)
                .rows(1000),
        )
        .query("SELECT * FROM orders")
        .expect_result(ResultExpectation::MinRowCount(900))
        .build();

    assert_eq!(scenario.name, "Basic test scenario");
    assert_eq!(scenario.tables.len(), 1);
    assert_eq!(scenario.result_expectations.len(), 1);
}

#[test]
fn test_scenario_builder_multi_table() {
    let scenario = TestScenario::builder()
        .name("Multi-table join scenario")
        .table(
            TableSetup::new("orders")
                .distribution(DistributionSetup::EpochPartitioned)
                .rows(100_000),
        )
        .table(
            TableSetup::new("products")
                .distribution(DistributionSetup::Replicated)
                .rows(10_000),
        )
        .query("SELECT * FROM orders JOIN products ON o_product_id = p_product_id")
        .expect_plan(PlanExpectation::MinStageCount(2))
        .expect_result(ResultExpectation::RowCountRange(90_000, 110_000))
        .build();

    assert_eq!(scenario.tables.len(), 2);
    assert_eq!(scenario.plan_expectations.len(), 1);
    assert_eq!(scenario.result_expectations.len(), 1);

    // Verify table setups
    assert_eq!(scenario.tables[0].name, "orders");
    assert_eq!(scenario.tables[0].rows, 100_000);
    assert_eq!(
        scenario.tables[0].distribution,
        DistributionSetup::EpochPartitioned
    );

    assert_eq!(scenario.tables[1].name, "products");
    assert_eq!(scenario.tables[1].rows, 10_000);
    assert_eq!(scenario.tables[1].distribution, DistributionSetup::Replicated);
}

#[test]
fn test_table_setup_description() {
    let table = TableSetup::new("test_table")
        .distribution(DistributionSetup::SingleWorker)
        .rows(5000);

    let desc = table.description();
    assert!(desc.contains("test_table"));
    assert!(desc.contains("5000"));
}

#[test]
fn test_plan_expectation_descriptions() {
    let expectations = vec![
        PlanExpectation::MinStageCount(3),
        PlanExpectation::ExactStageCount(5),
        PlanExpectation::MaxExchangeCount(10),
        PlanExpectation::NoExchangeFor("products".to_string()),
    ];

    for expectation in expectations {
        let desc = expectation.description();
        assert!(!desc.is_empty());
    }
}

#[test]
fn test_result_expectation_row_count() {
    use arrow::array::{Int32Array, ArrayRef};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    // Create a test batch
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let batch = RecordBatch::try_new(
        schema,
        vec![Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5])) as ArrayRef],
    )
    .unwrap();

    let batches = vec![batch];
    let row_count = 5;

    // Test exact match
    let expectation = ResultExpectation::RowCount(5);
    assert!(expectation.verify(&batches, row_count).is_ok());

    // Test mismatch
    let expectation = ResultExpectation::RowCount(10);
    assert!(expectation.verify(&batches, row_count).is_err());

    // Test range
    let expectation = ResultExpectation::RowCountRange(3, 7);
    assert!(expectation.verify(&batches, row_count).is_ok());

    // Test out of range
    let expectation = ResultExpectation::RowCountRange(10, 20);
    assert!(expectation.verify(&batches, row_count).is_err());
}

#[test]
fn test_distribution_setups() {
    let setups = vec![
        DistributionSetup::SingleWorker,
        DistributionSetup::Replicated,
        DistributionSetup::EpochPartitioned,
        DistributionSetup::HashPartitioned,
    ];

    for setup in setups {
        let table = TableSetup::new("test").distribution(setup.clone());
        assert_eq!(table.distribution, setup);
    }
}
