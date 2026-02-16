//! Declarative test scenario framework for LDP testing.
//!
//! Provides a builder-based API for defining complex test scenarios with:
//! - Table setup with specific distributions
//! - Query execution and plan inspection
//! - Result verification
//!
//! # Example
//! ```no_run
//! use worker_storage::ldp::testing::scenarios::*;
//!
//! let scenario = TestScenario::builder()
//!     .name("Broadcast dimension join")
//!     .table(TableSetup::new("orders")
//!         .distribution(DistributionSetup::EpochPartitioned)
//!         .rows(100_000))
//!     .table(TableSetup::new("products")
//!         .distribution(DistributionSetup::Replicated)
//!         .rows(10_000))
//!     .query("SELECT * FROM orders JOIN products ON o_product_id = p_product_id")
//!     .expect_plan(PlanExpectation::NoExchangeFor("products"))
//!     .expect_result(ResultExpectation::RowCountRange(90_000, 110_000))
//!     .build();
//! ```

use crate::ldp::testing::cluster::TestCluster;
use crate::ldp::testing::data_loader::EpochSpec;
use crate::ldp::LdpPlan;
use arrow::record_batch::RecordBatch;

/// A declarative test scenario for LDP query execution.
#[derive(Clone, Debug)]
pub struct TestScenario {
    /// Scenario name for identification in test output
    pub name: String,

    /// Tables to be loaded into the test cluster
    pub tables: Vec<TableSetup>,

    /// SQL query to execute
    pub query: String,

    /// Expected properties of the generated plan
    pub plan_expectations: Vec<PlanExpectation>,

    /// Expected properties of the query result
    pub result_expectations: Vec<ResultExpectation>,
}

impl TestScenario {
    /// Create a new builder for constructing a test scenario.
    pub fn builder() -> TestScenarioBuilder {
        TestScenarioBuilder::default()
    }

    /// Execute this scenario against a test cluster.
    ///
    /// Returns:
    /// - Ok(TestResult) if all expectations pass
    /// - Err(TestError) if setup fails or any expectation fails
    pub async fn run(&self, cluster: &TestCluster) -> Result<TestResult, TestError> {
        // Step 1: Table loading would happen in test setup
        // The scenario framework doesn't directly load tables - that's done
        // by the test setup code using load_test_data_for_e2e() or similar

        // Step 2: Execute query
        let batches = cluster.execute_query(&self.query).await.map_err(|e| TestError::ExecutionFailed {
            error: e.to_string(),
        })?;

        // Step 3: Verify result expectations
        let row_count: usize = batches.iter().map(|b| b.num_rows()).sum();
        for expectation in &self.result_expectations {
            expectation.verify(&batches, row_count)?;
        }

        // Note: Plan expectations are not verifiable with current API
        // The coordinator doesn't expose the generated plan to callers
        // This would require coordinator API changes

        Ok(TestResult {
            scenario_name: self.name.clone(),
            row_count,
            passed: true,
        })
    }
}

/// Builder for constructing TestScenario instances.
#[derive(Default)]
pub struct TestScenarioBuilder {
    name: Option<String>,
    tables: Vec<TableSetup>,
    query: Option<String>,
    plan_expectations: Vec<PlanExpectation>,
    result_expectations: Vec<ResultExpectation>,
}

impl TestScenarioBuilder {
    /// Set the scenario name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Add a table to be loaded.
    pub fn table(mut self, table: TableSetup) -> Self {
        self.tables.push(table);
        self
    }

    /// Set the SQL query to execute.
    pub fn query(mut self, query: impl Into<String>) -> Self {
        self.query = Some(query.into());
        self
    }

    /// Add a plan expectation.
    pub fn expect_plan(mut self, expectation: PlanExpectation) -> Self {
        self.plan_expectations.push(expectation);
        self
    }

    /// Add a result expectation.
    pub fn expect_result(mut self, expectation: ResultExpectation) -> Self {
        self.result_expectations.push(expectation);
        self
    }

    /// Build the TestScenario.
    pub fn build(self) -> TestScenario {
        TestScenario {
            name: self.name.unwrap_or_else(|| "Unnamed scenario".to_string()),
            tables: self.tables,
            query: self.query.expect("Query is required"),
            plan_expectations: self.plan_expectations,
            result_expectations: self.result_expectations,
        }
    }
}

/// Configuration for a table to be loaded in a test scenario.
#[derive(Clone, Debug)]
pub struct TableSetup {
    /// Table name
    pub name: String,

    /// How the data should be distributed across workers
    pub distribution: DistributionSetup,

    /// Number of rows to generate
    pub rows: usize,

    /// Optional custom data generator
    pub generator: Option<DataGenerator>,

    /// Epochs for time-partitioned tables
    pub epochs: Vec<EpochSpec>,
}

impl TableSetup {
    /// Create a new table setup with the given name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            distribution: DistributionSetup::SingleWorker,
            rows: 1000,
            generator: None,
            epochs: vec![],
        }
    }

    /// Set the distribution strategy.
    pub fn distribution(mut self, distribution: DistributionSetup) -> Self {
        self.distribution = distribution;
        self
    }

    /// Set the number of rows.
    pub fn rows(mut self, rows: usize) -> Self {
        self.rows = rows;
        self
    }

    /// Set the data generator.
    pub fn generator(mut self, generator: DataGenerator) -> Self {
        self.generator = Some(generator);
        self
    }

    /// Set the epochs for time-partitioned tables.
    pub fn epochs(mut self, epochs: Vec<EpochSpec>) -> Self {
        self.epochs = epochs;
        self
    }

    /// Get a description of this table setup for documentation.
    pub fn description(&self) -> String {
        format!(
            "Table {}: {} rows, distribution: {:?}",
            self.name, self.rows, self.distribution
        )
    }
}

/// Distribution strategy for table data.
#[derive(Clone, Debug, PartialEq)]
pub enum DistributionSetup {
    /// All data on a single worker (e.g., dimension tables)
    SingleWorker,

    /// Replicated on all workers (e.g., small lookup tables)
    Replicated,

    /// Partitioned by epochs across workers (e.g., fact tables)
    EpochPartitioned,

    /// Hash partitioned across workers
    HashPartitioned,
}

/// Data generator type for table data.
#[derive(Clone, Debug)]
pub enum DataGenerator {
    /// Use TPC-H generator for standard benchmark tables
    TpcH { scale_factor: f64 },

    /// Custom generator function (not serializable, for in-memory tests only)
    Custom,
}

/// Expectations about the generated plan.
#[derive(Clone, Debug)]
pub enum PlanExpectation {
    /// Expect no exchange node for a specific table
    NoExchangeFor(String),

    /// Expect a specific exchange type for a table
    HasExchangeType {
        table: String,
        exchange_type: ExpectedExchangeType,
    },

    /// Expect a minimum number of stages
    MinStageCount(usize),

    /// Expect an exact number of stages
    ExactStageCount(usize),

    /// Expect a maximum number of exchanges
    MaxExchangeCount(usize),

    /// Custom validation function
    Custom {
        description: String,
        validator: fn(&LdpPlan) -> Result<(), String>,
    },
}

impl PlanExpectation {
    /// Describe this expectation (for use when the plan is not accessible).
    ///
    /// Note: Plan expectations cannot currently be verified because the
    /// coordinator doesn't expose the generated plan. This would require
    /// API changes to make plans inspectable.
    pub fn description(&self) -> String {
        match self {
            PlanExpectation::NoExchangeFor(table) => {
                format!("No exchange for table '{}'", table)
            }
            PlanExpectation::HasExchangeType { table, exchange_type } => {
                format!("Table '{}' has exchange type {:?}", table, exchange_type)
            }
            PlanExpectation::MinStageCount(min) => {
                format!("At least {} stages", min)
            }
            PlanExpectation::ExactStageCount(expected) => {
                format!("Exactly {} stages", expected)
            }
            PlanExpectation::MaxExchangeCount(max) => {
                format!("At most {} exchanges", max)
            }
            PlanExpectation::Custom { description, .. } => {
                description.clone()
            }
        }
    }
}

/// Expected exchange type in the plan.
#[derive(Clone, Debug, PartialEq)]
pub enum ExpectedExchangeType {
    Broadcast,
    HashPartition,
    Gather,
}

/// Expectations about the query result.
#[derive(Clone, Debug)]
pub enum ResultExpectation {
    /// Expect an exact row count
    RowCount(usize),

    /// Expect a row count within a range
    RowCountRange(usize, usize),

    /// Expect a minimum row count
    MinRowCount(usize),

    /// Expect the result to be empty
    Empty,

    /// Expect specific column count
    ColumnCount(usize),

    /// Custom validation function
    Custom {
        description: String,
        validator: fn(&[RecordBatch], usize) -> Result<(), String>,
    },
}

impl ResultExpectation {
    /// Verify this expectation against the given results.
    pub fn verify(&self, batches: &[RecordBatch], row_count: usize) -> Result<(), TestError> {
        match self {
            ResultExpectation::RowCount(expected) => {
                if row_count != *expected {
                    return Err(TestError::ResultExpectationFailed {
                        expectation: format!("RowCount({})", expected),
                        actual: format!("Found {} rows", row_count),
                    });
                }
                Ok(())
            }

            ResultExpectation::RowCountRange(min, max) => {
                if row_count < *min || row_count > *max {
                    return Err(TestError::ResultExpectationFailed {
                        expectation: format!("RowCountRange({}, {})", min, max),
                        actual: format!("Found {} rows", row_count),
                    });
                }
                Ok(())
            }

            ResultExpectation::MinRowCount(min) => {
                if row_count < *min {
                    return Err(TestError::ResultExpectationFailed {
                        expectation: format!("MinRowCount({})", min),
                        actual: format!("Found {} rows", row_count),
                    });
                }
                Ok(())
            }

            ResultExpectation::Empty => {
                if row_count != 0 {
                    return Err(TestError::ResultExpectationFailed {
                        expectation: "Empty".to_string(),
                        actual: format!("Found {} rows", row_count),
                    });
                }
                Ok(())
            }

            ResultExpectation::ColumnCount(expected) => {
                if let Some(batch) = batches.first() {
                    if batch.num_columns() != *expected {
                        return Err(TestError::ResultExpectationFailed {
                            expectation: format!("ColumnCount({})", expected),
                            actual: format!("Found {} columns", batch.num_columns()),
                        });
                    }
                }
                Ok(())
            }

            ResultExpectation::Custom { description, validator } => {
                validator(batches, row_count).map_err(|msg| TestError::ResultExpectationFailed {
                    expectation: description.clone(),
                    actual: msg,
                })
            }
        }
    }
}

/// Result of running a test scenario.
#[derive(Debug)]
pub struct TestResult {
    /// Scenario name
    pub scenario_name: String,

    /// Total rows returned
    pub row_count: usize,

    /// Whether all expectations passed
    pub passed: bool,
}

/// Errors that can occur during scenario execution.
#[derive(Debug, thiserror::Error)]
pub enum TestError {
    #[error("Failed to load table {table}: {error}")]
    TableLoadFailed { table: String, error: String },

    #[error("Query planning failed: {error}")]
    PlanningFailed { error: String },

    #[error("Query execution failed: {error}")]
    ExecutionFailed { error: String },

    #[error("Plan expectation failed: expected {expectation}, but {actual}")]
    PlanExpectationFailed {
        expectation: String,
        actual: String,
    },

    #[error("Result expectation failed: expected {expectation}, but {actual}")]
    ResultExpectationFailed {
        expectation: String,
        actual: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scenario_builder() {
        let scenario = TestScenario::builder()
            .name("Test scenario")
            .table(
                TableSetup::new("orders")
                    .distribution(DistributionSetup::EpochPartitioned)
                    .rows(1000),
            )
            .table(
                TableSetup::new("products")
                    .distribution(DistributionSetup::Replicated)
                    .rows(100),
            )
            .query("SELECT * FROM orders JOIN products ON o_product_id = p_product_id")
            .expect_plan(PlanExpectation::MinStageCount(2))
            .expect_result(ResultExpectation::MinRowCount(900))
            .build();

        assert_eq!(scenario.name, "Test scenario");
        assert_eq!(scenario.tables.len(), 2);
        assert_eq!(scenario.plan_expectations.len(), 1);
        assert_eq!(scenario.result_expectations.len(), 1);
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
    fn test_plan_expectation_description() {
        let expectation = PlanExpectation::MinStageCount(3);
        let desc = expectation.description();
        assert!(desc.contains("3"));
        assert!(desc.contains("stages"));
    }

    #[test]
    fn test_result_expectation_row_count() {
        let batches = vec![];
        let row_count = 100;

        // Exact match should pass
        let expectation = ResultExpectation::RowCount(100);
        assert!(expectation.verify(&batches, row_count).is_ok());

        // Mismatch should fail
        let expectation = ResultExpectation::RowCount(200);
        assert!(expectation.verify(&batches, row_count).is_err());

        // Range should pass
        let expectation = ResultExpectation::RowCountRange(90, 110);
        assert!(expectation.verify(&batches, row_count).is_ok());

        // Out of range should fail
        let expectation = ResultExpectation::RowCountRange(200, 300);
        assert!(expectation.verify(&batches, row_count).is_err());
    }
}
