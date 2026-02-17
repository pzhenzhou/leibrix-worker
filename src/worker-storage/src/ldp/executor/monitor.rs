//! Stage execution monitor for runtime limit enforcement.
//!
//! This module provides facilities to monitor and enforce limits during
//! stage execution to prevent resource exhaustion and runaway queries.

use crate::ldp::StageLimits;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

/// Error types for limit violations.
#[derive(Debug, Error, Clone)]
pub enum LimitExceededError {
    /// Query execution exceeded timeout limit.
    #[error("Query timeout exceeded: {elapsed:?} > {limit:?}")]
    Timeout { elapsed: Duration, limit: Duration },

    /// Query exceeded maximum memory limit.
    #[error("Memory limit exceeded: {used} bytes > {limit} bytes")]
    Memory { used: u64, limit: u64 },

    /// Query exceeded maximum rows scan limit.
    #[error("Rows scan limit exceeded: {scanned} > {limit}")]
    RowsScan { scanned: u64, limit: u64 },

    /// Query exceeded maximum output rows limit.
    #[error("Output rows limit exceeded: {produced} > {limit}")]
    RowsOutput { produced: u64, limit: u64 },

    /// Query exceeded maximum output bytes limit.
    #[error("Output bytes limit exceeded: {produced} > {limit}")]
    BytesOutput { produced: u64, limit: u64 },
}

/// Monitor for enforcing limits during stage execution.
pub struct StageExecutionMonitor {
    /// Start time of the execution.
    start_time: Instant,

    /// The limits to enforce.
    limits: StageLimits,

    /// Counter for rows scanned during execution.
    rows_scanned: AtomicU64,

    /// Counter for rows produced during execution.
    rows_produced: AtomicU64,

    /// Counter for bytes produced during execution.
    bytes_produced: AtomicU64,

    /// Flag indicating if execution has been cancelled.
    cancelled: AtomicBool,
}

impl StageExecutionMonitor {
    /// Create a new stage execution monitor with the given limits.
    pub fn new(limits: StageLimits) -> Self {
        Self {
            start_time: Instant::now(),
            limits,
            rows_scanned: AtomicU64::new(0),
            rows_produced: AtomicU64::new(0),
            bytes_produced: AtomicU64::new(0),
            cancelled: AtomicBool::new(false),
        }
    }

    /// Increment the rows scanned counter.
    pub fn increment_rows_scanned(&self, count: u64) {
        self.rows_scanned.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment the rows produced counter.
    pub fn increment_rows_produced(&self, count: u64) {
        self.rows_produced.fetch_add(count, Ordering::Relaxed);
    }

    /// Increment the bytes produced counter.
    pub fn increment_bytes_produced(&self, count: u64) {
        self.bytes_produced.fetch_add(count, Ordering::Relaxed);
    }

    /// Check if any limit is exceeded.
    pub fn check_limits(&self) -> Result<(), LimitExceededError> {
        // Check for cancellation first
        if self.is_cancelled() {
            return Err(LimitExceededError::Timeout {
                elapsed: self.start_time.elapsed(),
                limit: Duration::from_millis(0),
            });
        }

        // Timeout check
        let elapsed = self.start_time.elapsed();
        let timeout_duration = Duration::from_millis(self.limits.timeout_ms);
        if elapsed > timeout_duration {
            return Err(LimitExceededError::Timeout {
                elapsed,
                limit: timeout_duration,
            });
        }

        // Row scan check
        let rows_scanned = self.rows_scanned.load(Ordering::Relaxed);
        if rows_scanned > self.limits.max_rows_output {
            return Err(LimitExceededError::RowsScan {
                scanned: rows_scanned,
                limit: self.limits.max_rows_output,
            });
        }

        // Output size checks
        let rows_produced = self.rows_produced.load(Ordering::Relaxed);
        if rows_produced > self.limits.max_rows_output {
            return Err(LimitExceededError::RowsOutput {
                produced: rows_produced,
                limit: self.limits.max_rows_output,
            });
        }

        let bytes_produced = self.bytes_produced.load(Ordering::Relaxed);
        if bytes_produced > self.limits.max_bytes_output {
            return Err(LimitExceededError::BytesOutput {
                produced: bytes_produced,
                limit: self.limits.max_bytes_output,
            });
        }

        Ok(())
    }

    /// Cancel execution.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Relaxed);
    }

    /// Check if execution has been cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Get current statistics.
    pub fn stats(&self) -> ExecutionStats {
        ExecutionStats {
            elapsed: self.start_time.elapsed(),
            rows_scanned: self.rows_scanned.load(Ordering::Relaxed),
            rows_produced: self.rows_produced.load(Ordering::Relaxed),
            bytes_produced: self.bytes_produced.load(Ordering::Relaxed),
        }
    }
}

/// Execution statistics.
#[derive(Debug, Clone)]
pub struct ExecutionStats {
    /// Elapsed time since start.
    pub elapsed: Duration,

    /// Number of rows scanned.
    pub rows_scanned: u64,

    /// Number of rows produced.
    pub rows_produced: u64,

    /// Number of bytes produced.
    pub bytes_produced: u64,
}

/// Shared reference to a stage execution monitor.
pub type SharedStageExecutionMonitor = Arc<StageExecutionMonitor>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_monitor_within_limits() {
        let limits = StageLimits {
            max_bytes_output: 1000,
            max_rows_output: 100,
            timeout_ms: 1000, // 1 second
            memory_bytes: 1024 * 1024, // 1 MB
        };

        let monitor = StageExecutionMonitor::new(limits);
        
        // Simulate some work within limits
        monitor.increment_rows_produced(50);
        monitor.increment_bytes_produced(500);
        
        // Should pass limit check
        assert!(monitor.check_limits().is_ok());
    }

    #[test]
    fn test_monitor_timeout() {
        let limits = StageLimits {
            max_bytes_output: 1000,
            max_rows_output: 100,
            timeout_ms: 1, // 1 millisecond - should timeout immediately
            memory_bytes: 1024 * 1024,
        };

        let monitor = StageExecutionMonitor::new(limits);
        
        // Sleep briefly to exceed the timeout
        std::thread::sleep(Duration::from_millis(10));
        
        // Should fail timeout check
        match monitor.check_limits() {
            Err(LimitExceededError::Timeout { .. }) => (),
            _ => panic!("Expected timeout error"),
        }
    }

    #[test]
    fn test_monitor_row_limit_exceeded() {
        let limits = StageLimits {
            max_bytes_output: 1000,
            max_rows_output: 10, // Small limit
            timeout_ms: 1000,
            memory_bytes: 1024 * 1024,
        };

        let monitor = StageExecutionMonitor::new(limits);
        
        // Exceed row limit
        monitor.increment_rows_produced(15);
        
        // Should fail row limit check
        match monitor.check_limits() {
            Err(LimitExceededError::RowsOutput { produced, limit }) => {
                assert_eq!(produced, 15);
                assert_eq!(limit, 10);
            }
            _ => panic!("Expected rows output error"),
        }
    }

    #[test]
    fn test_monitor_bytes_limit_exceeded() {
        let limits = StageLimits {
            max_bytes_output: 100, // Small limit
            max_rows_output: 100,
            timeout_ms: 1000,
            memory_bytes: 1024 * 1024,
        };

        let monitor = StageExecutionMonitor::new(limits);
        
        // Exceed bytes limit
        monitor.increment_bytes_produced(150);
        
        // Should fail bytes limit check
        match monitor.check_limits() {
            Err(LimitExceededError::BytesOutput { produced, limit }) => {
                assert_eq!(produced, 150);
                assert_eq!(limit, 100);
            }
            _ => panic!("Expected bytes output error"),
        }
    }

    #[test]
    fn test_monitor_cancellation() {
        let limits = StageLimits {
            max_bytes_output: 1000,
            max_rows_output: 100,
            timeout_ms: 1000,
            memory_bytes: 1024 * 1024,
        };

        let monitor = StageExecutionMonitor::new(limits);
        
        // Cancel execution
        monitor.cancel();
        
        // Should fail with timeout error due to cancellation
        assert!(monitor.check_limits().is_err());
        assert!(monitor.is_cancelled());
    }

    #[test]
    fn test_monitor_stats() {
        let limits = StageLimits {
            max_bytes_output: 1000,
            max_rows_output: 100,
            timeout_ms: 1000,
            memory_bytes: 1024 * 1024,
        };

        let monitor = StageExecutionMonitor::new(limits);
        
        // Update counters
        monitor.increment_rows_scanned(10);
        monitor.increment_rows_produced(20);
        monitor.increment_bytes_produced(30);
        
        let stats = monitor.stats();
        assert_eq!(stats.rows_scanned, 10);
        assert_eq!(stats.rows_produced, 20);
        assert_eq!(stats.bytes_produced, 30);
    }
}