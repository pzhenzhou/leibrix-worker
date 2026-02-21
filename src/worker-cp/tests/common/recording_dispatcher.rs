//! A test-only [`TaskDispatcher`] that records all dispatched commands.

use std::sync::Arc;
use tokio::sync::Mutex;
use worker_cp::dispatch::TaskDispatcher;
use worker_cp::types::ControlCommand;

/// Records every call to `handle_command` for later assertion.
pub struct RecordingDispatcher {
    commands: Arc<Mutex<Vec<ControlCommand>>>,
}

impl RecordingDispatcher {
    pub fn new() -> Self {
        Self {
            commands: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Return a snapshot of all recorded commands.
    pub async fn commands(&self) -> Vec<ControlCommand> {
        self.commands.lock().await.clone()
    }

    /// Wait until at least `n` commands have been recorded, with a timeout.
    pub async fn wait_for_commands(&self, n: usize, timeout: std::time::Duration) {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.commands.lock().await.len() >= n {
                return;
            }
            if tokio::time::Instant::now() >= deadline {
                panic!(
                    "timed out waiting for {} commands (got {})",
                    n,
                    self.commands.lock().await.len()
                );
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        }
    }
}

impl TaskDispatcher for RecordingDispatcher {
    fn handle_command(
        &self,
        command: ControlCommand,
    ) -> impl std::future::Future<Output = ()> + Send {
        let commands = Arc::clone(&self.commands);
        async move {
            commands.lock().await.push(command);
        }
    }
}
