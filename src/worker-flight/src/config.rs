use std::net::SocketAddr;
use tonic::transport::ServerTlsConfig;

/// Configuration for the Arrow Flight server.
///
/// Uses standard `tonic` types for TLS configuration to avoid reinventing the wheel.
#[derive(Debug, Clone)]
pub struct FlightServerConfig {
    /// Address to bind the Flight server (e.g., "0.0.0.0:8815")
    pub bind_addr: SocketAddr,

    /// Optional TLS configuration (paths to cert/key files)
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
            tls_config: None,
            max_message_size: 16 * 1024 * 1024, // 16 MB
            concurrency_limit: 1000,
        }
    }
}
