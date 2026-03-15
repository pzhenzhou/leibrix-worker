use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use std::net::SocketAddr;
use std::path::PathBuf;
use tonic::transport::{Certificate, ClientTlsConfig, Identity, ServerTlsConfig};

use worker_cp::ControlPlaneConfig;
use worker_storage::engine::duckdb::DuckDBConfig;

/// Type alias proving `worker-cli` is wired to the control-plane API surface.
///
/// This does not establish a connection; it only ensures the CLI config can
/// cleanly map into the generated client types.
#[allow(dead_code)]
pub type ControlPlaneClient =
    worker_proto::control_plane::control_plane_service_client::ControlPlaneServiceClient<
        tonic::transport::Channel,
    >;

#[derive(Debug, Parser)]
#[command(
    name = "liebrix-worker",
    about = "Leibrix worker process (data plane)",
    long_about = "Worker process for the Leibrix in-memory acceleration layer.\n\nThe worker is responsible for: (1) connecting to the master control plane, (2) pulling/loading epochs into its embedded DuckDB storage engine, and (3) serving queries.",
    arg_required_else_help = true,
    disable_help_subcommand = true
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Start the worker process.
    Run(RunArgs),
}

/// Fully resolved runtime configuration for `liebrix-worker`.
///
/// This struct lives in `worker-cli` and is produced from clap-parsed arguments.
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct LeibrixWorkerConfig {
    pub tenant_id: String,
    pub worker_id: String,
    pub master_endpoint: String,
    pub query_listen_addr: Option<SocketAddr>,
    /// Advertised address for registration. If set, this overrides query_listen_addr
    /// for the address reported to the control plane. Use this when binding to
    /// 0.0.0.0 or other non-routable addresses.
    pub advertise_addr: Option<String>,
    /// TLS config for CP client (outbound to master).
    pub tls: TlsConfig,
    /// TLS config for Flight server (inbound queries).
    pub flight_server_tls: FlightServerTlsConfig,
    pub duckdb: DuckDBConfig,
    /// Capacity of the domain-level outgoing event channel (worker → CP).
    pub cp_outgoing_capacity: usize,
    /// Max concurrent `DataLoader::load_epoch` operations.
    pub cp_max_concurrent_loads: usize,
}

#[derive(Debug, Clone, Default)]
#[allow(dead_code)]
pub struct TlsConfig {
    pub ca_path: Option<PathBuf>,
    pub cert_path: Option<PathBuf>,
    pub key_path: Option<PathBuf>,
    pub server_name: Option<String>,
}

/// TLS configuration for the Flight gRPC server.
#[derive(Debug, Clone, Default)]
pub struct FlightServerTlsConfig {
    /// Server certificate path (PEM format).
    pub cert_path: Option<PathBuf>,
    /// Server private key path (PEM format).
    pub key_path: Option<PathBuf>,
}

#[derive(Debug, Args, Clone)]
pub struct RunArgs {
    /// Tenant identifier (required).
    #[arg(
        long,
        env = "LEIBRIX_TENANT_ID",
        value_name = "TENANT_ID",
        help = "Tenant identifier for multi-tenancy isolation",
        long_help = "Tenant identifier for multi-tenancy isolation.\n\nThis value is attached to every control-plane event and is used for namespacing and quota enforcement.\n\nCan be provided via $LEIBRIX_TENANT_ID."
    )]
    pub tenant_id: String,

    /// Worker identifier (required).
    #[arg(
        long,
        env = "LEIBRIX_WORKER_ID",
        value_name = "WORKER_ID",
        help = "Worker node identifier (e.g., pod name)",
        long_help = "Worker node identifier.\n\nIn Kubernetes, a common choice is to set this to the Pod name via the Downward API.\n\nCan be provided via $LEIBRIX_WORKER_ID."
    )]
    pub worker_id: String,

    /// Master control-plane gRPC endpoint.
    #[arg(
        long,
        env = "LEIBRIX_MASTER_ENDPOINT",
        value_name = "HOST:PORT",
        help = "Master control-plane endpoint (gRPC)",
        long_help = "Master control-plane endpoint (gRPC), e.g. master.leibrix.svc.cluster.local:9090.\n\nCan be provided via $LEIBRIX_MASTER_ENDPOINT."
    )]
    pub master_endpoint: String,

    /// Optional query service listen address.
    ///
    /// Note: the query service is not implemented yet in this workspace; this is
    /// included to keep the CLI stable.
    #[arg(
        long,
        env = "LEIBRIX_QUERY_LISTEN_ADDR",
        value_name = "IP:PORT",
        help = "Query service listen address (optional)",
        long_help = "Query service listen address (optional).\n\nIf omitted, the worker may run without exposing a query endpoint (depending on build/runtime integration).\n\nCan be provided via $LEIBRIX_QUERY_LISTEN_ADDR."
    )]
    pub query_listen_addr: Option<SocketAddr>,

    /// Disable DuckDB memory limit (use with caution).
    #[arg(
        long,
        env = "LEIBRIX_DUCKDB_NO_MEMORY_LIMIT",
        default_value_t = false,
        help = "Disable DuckDB memory limit",
        long_help = "Disable DuckDB memory limit.\n\nBy default, the worker sets DuckDB's memory_limit pragmas to a bounded value. Disabling this may risk OOM.\n\nCan be provided via $LEIBRIX_DUCKDB_NO_MEMORY_LIMIT (set to 'true')."
    )]
    pub duckdb_no_memory_limit: bool,

    /// DuckDB memory limit in MB.
    #[arg(
        long,
        env = "LEIBRIX_DUCKDB_MEMORY_LIMIT_MB",
        default_value_t = 1024u64,
        value_parser = clap::value_parser!(u64).range(1..),
        conflicts_with = "duckdb_no_memory_limit",
        help = "DuckDB memory limit (MB)",
        long_help = "DuckDB memory limit in MB.\n\nDefaults to 1024 (1 GiB), matching the current storage engine default.\n\nCan be provided via $LEIBRIX_DUCKDB_MEMORY_LIMIT_MB."
    )]
    pub duckdb_memory_limit_mb: u64,

    /// Flush threshold in rows for buffering Arrow batches before appending into DuckDB.
    #[arg(
        long,
        env = "LEIBRIX_DUCKDB_FLUSH_ROWS_THRESHOLD",
        default_value_t = 10_000u64,
        value_parser = clap::value_parser!(u64).range(1..),
        help = "DuckDB flush threshold (rows)",
        long_help = "Flush threshold (rows) for buffering Arrow batches before appending into DuckDB.\n\nDefaults to 10,000, matching the current DuckDB engine default.\n\nCan be provided via $LEIBRIX_DUCKDB_FLUSH_ROWS_THRESHOLD."
    )]
    pub duckdb_flush_rows_threshold: u64,

    /// Capacity of the storage engine command channel.
    #[arg(
        long,
        env = "LEIBRIX_DUCKDB_CHANNEL_CAPACITY",
        default_value_t = 100u32,
        value_parser = clap::value_parser!(u32).range(1..),
        help = "DuckDB engine channel capacity",
        long_help = "Capacity of the in-process command channel used to talk to the DuckDB engine actor.\n\nDefaults to 100, matching the current DuckDB engine default.\n\nCan be provided via $LEIBRIX_DUCKDB_CHANNEL_CAPACITY."
    )]
    pub duckdb_channel_capacity: u32,

    /// DuckDB temp directory (enables disk spill if needed).
    #[arg(
        long,
        env = "LEIBRIX_DUCKDB_TMP_DIR",
        value_name = "PATH",
        help = "DuckDB temp directory for disk spilling (optional)",
        long_help = "DuckDB temp directory for disk spilling (optional).\n\nIf provided, the worker sets DuckDB's temp_directory pragma.\n\nCan be provided via $LEIBRIX_DUCKDB_TMP_DIR."
    )]
    pub duckdb_tmp_dir: Option<PathBuf>,

    /// Maximum number of identifiers (internal safety cap).
    #[arg(
        long,
        env = "LEIBRIX_DUCKDB_MAX_IDENTIFIERS",
        default_value_t = 1000u32,
        value_parser = clap::value_parser!(u32).range(1..),
        help = "Maximum identifiers (safety cap)",
        long_help = "Maximum number of identifiers (safety cap).\n\nDefaults to 1000, matching the current DuckDB engine default.\n\nCan be provided via $LEIBRIX_DUCKDB_MAX_IDENTIFIERS."
    )]
    pub duckdb_max_identifiers: u32,

    /// CA certificate path for TLS.
    #[arg(
        long,
        env = "LEIBRIX_TLS_CA",
        value_name = "PATH",
        help = "TLS CA certificate path (optional)",
        long_help = "TLS CA certificate path (optional).\n\nIf any TLS material is provided, CA/cert/key must all be set.\n\nCan be provided via $LEIBRIX_TLS_CA."
    )]
    pub tls_ca: Option<PathBuf>,

    /// Client certificate path for TLS.
    #[arg(
        long,
        env = "LEIBRIX_TLS_CERT",
        value_name = "PATH",
        help = "TLS client certificate path (optional)",
        long_help = "TLS client certificate path (optional).\n\nIf any TLS material is provided, CA/cert/key must all be set.\n\nCan be provided via $LEIBRIX_TLS_CERT."
    )]
    pub tls_cert: Option<PathBuf>,

    /// Client private key path for TLS.
    #[arg(
        long,
        env = "LEIBRIX_TLS_KEY",
        value_name = "PATH",
        help = "TLS client private key path (optional)",
        long_help = "TLS client private key path (optional).\n\nIf any TLS material is provided, CA/cert/key must all be set.\n\nCan be provided via $LEIBRIX_TLS_KEY."
    )]
    pub tls_key: Option<PathBuf>,

    /// Override expected server name for TLS verification.
    #[arg(
        long,
        env = "LEIBRIX_TLS_SERVER_NAME",
        value_name = "DNS_NAME",
        help = "TLS server name override (optional)",
        long_help = "Override expected server name for TLS verification (optional).\n\nCan be provided via $LEIBRIX_TLS_SERVER_NAME."
    )]
    pub tls_server_name: Option<String>,

    /// Flight server TLS certificate path.
    #[arg(
        long,
        env = "LEIBRIX_FLIGHT_TLS_CERT",
        value_name = "PATH",
        help = "Flight server TLS certificate path (optional)",
        long_help = "Flight server TLS certificate path (PEM format).\n\nIf provided, --flight-tls-key must also be set.\n\nCan be provided via $LEIBRIX_FLIGHT_TLS_CERT."
    )]
    pub flight_tls_cert: Option<PathBuf>,

    /// Flight server TLS private key path.
    #[arg(
        long,
        env = "LEIBRIX_FLIGHT_TLS_KEY",
        value_name = "PATH",
        help = "Flight server TLS private key path (optional)",
        long_help = "Flight server TLS private key path (PEM format).\n\nIf provided, --flight-tls-cert must also be set.\n\nCan be provided via $LEIBRIX_FLIGHT_TLS_KEY."
    )]
    pub flight_tls_key: Option<PathBuf>,

    /// Outgoing CP event channel capacity.
    #[arg(
        long,
        env = "LEIBRIX_CP_OUTGOING_CAPACITY",
        default_value_t = 64usize,
        value_parser = clap::value_parser!(usize),
        help = "Outgoing CP channel capacity (default: 64)"
    )]
    pub cp_outgoing_capacity: usize,

    /// Max concurrent epoch loads.
    #[arg(
        long,
        env = "LEIBRIX_CP_MAX_CONCURRENT_LOADS",
        default_value_t = 4usize,
        value_parser = clap::value_parser!(usize),
        help = "Max concurrent DataLoader::load_epoch operations (default: 4)"
    )]
    pub cp_max_concurrent_loads: usize,

    /// Advertised query address for the worker.
    ///
    /// This is the routable address that the control plane and gateways will use
    /// to connect to this worker. Use this when `query_listen_addr` is bound to
    /// `0.0.0.0` or another non-routable address.
    ///
    /// If not provided, `query_listen_addr` is used (which may be non-routable).
    #[arg(
        long,
        env = "LEIBRIX_ADVERTISE_ADDR",
        value_name = "HOST:PORT",
        help = "Advertised query address (overrides query-listen-addr for registration)",
        long_help = "Advertised query address for the worker.\n\nThis is the routable address that the control plane and gateways will use to connect to this worker. Use this when --query-listen-addr is bound to 0.0.0.0 or another non-routable address.\n\nIf not provided, --query-listen-addr is used (which may be non-routable in containerized environments).\n\nCan be provided via $LEIBRIX_ADVERTISE_ADDR."
    )]
    pub advertise_addr: Option<String>,
}

impl LeibrixWorkerConfig {
    pub fn from_run_args(args: RunArgs) -> Result<Self> {
        validate_non_empty("tenant-id", &args.tenant_id)?;
        validate_non_empty("worker-id", &args.worker_id)?;
        validate_non_empty("master-endpoint", &args.master_endpoint)?;

        if args.cp_outgoing_capacity == 0 {
            anyhow::bail!("cp-outgoing-capacity must be at least 1");
        }
        if args.cp_max_concurrent_loads == 0 {
            anyhow::bail!("cp-max-concurrent-loads must be at least 1");
        }

        // TLS: either all (ca/cert/key) are present, or none.
        let tls_any = args.tls_ca.is_some() || args.tls_cert.is_some() || args.tls_key.is_some();
        if tls_any && (args.tls_ca.is_none() || args.tls_cert.is_none() || args.tls_key.is_none()) {
            anyhow::bail!(
                "TLS is partially configured; provide --tls-ca, --tls-cert, and --tls-key together"
            );
        }

        // Flight server TLS: either both (cert/key) are present, or none.
        let flight_tls_any = args.flight_tls_cert.is_some() || args.flight_tls_key.is_some();
        if flight_tls_any
            && (args.flight_tls_cert.is_none() || args.flight_tls_key.is_none())
        {
            anyhow::bail!(
                "Flight server TLS is partially configured; provide --flight-tls-cert and --flight-tls-key together"
            );
        }

        let channel_capacity: usize = usize::try_from(args.duckdb_channel_capacity)
            .context("duckdb-channel-capacity is too large for this platform")?;
        let max_identifiers: usize = usize::try_from(args.duckdb_max_identifiers)
            .context("duckdb-max-identifiers is too large for this platform")?;

        let duckdb = DuckDBConfig {
            flush_rows_threshold: args.duckdb_flush_rows_threshold,
            channel_capacity,
            tmp_dir: args.duckdb_tmp_dir,
            max_identifiers,
            memory_limit_mb: if args.duckdb_no_memory_limit {
                None
            } else {
                Some(args.duckdb_memory_limit_mb)
            },
            ..DuckDBConfig::default()
        };

        Ok(Self {
            tenant_id: args.tenant_id,
            worker_id: args.worker_id,
            master_endpoint: args.master_endpoint,
            query_listen_addr: args.query_listen_addr,
            advertise_addr: args.advertise_addr,
            tls: TlsConfig {
                ca_path: args.tls_ca,
                cert_path: args.tls_cert,
                key_path: args.tls_key,
                server_name: args.tls_server_name,
            },
            flight_server_tls: FlightServerTlsConfig {
                cert_path: args.flight_tls_cert,
                key_path: args.flight_tls_key,
            },
            duckdb,
            cp_outgoing_capacity: args.cp_outgoing_capacity,
            cp_max_concurrent_loads: args.cp_max_concurrent_loads,
        })
    }

    /// Build a [`ControlPlaneConfig`] from this runtime configuration.
    ///
    /// Returns an error if TLS certificate/key files are specified but cannot be read.
    pub fn cp_config(&self) -> anyhow::Result<ControlPlaneConfig> {
        // Build TLS config if certificate material was provided.
        // All three files (CA, cert, key) must be present or none (validated at parse time).
        let tls = if let Some(ca_path) = &self.tls.ca_path {
            let ca_pem = std::fs::read(ca_path).with_context(|| {
                format!("failed to read TLS CA certificate: {}", ca_path.display())
            })?;
            let cert_pem =
                std::fs::read(self.tls.cert_path.as_ref().unwrap()).with_context(|| {
                    format!(
                        "failed to read TLS client certificate: {}",
                        self.tls.cert_path.as_ref().unwrap().display()
                    )
                })?;
            let key_pem =
                std::fs::read(self.tls.key_path.as_ref().unwrap()).with_context(|| {
                    format!(
                        "failed to read TLS client key: {}",
                        self.tls.key_path.as_ref().unwrap().display()
                    )
                })?;

            let mut tls_config = ClientTlsConfig::new()
                .ca_certificate(Certificate::from_pem(ca_pem))
                .identity(Identity::from_pem(cert_pem, key_pem));

            if let Some(ref server_name) = self.tls.server_name {
                tls_config = tls_config.domain_name(server_name);
            }

            Some(tls_config)
        } else {
            None
        };

        Ok(ControlPlaneConfig {
            master_addr: self.master_endpoint.clone(),
            worker_id: self.worker_id.clone(),
            tenant_id: self.tenant_id.clone(),
            // Prefer advertise_addr > query_listen_addr for the address reported to CP.
            // advertise_addr should be set when query_listen_addr is non-routable (e.g., 0.0.0.0).
            worker_addr: self
                .advertise_addr
                .clone()
                .or_else(|| self.query_listen_addr.map(|a| a.to_string())),
            outgoing_channel_capacity: self.cp_outgoing_capacity,
            tls,
            ..ControlPlaneConfig::default()
        })
    }

    /// Build a [`ServerTlsConfig`] for the Flight server, if TLS material was provided.
    ///
    /// Returns `Ok(None)` if no Flight server TLS is configured.
    /// Returns an error if certificate/key files are specified but cannot be read.
    pub fn flight_server_tls_config(&self) -> anyhow::Result<Option<ServerTlsConfig>> {
        let Some(cert_path) = &self.flight_server_tls.cert_path else {
            return Ok(None);
        };
        let key_path = self.flight_server_tls.key_path.as_ref().unwrap(); // validated at parse time

        let cert_pem = std::fs::read(cert_path).with_context(|| {
            format!(
                "failed to read Flight server TLS certificate: {}",
                cert_path.display()
            )
        })?;
        let key_pem = std::fs::read(key_path).with_context(|| {
            format!(
                "failed to read Flight server TLS key: {}",
                key_path.display()
            )
        })?;

        let tls_config = ServerTlsConfig::new().identity(Identity::from_pem(cert_pem, key_pem));

        Ok(Some(tls_config))
    }
}

fn validate_non_empty(field: &'static str, value: &str) -> Result<()> {
    if value.trim().is_empty() {
        anyhow::bail!("{field} must be non-empty");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Parser;

    #[test]
    fn parse_minimal_args_ok() {
        let cli = Cli::try_parse_from([
            "liebrix-worker",
            "run",
            "--tenant-id",
            "t1",
            "--worker-id",
            "w1",
            "--master-endpoint",
            "master:9090",
        ])
        .expect("cli parse should succeed");

        let Command::Run(args) = cli.command;
        let cfg = LeibrixWorkerConfig::from_run_args(args).expect("config build should succeed");

        assert_eq!(cfg.tenant_id, "t1");
        assert_eq!(cfg.worker_id, "w1");
        assert_eq!(cfg.master_endpoint, "master:9090");

        // Defaults should match DuckDBConfig::default().
        assert_eq!(cfg.duckdb.memory_limit_mb, Some(1024));
        assert_eq!(cfg.duckdb.flush_rows_threshold, 10_000);
        assert_eq!(cfg.duckdb.channel_capacity, 100);
        assert_eq!(cfg.duckdb.max_identifiers, 1000);
    }

    #[test]
    fn invalid_args_rejected() {
        let cli = Cli::try_parse_from([
            "liebrix-worker",
            "run",
            "--tenant-id",
            " ",
            "--worker-id",
            "w1",
            "--master-endpoint",
            "master:9090",
        ])
        .expect("clap accepts the value; semantic validation happens later");

        let Command::Run(args) = cli.command;
        let err = LeibrixWorkerConfig::from_run_args(args).unwrap_err();
        assert!(
            err.to_string().contains("tenant-id must be non-empty"),
            "unexpected error: {err:?}"
        );
    }

    #[test]
    fn non_default_duckdb_options_applied() {
        let cli = Cli::try_parse_from([
            "liebrix-worker",
            "run",
            "--tenant-id",
            "t1",
            "--worker-id",
            "w1",
            "--master-endpoint",
            "master:9090",
            "--duckdb-flush-rows-threshold",
            "500",
            "--duckdb-channel-capacity",
            "10",
            "--duckdb-max-identifiers",
            "42",
            "--duckdb-no-memory-limit",
        ])
        .expect("cli parse should succeed");

        let Command::Run(args) = cli.command;
        let cfg = LeibrixWorkerConfig::from_run_args(args).expect("config build should succeed");

        assert_eq!(cfg.duckdb.memory_limit_mb, None);
        assert_eq!(cfg.duckdb.flush_rows_threshold, 500);
        assert_eq!(cfg.duckdb.channel_capacity, 10);
        assert_eq!(cfg.duckdb.max_identifiers, 42);
    }
}
