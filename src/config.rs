use clap::Parser;
use reqwest::header::HeaderName;
use reqwest::header::HeaderValue;
use std::net::SocketAddr;
use std::path::PathBuf;
use url::Url;

use crate::AppEnv;

#[derive(Debug, Clone, clap::ValueEnum)]
pub enum Package {
    /// Index your own application data.
    App,

    /// Index the frontier world data.
    World,
}

#[derive(Parser)]
pub struct DbConfig {
    #[arg(
        id = "db_user",
        long = "db_user",
        env = "DB_USER",
        default_value = "postgres"
    )]
    pub db_user: String,

    #[arg(
        id = "db_password",
        long = "db_password",
        env = "DB_PASSWORD",
        default_value = "postgres"
    )]
    pub db_password: String,

    #[arg(
        id = "db_host",
        long = "db_host",
        env = "DB_HOST",
        default_value = "localhost"
    )]
    pub db_host: String,

    #[arg(
        id = "db_port",
        long = "db_port",
        env = "DB_PORT",
        default_value_t = 5432
    )]
    pub db_port: u16,

    #[arg(
        id = "db_name",
        long = "db_name",
        env = "DB_NAME",
        default_value = "postgres"
    )]
    pub db_name: String,

    #[arg(
        id = "db_schema",
        long = "db_schema",
        env = "DB_SCHEMA",
        default_value = "indexer"
    )]
    pub db_schema: String,

    #[arg(
        id = "db_connection_pool_size",
        long = "db_connection_pool_size",
        env = "DB_CONNECTION_POOL_SIZE",
        default_value_t = 100
    )]
    pub db_connection_pool_size: u32,

    #[arg(
        id = "db_connection_timeout_ms",
        long = "db_connection_timeout_ms",
        env = "DB_CONNECTION_TIMEOUT_MS",
        default_value_t = 60_000
    )]
    pub db_connection_timeout_ms: u64,

    #[arg(
        id = "db_statement_timeout_ms",
        long = "db_statement_timeout_ms",
        env = "DB_STATEMENT_TIMEOUT_MS"
    )]
    pub db_statement_timeout_ms: Option<u64>,

    #[arg(
        id = "tls_verify_cert",
        long = "tls_verify_cert",
        env = "DB_TLS_VERIFY_CERT",
        default_value_t = false
    )]
    pub tls_verify_cert: bool,

    #[arg(
        id = "tls_ca_cert_path",
        long = "tls_ca_cert_path",
        env = "DB_TLS_CA_CERT_PATH"
    )]
    pub tls_ca_cert_path: Option<PathBuf>,
}

#[derive(Parser)]
pub struct IndexerConfig {
    #[arg(
        id = "first_checkpoint",
        long = "first_checkpoint",
        env = "FIRST_CHECKPOINT"
    )]
    pub first_checkpoint: Option<u64>,

    #[arg(
        id = "last_checkpoint",
        long = "last_checkpoint",
        env = "LAST_CHECKPOINT"
    )]
    pub last_checkpoint: Option<u64>,

    #[arg(
        id = "pipelines",
        long = "pipelines",
        env = "PIPELINES",
        value_delimiter = ','
    )]
    pub pipeline: Vec<String>,
}

#[derive(Parser)]
pub struct Sequential {
    #[arg(id = "min_eager_rows", long = "min_eager_rows", env = "MIN_EAGER_ROWS")]
    pub min_eager_rows: Option<usize>,

    #[arg(
        id = "max_batch_checkpoints",
        long = "max_batch_checkpoints",
        env = "MAX_BATCH_CHECKPOINTS"
    )]
    pub max_batch_checkpoints: Option<usize>,

    #[arg(
        id = "processor_channel_size",
        long = "processor_channel_size",
        env = "PROCESSOR_CHANNEL_SIZE"
    )]
    pub processor_channel_size: Option<usize>,

    #[arg(
        id = "write_concurrency",
        long = "write_concurrency",
        env = "WRITE_CONCURRENCY",
        default_value_t = 5
    )]
    pub write_concurrency: usize,

    #[arg(
        id = "collect_interval_ms",
        long = "collect_interval_ms",
        env = "COLLECT_INTERVAL_MS",
        default_value_t = 500
    )]
    pub collect_interval_ms: u64,

    #[arg(
        id = "watermark_interval_ms",
        long = "watermark_interval_ms",
        env = "WATERMARK_INTERVAL_MS",
        default_value_t = 500
    )]
    pub watermark_interval_ms: u64,

    #[arg(
        id = "watermark_interval_jitter_ms",
        long = "watermark_interval_jitter_ms",
        env = "WATERMARK_INTERVAL_JITTER_MS",
        default_value_t = 0
    )]
    pub watermark_interval_jitter_ms: u64,
}

#[derive(Parser)]
pub struct Ingestion {
    #[arg(
        id = "ingest_concurrency_max",
        long = "ingest_concurrency_max",
        env = "INGEST_CONCURRENCY_MAX",
        default_value_t = 500
    )]
    pub ingest_concurrency_max: usize,

    #[arg(
        id = "retry_interval_ms",
        long = "retry_interval_ms",
        env = "RETRY_INTERVAL_MS",
        default_value_t = 200
    )]
    pub retry_interval_ms: u64,

    #[arg(
        id = "streaming_backoff_initial_batch_size",
        long = "streaming_backoff_initial_batch_size",
        env = "STREAMING_BACKOFF_INITIAL_BATCH_SIZE",
        default_value_t = 10
    )]
    pub streaming_backoff_initial_batch_size: usize,

    #[arg(
        id = "streaming_backoff_max_batch_size",
        long = "streaming_backoff_max_batch_size",
        env = "STREAMING_BACKOFF_MAX_BATCH_SIZE",
        default_value_t = 10000
    )]
    pub streaming_backoff_max_batch_size: usize,

    #[arg(
        id = "streaming_connection_timeout_ms",
        long = "streaming_connection_timeout_ms",
        env = "STREAMING_CONNECTION_TIMEOUT_MS",
        default_value_t = 5000
    )]
    pub streaming_connection_timeout_ms: u64,

    #[arg(
        id = "streaming_statement_timeout_ms",
        long = "streaming_statement_timeout_ms",
        env = "STREAMING_STATEMENT_TIMEOUT_MS",
        default_value_t = 5000
    )]
    pub streaming_statement_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum IngestionSource {
    /// Ingest checkpoints from a store like https://checkpoints.testnet.sui.io
    Store,

    /// Ingest checlpoints from a fullnode like https://fullnode.testnet.sui.io:443
    Fullnode,

    // Ingest checkpoints from a local file path
    Local,
}

#[derive(Parser)]
pub struct IngestionClient {
    #[arg(
        id = "ingestion_source",
        long = "ingestion_source",
        env = "INGESTION_SOURCE",
        default_value="store",
    )]
    pub ingestion_source: IngestionSource,

    #[arg(
        id = "remote_store_url",
        long = "remote_store_url",
        env = "REMOTE_STORE_URL"
    )]
    pub remote_store_url: Option<String>,

    #[arg(
        id = "remote_store_s3",
        long = "remote_store_s3",
        env = "REMOTE_STORE_S3"
    )]
    pub remote_store_s3: Option<String>,

    #[arg(
        id = "remote_store_gcs",
        long = "remote_store_gcs",
        env = "REMOTE_STORE_GCS"
    )]
    pub remote_store_gcs: Option<String>,

    #[arg(
        id = "remote_store_azure",
        long = "remote_store_azure",
        env = "REMOTE_STORE_AZURE"
    )]
    pub remote_store_azure: Option<String>,

    #[arg(
        id = "remote_store_headers",
        long = "remote_store_headers",
        env = "REMOTE_STORE_HEADERS",
        value_delimiter = ',',
        value_parser = parse_remote_store_header
    )]
    pub remote_store_headers: Vec<(HeaderName, HeaderValue)>,

    #[arg(
        id = "local_ingestion_path",
        long = "local_ingestion_path",
        env = "LOCAL_INGESTION_PATH"
    )]
    pub local_ingestion_path: Option<PathBuf>,

    #[arg(id = "rpc_api_url", long = "rpc_api_url", env = "RPC_API_URL")]
    pub rpc_api_url: Option<Url>,

    #[arg(id = "rpc_username", long = "rpc_username", env = "RPC_USERNAME")]
    pub rpc_username: Option<String>,

    #[arg(id = "rpc_password", long = "rpc_password", env = "RPC_PASSWORD")]
    pub rpc_password: Option<String>,

    #[arg(
        id = "checkpoint_timeout_ms",
        long = "checkpoint_timeout_ms",
        env = "CHECKPOINT_TIMEOUT_MS",
        default_value_t = 120_000
    )]
    pub checkpoint_timeout_ms: u64,

    #[arg(
        id = "checkpoint_connection_timeout_ms",
        long = "checkpoint_connection_timeout_ms",
        env = "CHECKPOINT_CONNECTION_TIMEOUT_MS",
        default_value_t = 120_000
    )]
    pub checkpoint_connection_timeout_ms: u64,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum SandboxEnv {
    Testnet,
    Localnet,
}

#[derive(Parser)]
pub struct SandboxArgs {
    #[arg(
        id = "sandbox_enabled",
        long = "sandbox_enabled",
        env = "SANDBOX_ENABLED",
        requires = "sandbox_app_packages",
        default_value_t = false
    )]
    pub enabled: bool,

    #[arg(
        id = "sandbox_network",
        long = "sandbox_network",
        env = "SANDBOX_NETWORK",
        default_value = "localnet"
    )]
    pub env: SandboxEnv,

    #[arg(
        id = "sandbox_app_packages",
        long = "sandbox_app_packages",
        env = "SANDBOX_APP_PACKAGES",
        value_delimiter = ','
    )]
    pub app_package_ids: Vec<String>,

    #[clap(
        id = "sandbox_world_packages",
        long = "sandbox_world_packages",
        env = "SANDBOX_WORLD_PACKAGES",
        value_delimiter = ','
    )]
    pub world_packages: Vec<String>,

    #[clap(
        id = "sandbox_ingestion_path",
        long = "sandbox_ingestion_path",
        env = "SANDBOX_INGESTION_PATH"
    )]
    pub local_ingestion_path: Option<PathBuf>,
}

#[derive(Parser)]
pub struct AmqpConfig {
    #[arg(id = "amqp_url", long = "amqp_url", env = "AMQP_URL")]
    pub url: Option<String>,

    #[arg(
        id = "amqp_exchange",
        long = "amqp_exchange",
        env = "AMQP_EXCHANGE",
        default_value = "indexer"
    )]
    pub exchange: String,

    #[arg(
        id = "amqp_pool_size",
        long = "amqp_pool_size",
        env = "AMQP_POOL_SIZE",
        default_value_t = 10
    )]
    pub pool_size: usize,
}

#[derive(Parser)]
pub struct NatsConfig {
    #[arg(id = "nats_url", long = "nats_url", env = "NATS_URL")]
    pub url: Option<String>,

    #[arg(
        id = "nats_subject_prefix",
        long = "nats_subject_prefix",
        env = "NATS_SUBJECT_PREFIX",
        default_value = "indexer"
    )]
    pub subject_prefix: String,
}

#[derive(Parser)]
pub struct RedisConfig {
    #[arg(id = "redis_url", long = "redis_url", env = "REDIS_URL")]
    pub url: Option<String>,

    #[arg(
        id = "redis_channel_prefix",
        long = "redis_channel_prefix",
        env = "REDIS_CHANNEL_PREFIX",
        default_value = "indexer"
    )]
    pub channel_prefix: String,
}

#[derive(Parser)]
pub struct SocketIoConfig {
    #[arg(id = "socket_io_url", long = "socket_io_url", env = "SOCKET_IO_URL")]
    pub url: Option<SocketAddr>,
}

#[derive(Parser)]
pub struct TransportConfig {
    #[command(flatten)]
    pub amqp: AmqpConfig,

    #[command(flatten)]
    pub nats: NatsConfig,

    #[command(flatten)]
    pub redis: RedisConfig,

    #[command(flatten)]
    pub socketio: SocketIoConfig,
}

#[derive(Parser)]
pub struct AppConfig {
    #[command(flatten)]
    pub db_config: DbConfig,

    #[command(flatten)]
    pub indexer: IndexerConfig,

    #[command(flatten)]
    pub sequential: Sequential,

    #[command(flatten)]
    pub ingestion: Ingestion,

    #[command(flatten)]
    pub ingestion_client: IngestionClient,

    #[arg(
        id = "sui_network",
        long = "sui_network",
        env = "SUI_NETWORK",
        default_value = "testnet"
    )]
    pub network: Option<AppEnv>,

    #[arg(id = "packages", long = "packages", env = "PACKAGES", value_enum, default_values = ["app", "world"], value_delimiter = ',')]
    pub packages: Vec<Package>,

    #[arg(
        id = "metrics_address",
        long = "metrics_address",
        env = "METRICS_ADDRESS",
        default_value = "0.0.0.0:9184"
    )]
    pub metrics_address: SocketAddr,

    #[command(flatten)]
    pub transport_config: TransportConfig,

    #[command(flatten)]
    pub sandbox: SandboxArgs,
}

fn parse_remote_store_header(header: &str) -> Result<(HeaderName, HeaderValue), String> {
    let (name, value) = header
        .split_once(':')
        .ok_or_else(|| "remote store header must be in `<name>:<value>` format".to_string())?;

    let name = HeaderName::from_bytes(name.as_bytes())
        .map_err(|err| format!("invalid remote store header name `{name}`: {err}"))?;
    let value = HeaderValue::from_str(value)
        .map_err(|err| format!("invalid remote store header value for `{name}`: {err}"))?;

    Ok((name, value))
}
