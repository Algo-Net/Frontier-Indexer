use anyhow::Context;
use clap::Parser;
use prometheus::Registry;
use std::num::NonZeroUsize;
use std::path::Path;
use url::Url;

use diesel_async::RunQueryDsl;
use diesel_migrations::{EmbeddedMigrations, embed_migrations};

use sui_indexer_alt_framework::ingestion::ingestion_client::IngestionClientArgs;
use sui_indexer_alt_framework::ingestion::streaming_client::StreamingClientArgs;
use sui_indexer_alt_framework::ingestion::{ClientArgs, IngestionConfig};
use sui_indexer_alt_framework::pipeline::sequential::SequentialConfig;
use sui_indexer_alt_framework::pipeline::{CommitterConfig, ConcurrencyConfig};
use sui_indexer_alt_framework::{Indexer, IndexerArgs};
use sui_indexer_alt_metrics::db::DbConnectionStatsCollector;
use sui_indexer_alt_metrics::{MetricsArgs, MetricsService};
use sui_pg_db::{Db, DbArgs};

use indexer::config::*;
use indexer::handlers::*;
use indexer::models::system::*;
use indexer::pipelines::*;
use indexer::transports::*;
use indexer::{AppContext, AppEnv};

const MIGRATIONS: EmbeddedMigrations = embed_migrations!("migrations");

#[tokio::main]
async fn main() -> Result<(), anyhow::Error> {
    dotenvy::dotenv().ok();

    let _guard = telemetry_subscribers::TelemetryConfig::new()
        .with_env()
        .init();

    let AppConfig {
        metrics_address,
        db_config,
        indexer,
        sequential,
        ingestion,
        ingestion_client,
        network,
        packages,
        transport_config,
        sandbox,
    } = AppConfig::parse();

    let DbConfig {
        db_user,
        db_password,
        db_host,
        db_port,
        db_name,
        db_schema,
        db_connection_pool_size,
        db_connection_timeout_ms,
        db_statement_timeout_ms,
        tls_verify_cert,
        tls_ca_cert_path,
    } = db_config;

    let mut database_url = Url::parse(&format!("postgres://{}:{}/{}", db_host, db_port, db_name))
        .context("Failed to construct base Database URL")?;

    database_url
        .set_username(&db_user)
        .map_err(|_| anyhow::anyhow!("Failed to set database username."))?;

    database_url
        .set_password(Some(&db_password))
        .map_err(|_| anyhow::anyhow!("Failed to set database password."))?;

    database_url
        .query_pairs_mut()
        .append_pair("options", &format!("-csearch_path={}", db_schema));

    let db_args = DbArgs {
        db_connection_pool_size,
        db_connection_timeout_ms,
        db_statement_timeout_ms,
        tls_verify_cert,
        tls_ca_cert_path,
    };

    let IndexerConfig {
        first_checkpoint,
        last_checkpoint,
        pipeline,
    } = indexer;

    let pipelines: Vec<String> = if !pipeline.is_empty() {
        pipeline
    } else {
        let config = PipelineConfig::from_file(Path::new("./pipelines.toml"))?;
        config.enabled_pipelines().unwrap_or_default()
    };

    let indexer_args = IndexerArgs {
        first_checkpoint,
        last_checkpoint,
        pipeline: pipelines,
        ..Default::default()
    };

    let Sequential {
        min_eager_rows,
        max_batch_checkpoints,
        processor_channel_size,
        write_concurrency,
        collect_interval_ms,
        watermark_interval_ms,
        watermark_interval_jitter_ms,
    } = sequential;

    let sequential = SequentialConfig {
        committer: CommitterConfig {
            write_concurrency,
            collect_interval_ms,
            watermark_interval_ms,
            watermark_interval_jitter_ms,
        },
        min_eager_rows,
        max_batch_checkpoints,
        processor_channel_size,
        ..Default::default()
    };

    let Ingestion {
        ingest_concurrency_max,
        retry_interval_ms,
        streaming_backoff_initial_batch_size,
        streaming_backoff_max_batch_size,
        streaming_connection_timeout_ms,
        streaming_statement_timeout_ms,
    } = ingestion;

    let ingestion = IngestionConfig {
        retry_interval_ms,
        streaming_backoff_initial_batch_size: NonZeroUsize::new(
            streaming_backoff_initial_batch_size,
        )
        .expect("Failed to parse streaming_backoff_initial_batch_size"),
        streaming_backoff_max_batch_size,
        streaming_connection_timeout_ms,
        streaming_statement_timeout_ms,
        ingest_concurrency: ConcurrencyConfig::Adaptive {
            initial: 1,
            min: 1,
            max: ingest_concurrency_max,
            dead_band: None,
        },
        ..Default::default()
    };

    let IngestionClient {
        ingestion_source,
        remote_store_url,
        remote_store_s3,
        remote_store_gcs,
        remote_store_azure,
        remote_store_headers,
        local_ingestion_path,
        rpc_api_url,
        rpc_username,
        rpc_password,
        checkpoint_timeout_ms,
        checkpoint_connection_timeout_ms,
    } = ingestion_client;

    let (env, ingestion_args, streaming_args, packages) = if sandbox.enabled {
        // Sandbox mode - override package addresses then pick ingestion source
        let has_world = !sandbox.world_packages.is_empty();

        indexer::sandbox::init_package_override(sandbox.app_package_ids, sandbox.world_packages);

        let app_env = match sandbox.env {
            SandboxEnv::Testnet | SandboxEnv::Localnet => AppEnv::Testnet,
        };

        let ingestion = match sandbox.env {
            SandboxEnv::Localnet => IngestionClientArgs {
                local_ingestion_path: Some(
                    sandbox
                        .local_ingestion_path
                        .context("SANDBOX_INGESTION_PATH is required for localnet")?,
                ),
                ..Default::default()
            },
            SandboxEnv::Testnet => IngestionClientArgs {
                remote_store_url: Some(
                    Url::parse(
                        remote_store_url
                            .unwrap_or(app_env.remote_store_url().to_string())
                            .as_str(),
                    )
                    .expect("invalid testnet remote_store_url"),
                ),
                ..Default::default()
            },
        };

        let mut packages = packages;

        if !has_world {
            packages.retain(|p| matches!(p, Package::App));
        }

        let streaming = StreamingClientArgs::default();

        (app_env, ingestion, streaming, packages)
    } else {
        let env = network.context("network is required when not using sandbox")?;
        let ingestion = match ingestion_source {
            IngestionSource::Store => IngestionClientArgs {
                remote_store_url: Some(
                    Url::parse(
                        remote_store_url
                            .unwrap_or(env.remote_store_url().to_string())
                            .as_str(),
                    )
                    .expect("Invalid remote_store_url"),
                ),
                remote_store_s3,
                remote_store_gcs,
                remote_store_azure,
                remote_store_headers,
                checkpoint_timeout_ms,
                checkpoint_connection_timeout_ms,
                ..Default::default()
            },
            IngestionSource::Fullnode => IngestionClientArgs {
                rpc_api_url: Some(rpc_api_url.unwrap_or(env.fullnode_url())),
                rpc_username,
                rpc_password,
                checkpoint_timeout_ms,
                checkpoint_connection_timeout_ms,
                ..Default::default()
            },
            IngestionSource::Local => IngestionClientArgs {
                local_ingestion_path,
                checkpoint_timeout_ms,
                checkpoint_connection_timeout_ms,
                ..Default::default()
            },
        };

        let streaming = StreamingClientArgs {
            streaming_url: Some(env.streaming_url()),
            ..StreamingClientArgs::default()
        };

        (env, ingestion, streaming, packages)
    };

    let registry = Registry::new_custom(Some("frontier".into()), None)
        .context("Failed to create Prometheus registry.")?;

    let metrics = MetricsService::new(MetricsArgs { metrics_address }, registry.clone());

    let store = Db::for_write(database_url, db_args)
        .await
        .context("Failed to connect to database")?;

    let mut conn = store
        .connect()
        .await
        .context("Failed to get connection for database setup")?;

    let schema_name = db_schema.replace('"', "\"\"");
    diesel::sql_query(format!("CREATE SCHEMA IF NOT EXISTS \"{schema_name}\""))
        .execute(&mut *conn)
        .await
        .context("Failed to create database schema")?;

    store
        .run_migrations(Some(&MIGRATIONS))
        .await
        .context("Failed to run pending migrations.")?;

    let table_registry = TableRegistry::load_from_db(&mut conn).await?;
    let fuel_registry = FuelRegistry::load_from_db(&mut conn).await?;

    let context = AppContext::new(env, table_registry, fuel_registry);

    registry.register(Box::new(DbConnectionStatsCollector::new(
        Some("frontier_indexer_db"),
        store.clone(),
    )))?;

    let client_args = ClientArgs {
        ingestion: ingestion_args,
        streaming: streaming_args,
    };

    // system::upload_address_aliases(&mut conn).await?;

    let mut indexer = Indexer::new(
        store.clone(),
        indexer_args,
        client_args,
        ingestion,
        None,
        metrics.registry(),
    )
    .await?;

    let transports = Transports::init(&transport_config)
        .await
        .context("Could not initialize transports")?;

    // Handler for address_alias always runs.
    indexer
        .sequential_pipeline(
            system::AddressAliasHandler::new(&context, transports.for_pipeline()),
            sequential.clone(),
        )
        .await?;

    // Handler for tx digests always runs.
    indexer
        .sequential_pipeline(
            system::TransactionDigestHandler::new(&context, transports.for_pipeline()),
            sequential.clone(),
        )
        .await?;

    // Register handlers based on selected packages
    for package in &packages {
        match package {
            #[rustfmt::skip]
            Package::App => {}

            #[rustfmt::skip]
            Package::World => {
                // Owner Caps
                indexer.sequential_pipeline(world::OwnerCapCreatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::OwnerCapHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::OwnerCapTransferredHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Assemblies
                indexer.sequential_pipeline(world::AssemblyCreatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::AssemblyHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Extensions
                indexer.sequential_pipeline(world::ExtensionFrozenHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Gates
                indexer.sequential_pipeline(world::GateConfigHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GateCreatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GateExtensionAuthorizedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GateExtensionRevokedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GateHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GateJumpedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GateLinkedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GatePermitHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GatePermitIssuedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::GateUnlinkedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Network Nodes
                indexer.sequential_pipeline(world::NetworkNodeCreatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::NetworkNodeHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Storage Units
                indexer.sequential_pipeline(world::StorageUnitCreatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::StorageUnitExtensionAuthorizedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::StorageUnitExtensionRevokedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::StorageUnitHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Turrets
                indexer.sequential_pipeline(world::TurretCreatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::TurretExtensionAuthorizedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::TurretExtensionRevokedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::TurretHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Characters
                indexer.sequential_pipeline(world::CharacterCreatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::CharacterHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Killmails
                indexer.sequential_pipeline(world::KillmailHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Energy
                indexer.sequential_pipeline(world::EnergyConfigHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::EnergyProductionStartedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::EnergyProductionStoppedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::EnergyReleasedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::EnergyReservedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Fuel
                indexer.sequential_pipeline(world::FuelBurningStartedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelBurningStoppedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelBurningUpdatedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelConfigHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelDeletedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelDepositedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelEfficiencyRemovedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelEfficiencySetHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::FuelWithdrawnHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Inventories
                indexer.sequential_pipeline(world::InventoryHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemBurnedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemDepositedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemDepositedV2Handler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemDestroyedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemMintedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemWithdrawnHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::ItemWithdrawnV2Handler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Locations
                indexer.sequential_pipeline(world::LocationRevealedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Status
                indexer.sequential_pipeline(world::StatusChangedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;

                // Rifts
                indexer.sequential_pipeline(world::RiftHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::RiftLocationBroadcastedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::RiftMiningStartedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
                indexer.sequential_pipeline(world::RiftSpawnedHandler::new(&context, transports.for_pipeline()), sequential.clone()).await?;
            }
        }
    }

    let s_indexer = indexer.run().await?;
    let s_metrics = metrics.run().await?;

    s_indexer.attach(s_metrics).main().await?;
    Ok(())
}
