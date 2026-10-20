# Container Configuration
This guide describes how to configure the indexer container using environment variables. The indexer requires a [TimescaleDB](https://www.timescale.com/) database (`timescale/timescaledb-ha:pg17` is the recommended container).

If you are using the repository's local compose workflow, you can copy `.env.sample` to `.env` and start the deployment with `./scripts/compose.sh up -d`. If `.env` is missing, the helper script will also create it from `.env.sample` automatically. The compose file passes the same environment variables documented here and overrides `DB_HOST` to `db` so the indexer connects to the database service over the compose network.

## Database Configuration
These variables are used to connect the indexer to your TimescaleDB database.

| Variable                   | Description                                                                                                                              | Default     |
| -------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------- | ----------- |
| `DB_USER`                  | Database username                                                                                                                        | `postgres`  |
| `DB_PASSWORD`              | Database password                                                                                                                        | `postgres`  |
| `DB_HOST`                  | Database host address                                                                                                                    | `localhost` |
| `DB_PORT`                  | Database port                                                                                                                            | `5432`      |
| `DB_NAME`                  | Database name                                                                                                                            | `postgres`  |
| `DB_SCHEMA`                | Database schema to use (Note: This is required to prevent Diesel from contaminating the `public` schema)                                 | `indexer`   |
| `DB_CONNECTION_POOL_SIZE`  | Maximum number of concurrent database connections the indexer can use                                                                    | `100`       |
| `DB_CONNECTION_TIMEOUT_MS` | How long to wait when acquiring a connection from the pool before giving up, in milliseconds                                             | `60000`     |
| `DB_STATEMENT_TIMEOUT_MS`  | Maximum time a single database statement is allowed to run before being cancelled, in milliseconds. Useful for catching runaway queries. | (None)      |
| `DB_TLS_VERIFY_CERT`       | Whether to verify the database server's TLS certificate                                                                                  | `false`     |
| `DB_TLS_CA_CERT_PATH`      | Path to a custom TLS CA certificate file. Required when connecting to a database with a self-signed or private CA certificate.           | (None)      |

## General Settings

### Indexer
Control which network and data the indexer targets.

| Variable           | Description                                                                                                                                    | Default                                                   |
| ------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------- |
| `SUI_NETWORK`      | The Sui network to index (`mainnet` or `testnet`)                                                                                              | `testnet`                                                 |
| `PACKAGES`         | Comma-separated list of data groups to index. `world` indexes the EVE Frontier world contracts. `app` is reserved for custom application data. | `app,world`                                               |
| `FIRST_CHECKPOINT` | Start indexing from this checkpoint sequence number. Useful when backfilling data so the indexer doesnt start from 0.                          | (None — starts from 0 or last committed watermark if any) |
| `LAST_CHECKPOINT`  | Stop indexing after reaching this checkpoint sequence number.                                                                                  | (None — runs continuously)                                |
| `PIPELINES`        | Comma-separated list of pipeline names to run. When set, only the named pipelines are active and all others are skipped.                       | (None — all pipelines run)                                |

### Ingestion Client
Controls where the indexer fetches data from.

| Variable                           | Description                                                                                                                                                                                          | Default                                                                   |
| ---------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------- |
| `INGESTION_SOURCE`                 | Determines if checkpoint data is fetched from a checkpoint `store`, streamed from a `fullnode` gRPC url or ingested from `local` files                                                               | `store`                                                                   |
| `REMOTE_STORE_URL`                 | Url of the checkpoint store from which to backfill data, typically the `checkpoints.[network].sui.io` service provided by Sui.                                                                       | `https://checkpoints.[network].sui.io` if no other store variable is set. |
| `REMOTE_STORE_S3`                  | Fetch checkpoints from AWS S3. Provide the bucket name or endpoint-and-bucket. Also set the variables `AWS_ENDPOINT`, `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` and `AWS_DEFAULT_REGION` as well. | (None)                                                                    |
| `REMOTE_STORE_GCS`                 | Fetch checkpoints from Google Cloud Storage. Provide the bucket name. Also set the variables `GOOGLE_SERVICE_ACCOUNT_PATH` as well.                                                                  | (None)                                                                    |
| `REMOTE_STORE_AZURE`               | Fetch checkpoints from Azure Blob Storage. Provide the container name. Also set the variable `AZURE_STORAGE_ACCOUNT_NAME` and `AZURE_STORAGE_ACCESS_KEY` as well.                                    | (None)                                                                    |
| `REMOTE_STORE_HEADERS`             | Comma delimited list of headers to include in remote store requests as `<name>:<value>`                                                                                                              | (None)                                                                    |
| `RPC_API_URL`                      | Sui fullnode gRPC url to fetch checkpoints from.                                                                                                                                                     | (None)                                                                    |
| `RPC_USERNAME`                     | Optional username for the gRPC service.                                                                                                                                                              | (None)                                                                    |
| `RPC_PASSWORD`                     | Optional password for the gRPC service.                                                                                                                                                              | (None)                                                                    |
| `LOCAL_INGESTION_PATH`             | Path to a local ingestion directory.                                                                                                                                                                 | (None)                                                                    |
| `CHECKPOINT_TIMEOUT_MS`            | How long to wait for a checkpoint file to be downloaded (milliseconds). Set to 0 to disable the timeout.                                                                                             | `120000`                                                                  |
| `CHECKPOINT_CONNECTION_TIMEOUT_MS` | How long to wait while establishing a connection to the checkpoint store (milliseconds). Set to 0 to disable the timeout.                                                                            | `120000`                                                                  |

> [!Note]
> The ingestion source determines which of the values above are used. As an example, setting gRPC values while `store` is selected as the source will not use the gRPC values.
>
> Importantly only one remote store option will be used at a time in the order they are listed above.
> If `REMOTE_STORE_URL` is set then it will be used even if `REMOTE_STORE_AZURE` is also set.
> The only exception is that if none of the remote store values are set then the default checkpoint store will be used.
> Store Headers can be used with any of the other options.

### Event Emitting
The indexer has the ability to broadcast the data being stored in the database out to other systems using a select number of transport protocols. This section controls the configuration of these transport protocols

| Variable               | Description                                                                                                                                    | Default   |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- | --------- |
| `AMQP_URL`             | Url to AMQP compatible server such as RabbitMQ. This specific transport is activated when this value is assigned.                              | (None)    |
| `AMQP_EXCHANGE`        | Topic exchange to which all messages will be published.                                                                                        | `indexer` |
| `AMQP_POOL_SIZE`       | Number of connections in connection pool.                                                                                                      | `10`      |
| `NATS_URL`             | Url to NATS server. This specific transport is activated when this value is assigned.                                                          | (None)    |
| `NATS_SUBJECT_PREFIX`  | Value added to the front of topics.                                                                                                            | `indexer` |
| `REDIS_URL`            | Url to Redis server. This specific transport is activated when this value is assigned.                                                         | (None)    |
| `REDIS_CHANNEL_PREFIX` | Value added to the front of pub/sub channels                                                                                                   | `indexer` |
| `SOCKET_IO_URL`        | Starts a socket.io web server on the provided url for clients to connect to. This specific transport is activated when this value is assigned. | (None)    |

### Monitoring
Contains settings that provide logging or metrics about the state of the indexer.

| Variable          | Description                                            | Default        |
| ----------------- | ------------------------------------------------------ | -------------- |
| `METRICS_ADDRESS` | Address and port where Prometheus metrics are exposed. | `0.0.0.0:9184` |


### Sandbox Mode
Sandbox mode is for testing and development. It allows the indexer to run against custom package IDs instead of the hardcoded network addresses, and can source checkpoint data locally rather than from the network.

| Variable                 | Description                                                                                                                        | Default    |
| ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------- | ---------- |
| `SANDBOX_ENABLE`         | Enable sandbox mode. Requires `SANDBOX_APP_PACKAGES` to also be set.                                                               | `false`    |
| `SANDBOX_NETWORK`        | The network environment for sandbox mode (`localnet` or `testnet`).                                                                | `localnet` |
| `SANDBOX_APP_PACKAGES`   | Comma-separated list of App package IDs to track. Required when `SANDBOX=true`.                                                    | (None)     |
| `SANDBOX_WORLD_PACKAGES` | Comma-separated list of World package IDs to use instead of the hardcoded addresses. When omitted, the World pipeline is disabled. | (None)     |
| `SANDBOX_INGESTION_PATH` | Path to a local directory of checkpoint files. Required when `SANDBOX_NETWORK=localnet`.                                           | (None)     |

## Performance Tuning
These settings control how the indexer batches and commits data. The defaults are a reasonable starting point; adjust them if you are seeing lag or high database load.

### Sequential Pipeline
Controls how checkpoints are gathered and written to the database.

| Variable                       | Description                                                                                                                                        | Default |
| ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------- |
| `WRITE_CONCURRENCY`            | Number of pipeline committers that can write to the database simultaneously.                                                                       | `5`     |
| `COLLECT_INTERVAL_MS`          | How frequently the pipeline collects processed results into a batch, in milliseconds.                                                              | `500`   |
| `WATERMARK_INTERVAL_MS`        | How frequently the pipeline updates its progress watermark in the database, in milliseconds.                                                       | `500`   |
| `WATERMARK_INTERVAL_JITTER_MS` | Random jitter added to the watermark interval to spread out writes when running multiple indexer instances.                                        | `0`     |
| `MIN_EAGER_ROWS`               | Minimum number of rows in a batch before the pipeline will commit early without waiting for `COLLECT_INTERVAL_MS`.                                 | (None)  |
| `MAX_BATCH_CHECKPOINTS`        | Maximum number of checkpoints that can be grouped into a single commit batch.                                                                      | (None)  |
| `PROCESSOR_CHANNEL_SIZE`       | Size of the internal channel between the processor and committer stages. Increasing this allows more checkpoints to be processed ahead of commits. | (None)  |

### Ingestion
Controls how aggresively checkpoint data is fetched.

| Variable                               | Description                                                                       | Default |
| -------------------------------------- | --------------------------------------------------------------------------------- | ------- |
| `INGEST_CONCURRENCY_MAX`               | How many concurrent ingestion requests are allowed to run at any give time.       | `500`   |
| `RETRY_INTERVAL_MS`                    | How long to wait before retrying a failed checkpoint fetch, in milliseconds.      | `200`   |
| `STREAMING_BACKOFF_INITIAL_BATCH_SIZE` | Starting batch size for the streaming backoff strategy when fetching checkpoints. | `10`    |
| `STREAMING_BACKOFF_MAX_BATCH_SIZE`     | Maximum batch size the streaming backoff strategy will grow to.                   | `10000` |
| `STREAMING_CONNECTION_TIMEOUT_MS`      | Timeout for establishing a connection to the checkpoint stream, in milliseconds.  | `5000`  |
| `STREAMING_STATEMENT_TIMEOUT_MS`       | Timeout for individual requests made to the checkpoint stream, in milliseconds.   | `5000`  |
