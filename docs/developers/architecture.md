# System Architecture

The indexer is designed to process Sui blockchain checkpoints and project the state into a relational PostgreSQL database. The indexer can also emit on-chain events and object updates using a variety of protocols such as AMQP, NATS, Redis Pub/Sub, etc.

## Framework
The project is built on top of [`sui-indexer-alt-framework`](https://github.com/MystenLabs/sui), which provides the core logic for:
- **Ingestion**: Fetching full checkpoint data (transactions, effects, events, object changes) from a remote Sui checkpoint store or gRPC stream.
- **Sequential Pipelines**: An ordered processing system that guarantees checkpoints are committed in sequence. This ensures the database always reflects a consistent, monotonically advancing state.
- **Checkpointing**: Tracking the last committed checkpoint so the indexer can resume exactly where it left off after a restart.

## Data Flow

```
Sui Checkpoint Store/Stream
  |
  v
Ingestion Client        Fetches full checkpoint content
  |
  v
Sequential Pipelines    Checkpoint content is broadcast to all registered Handlers
  |
  v
Handlers                Filters checkpoint data to relevant packages and transforms data for storage / emitting
  |
  v
Database                Batches of events and object updates are stored in PostgreSQL via Diesel
  |
  v
Event Emitter           Batches are emitted to registered transport protocols only after successful commit to the database.
```

1. **Ingestion Client**: Pulls checkpoint bundles from a remote store (e.g. `https://checkpoints.testnet.sui.io`), gRPC stream (e.g. `https://fullnode.testnet.sui.io:443`) or a local path.
2. **Sequential Pipelines**: Receives each checkpoint and fans it out to all registered handlers. Batches results and manages the commit cycle. Configured via the `Sequential` and `Ingestion` settings.
3. **Handlers**: Each handler implements a `Processor` (filtering and transformation) and a `Handler` (database commit) trait. See [World Contracts Integration](./world_contracts.md) for details on how handlers are structured.
4. **Database**: PostgreSQL, managed through Diesel. Schema is defined by migrations that run automatically at startup. See [Database and Models](./database.md).
5. **Event Emitter**: Each pipeline can include an optional event emitter. After a batch of data is successfully stored to the database, the batch is also passed to the emitter to send out using registered transport protocols. See [Emitters and Transports](./emitters-and-transports.md) for details on how emitters and transports work.

## Context

The `AppContext` struct (`src/lib.rs`) is constructed once at startup and shared (cloned) across all handlers. It holds:
- The current network environment (`Mainnet` / `Testnet`).
- The set of known world and app package addresses to index.
- Functions to help determine if transactions should be indexed or not.
- The `TableRegistry` — an in-memory cache of Move `Table` object IDs mapped to their parent structs, used to index table entries.
- The `FuelRegistry` — an in-memory cache to look up world fuel efficiency values.

## Monitoring
The system exposes Prometheus metrics via a dedicated `MetricsService` on `0.0.0.0:9184` by default, including database connection pool statistics.
