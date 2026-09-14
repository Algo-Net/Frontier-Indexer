# Frontier Indexer
A custom [Sui](https://sui.io/) indexer for the EVE Frontier [world contracts](https://github.com/evefrontier/world-contracts). It processes Sui blockchain checkpoints and projects on-chain state into a PostgreSQL database, making it queryable for front-end services, analytics, and other downstream systems.

> [!NOTE]
> This is an unofficial, fan-made tool. It is not affiliated with, endorsed by, or associated with Fenris Creations or EVE Frontier.
> 
> This project is still under active development. Interfaces and configuration options may change between releases.

## Getting Started
If you are only looking for an indexer that covers the world contracts and dont intend to customize it in any way then the indexer is available as a docker container from [Github Container Registry.](https://github.com/Algo-Net/Frontier-Indexer/pkgs/container/frontier-indexer) Please see the [Getting Started](docs/users/getting-started.md) guide for an example of how it can be deployed.

## Quick Start (Local Deployment)
The easiest way to start the indexer is using the provided Docker Compose stack, which includes both the indexer and a TimescaleDB instance.

### 1. Clone the Repository
```sh
git clone https://github.com/Algo-Net/Frontier-Indexer.git
cd Frontier-Indexer
```

### 2. Configure Environment
Copy the sample environment file to create your local configuration:
```sh
cp .env.sample .env
```
You can edit `.env` to change database credentials, the target Sui network (`mainnet` or `testnet`), or the specific data packages you wish to index.

### 3. Launch the Stack
Use the provided helper script to start the containers:
```sh
./scripts/compose.sh up -d
```
The helper script auto-detects `docker compose`, `podman compose`, or `podman-compose`. To force a specific runtime, set `CONTAINER_RUNTIME`:

```sh
CONTAINER_RUNTIME=podman ./scripts/compose.sh up -d
CONTAINER_RUNTIME=podman-compose ./scripts/compose.sh up -d
```
The indexer will wait for the database to become ready and then automatically apply all necessary schema migrations.

### 4. Verify Installation
To validate the stack end-to-end, run the smoke test. It validates the compose configuration, builds the indexer image, waits for the database, checks the metrics endpoint on port `9184`, verifies the `indexer` schema exists, and then tears the stack down again.

```sh
./scripts/compose.sh smoke-test
```

The compose stack publishes PostgreSQL on port `5432` and Prometheus metrics on port `9184` for local inspection.

## Manual Container Setup
If you prefer to run the containers without using compose or the script, the equivalent manual flow still works. The recommended database image remains `docker.io/timescale/timescaledb-ha:pg17`:

```sh
docker network create frontier
```

```sh
docker run -d --network frontier \
   --name timescaledb \
   -e POSTGRES_USER=postgres \
   -e POSTGRES_PASSWORD=postgres \
   -e POSTGRES_DB=postgres \
   -p 5432:5432 \
   docker.io/timescale/timescaledb-ha:pg17
```

Then start the indexer container:

```sh
INDEXER_VERSION=$(curl -s https://api.github.com/repos/Algo-Net/Frontier-Indexer/releases/latest | grep '"tag_name"' | sed 's/.*"tag_name": *"\(.*\)".*/\1/')

docker run --rm --network frontier \
  --name frontier-indexer \
  -e DB_HOST=timescaledb \
  -e DB_NAME=postgres \
  -e DB_USER=postgres \
  -e DB_PASSWORD=postgres \
  -e DB_SCHEMA=indexer \
  -e SUI_NETWORK=testnet \
  ghcr.io/algo-net/frontier-indexer:$INDEXER_VERSION
```

All behaviour is controlled through environment variables. See [Container Configuration](docs/users/configuration.md) for the full list of available options.

## Development
The indexer is designed to be extended with application-specific additions for your own smart contracts. The `PACKAGES` environment variable controls which package groups are indexed (`world`, `app`, or both). Application-specific additions go in the `App` sections of the project to allow for clean updates as the world contracts change over time.

For a full explanation of the handler system, package filtering, and how the indexer interacts with the world contracts, see the [Developer Documentation](docs/developers/):

- [Architecture Overview](docs/developers/architecture.md)
- [World Contracts Integration](docs/developers/world_contracts.md)
- [Database and Models](docs/developers/database.md)
- [Emitters and Transports](docs/developers/emitters-and-transports.md)

## Contributing
Contributions to the project are welcomed as long as they stay within the scope of indexing official EVE Frontier data sources into a unified dataset or improving the implementation of this indexer. If you are unsure if your additions or improvements fit this criteria please create an issue or reach out to `Ocky` in the official [EVE Frontier discord.](https://discord.com/channels/1021714190102175754/1491364634920489003)

### Prerequisites
- [Rust](https://www.rust-lang.org/tools/install) (version specified in `Dockerfile`, currently `1.90.0`)
- [libpq-dev](https://www.postgresql.org/download/) (PostgreSQL client library, required to compile the `diesel` postgres driver — install via `apt-get install libpq-dev` or equivalent)
- [TimescaleDB](https://docs.timescale.com/self-hosted/latest/install/) (`timescale/timescaledb-ha:pg17` is the recommended container)
- [Diesel CLI](https://diesel.rs/guides/getting-started)

### Running Locally
1. Clone the repository:

   ```sh
   git clone https://github.com/Algo-Net/Frontier-Indexer.git
   cd Frontier-Indexer
   ```

2. Copy your environment config (or export the variables directly):

   ```sh
   cp .env.sample .env
   # edit .env with your database credentials and network settings
   ```

3. Run the indexer:

   ```sh
   cargo run
   ```

For information on adding database migrations, see [Database and Models](docs/developers/database.md#adding-a-new-migration).
