# Getting Started
The Frontier Indexer is designed to be easy to deploy using Docker. This guide will walk you through the quickest way to get the indexer running by running it as a container.

## Deployment via Container Registry
If you don't intend to customize the indexer and just want to run it, you can pull the official image from the [GitHub Container Registry](https://github.com/Algo-Net/Frontier-Indexer/pkgs/container/frontier-indexer).

### Example Docker Compose Deployment

Create a `docker-compose.yml` file with the following content:

```yaml
services:
  postgresql:
    image: timescale/timescaledb-ha:pg17
    volumes:
      - ./postgresql:/home/postgres/pgdata/data
    environment:
      - POSTGRES_PASSWORD=postgres
    user: '$UID:$GID'
    restart: on-failure

  pgadmin:
    image: dpage/pgadmin4:latest
    ports:
      - 8080:80
    volumes:
      - ./pgadmin:/var/lib/pgadmin
    environment:
      - PGADMIN_DEFAULT_EMAIL=admin@domain.com
      - PGADMIN_DEFAULT_PASSWORD=admin
    user: '$UID:$GID'
    restart: on-failure

  indexer:
    image: ghcr.io/algo-net/frontier-indexer:latest
    ports:
      - 9184:9184
    environment:
      SUI_NETWORK: testnet
      FIRST_CHECKPOINT: 308265845

      DB_USER: postgres
      DB_PASSWORD: postgres
      DB_HOST: postgresql
      DB_PORT: 5432
      DB_NAME: postgres
      DB_SCHEMA: indexer
      DB_CONNECTION_POOL_SIZE: 10

    volumes:
      - ./indexer/pipelines.toml:/opt/indexer/bin/pipelines.toml
    restart: on-failure

  prometheus:
    image: prom/prometheus:latest
    ports:
      - 9090:9090
    volumes:
      - ./prometheus/prometheus.yml:/etc/prometheus/prometheus.yml
      - ./prometheus/data/:/prometheus
    command:
      - '--config.file=/etc/prometheus/prometheus.yml'
      - '--storage.tsdb.path=/prometheus'

  grafana:
    image: grafana/grafana:latest
    ports:
      - 8081:3000
    environment:
      - GF_SECURITY_ADMIN_PASSWORD=Grafan@321
    depends_on:
      - prometheus
```

This file will deploy a collection of containers and create directoryies in the same location as this file for the containers that require storge space:
- TimescaleDB container with a `postgresql` directory to store database.
- PgAdmin container with a `pgadmin` directory to store user information. User as the administrative tool for the database and can be access at `http://localhost:8080`
- Indexer container with a `indexer` directory. This direcctory contains the `pipelines.toml` file which can be used to enable or disable any of the pipeline in the indexer.
- Prometheus container with a `prometheus` directory to store configuration and data.
- Grafana container used to query and visualize the prometheus data. Can be accessed at `http://localhost:8081`

If you already have your own administrative or monitoring solutions you can omit those entries from the file or replacce it with other solutions you might prefer.

Run it with:
```sh
docker compose up -d
```

### Configuring Prometheus
The above exmaple deploys a prometheus container as part of the stack. In order for prometheus to collect data from the indexer it requires a configuration file.
The file will be located at `/prometheus/prometheus.yml` and a simple example configuration looks as follows:

``` yaml
# prometheus.yml
global:
  scrape_interval: 30s

scrape_configs:
  - job_name: 'frontier-indexer'
    static_configs:
      - targets: ['indexer:9184'] # Change to your indexer host and port
```
Once in place you can restart the prometheus container and it should start recording telemetry for the indexer.

## Next Steps

- **Configuration**: To customize how the indexer behaves, see the [Container Configuration](configuration.md) guide.
- **Developer Docs**: If you want to extend the indexer with your own smart contract handlers, visit the [Developer Documentation](../developers/).
