# Data Retention

The indexer utilizes [TimescaleDB](https://www.timescale.com/) hypertables to store event data. Because the volume of blockchain events can be massive, it is important to consider data retention to prevent the database from growing indefinitely and consuming all available disk space.

## How Retention Works in TimescaleDB

TimescaleDB organizes data into "chunks" based on the time dimension (in this project, the `occurred_at` column). Retention is handled by dropping entire chunks that are older than a specified interval. This is significantly more efficient than running standard SQL `DELETE` statements, as it simply removes files from the disk.

## Adjusting Retention for the Indexer

To set up a retention policy, you must use the `add_retention_policy` function provided by TimescaleDB. This should be executed as a superuser or the database owner.

### Example: Setting a 30-Day Retention Policy

If you want to keep event data for 30 days and automatically drop anything older, run the following SQL command for each hypertable:

```sql
SELECT add_retention_policy('events_fuel_burning_updated', INTERVAL '30 days');
```

### Common Hypertables to Manage

All event tables in the `indexer` schema as well as the `killmails` table are hypertables partitioned by `occurred_at`. You should consider applying retention policies to the ones that grow the fastest and that you don't really need old historical data from, such as:

- `events_energy_production_started`
- `events_energy_production_stopped`
- `events_energy_released`
- `events_energy_reserved`
- `events_fuel_burning_started`
- `events_fuel_burning_stopped`
- `events_fuel_burning_updated`
- `events_fuel_deleted`
- `events_item_burned`
- `events_item_deposited`
- `events_item_destroyed`
- `events_item_minted`
- `events_item_withdrawn`
- `events_rift_location_broadcasted`
- `events_rift_mining_started`
- `events_rift_spawned`
- `killmails`

### Checking Current Policies

You can view all active retention policies in your database by querying the `timescaledb_information.jobs` view:

```sql
SELECT hypertable_name, config 
FROM timescaledb_information.jobs 
WHERE proc_name = 'policy_retention';
```

### Removing a Retention Policy

If you need to change the interval or remove the policy entirely, use the `remove_retention_policy` function:

```sql
SELECT remove_retention_policy('events_character_created');
```

## Recommendations

- **Cold Storage**: If you need to keep data for longer than 30-90 days but don't need it to be immediately queryable, consider using TimescaleDB's **compression** features before the retention policy kicks in.
- **Monitoring**: Regularly monitor your disk usage and the size of your hypertables to determine the optimal retention interval for your specific needs.
