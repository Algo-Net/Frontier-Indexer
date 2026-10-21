# Database and Models

The indexer uses PostgreSQL for storage and Diesel as the ORM/Query builder.

## Migrations
Database schema evolutions are handled via Diesel migrations.
- **Location**: All migration scripts are located in the `migrations/` directory.
- **Application**: Migrations are automatically run at startup in `main.rs` using `embed_migrations!`.

Each migration consists of an `up.sql` (to apply changes) and a `down.sql` (to revert changes).

### Adding a New Migration

The project uses the [Diesel CLI](https://diesel.rs/guides/getting-started) to manage migrations. Make sure it is installed before proceeding.

1. Set the database URL:
   ```sh
   PSQL_URL=postgres://[user]:[password]@[host]:5432/[database]?options=-csearch_path%3D[schema]
   ```
> [!NOTE]
> It is important to include the `search_path` in the connection string if you want everything contained in the specified schema. Without it, Diesel will put internal functions and tables in the `public` schema and it becomes much harder to cleanly manage everything related to the indexer.

2. Verify the connection works:
   ```sh
   psql $PSQL_URL -c "SELECT 'Connected';"
   ```
   Expected output:
   ```
    ?column?
   -----------
    Connected
   (1 row)
   ```

3. Configure the Diesel CLI:
   ```sh
   diesel setup --database-url $PSQL_URL
   ```

4. Generate the migration (choose a descriptive name):
   ```sh
   diesel migration generate <name_here>
   ```
   This creates a new directory under `migrations/` with blank `up.sql` and `down.sql` files for you to fill in.

5. Apply the migration:
   ```sh
   diesel migration run --database-url $PSQL_URL
   ```

6. If `src/schema.rs` does not update automatically after running the migration, regenerate it manually:
   ```sh
   diesel print-schema --database-url $PSQL_URL > src/schema.rs
   ```

## Models
The data models are defined in `src/models/`. They are split into:
- **App Models** (`src/models/app/`): Contains application-specific object models. If you are indexing your own application data alongside the world data, define your models here.
- **World Models** (`src/models/world/`): Contains models related to the state of the world contracts (re-exported via `src/models/world.rs`).
- **System Models** (`src/models/system/`): Contains internal indexer models or models used to index the world contracts (re-exported via `src/models/system.rs`).

## Registries
The system uses "Registries" to load configuration from the database at startup:
- `TableRegistry`: Tracks which objects contain tables that should be indexed.
- `FuelRegistry`: Cached fuel efficiency values used to calculate remaining fuel data faster.

Registries are loaded in `main.rs` and passed into the `AppContext`, allowing handlers to access this data while parsing checkpoints.
