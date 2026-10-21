# Parsing Move Objects

This guide walks through the different ways Move on-chain data is represented in Sui checkpoints, and how this indexer reads each one. The patterns here are used consistently throughout every handler in `src/handlers/world/`.

## Background
When the indexer receives a checkpoint it gets access to two sources of data:

- **Object changes** — every object that was created, mutated, or deleted in that checkpoint.
- **Events** — structured log entries emitted by Move functions during execution.

Both are delivered as raw BCS-encoded bytes. The indexer deserializes those bytes into Rust structs that mirror the original Move struct layout, then maps them into the database model types.

## Pattern 1: Plain Move Objects
Most world contract state lives in regular Move objects (e.g. `Assembly`, `Gate`, `Character`). These are straightforward to index:

**Step 1 — Define a `Move*` struct that mirrors the on-chain layout**

The field order and types must exactly match the Move struct definition. The struct derives `Deserialize` so that BCS can decode it.

```rust
// src/models/world/assemblies/assemblies/assemblies.rs

#[derive(Deserialize)]
pub struct MoveAssembly {
    pub id: Address,
    pub key: MoveTenantItemId,
    pub owner_cap_id: Address,
    pub type_id: u64,
    pub status: MoveAssemblyStatus,
    pub location: MoveLocation,
    pub energy_source_id: Option<Address>,
    pub metadata: Option<MoveMetadata>,
}
```

**Step 2 — Define a `Stored*` struct for the database row**

This is the Diesel model. Types are mapped to what PostgreSQL can store (e.g. `u64` → `i64`, `Address` → `String`). Each stored struct typically has a `checkpoint_updated` field at the end to ensure that only newer data overrides already indexed data.

```rust
#[derive(Insertable, Debug, Clone, FieldCount)]
#[diesel(table_name = assemblies)]
pub struct StoredAssembly {
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub type_id: i64,
    pub owner_cap_id: String,
    pub location: String,
    pub status: String,
    pub energy_source_id: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub checkpoint_updated: i64,
}
```

**Step 3 — Deserialize in `from_object`**

Along with defining a stored struct, a function is also added that is used to convert from the move struct to the stored struct.

```rust
impl StoredAssembly {
    pub fn from_object(obj: &Object, checkpoint_updated: i64) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;
        let bytes = move_obj.contents();

        // Decode the raw BCS bytes into the Move mirror struct.
        let assembly: MoveAssembly =
            bcs::from_bytes(bytes).context("Failed to deserialize Assembly object")?;

        // Handle any unique fields that don't map cleanly from the move struct to the stored struct.
        let location = format!("0x{:0>64}", hex::encode(&assembly.location.location_hash));

        let energy_source_id = assembly.energy_source_id.map(|source| source.to_hex());

        let (name, description, url) = assembly
            .metadata
            .map(|meta| (Some(meta.name), Some(meta.description), Some(meta.url)))
            .unwrap_or_default();

        // Return the fully populated stored struct.
        Ok(Self {
            id: assembly.id.to_hex(),
            item_id: assembly.key.item_id.to_string(),
            tenant: assembly.key.tenant,
            type_id: assembly.type_id as i64,
            owner_cap_id: assembly.owner_cap_id.to_hex(),
            location,
            status: assembly.status.status.as_ref().to_string(),
            energy_source_id,
            name,
            description,
            url,
            checkpoint_updated,
        })
    }
}
```

**Step 4 — Detect and process the object in the handler**

`AppContext` includes a collection of functions that can be used to determine if a transaction, object, table, etc. should be indexed. There should be plenty of example handlers in the world section showing how to structure a handler and use these functions:

```rust
// src/handlers/world/assemblies/assemblies/assembly_handler.rs

fn is_assembly(&self, obj: &Object) -> bool {
    self.ctx.is_world_object(obj, "assembly", "Assembly")
}
```

Inside the handler's `process` function, the handler iterates over every object change in every transaction and calls `from_object` when the check passes:

```rust
if self.is_assembly(obj) {
    let assembly = StoredAssembly::from_object(obj, checkpoint_updated)?;
    results.push(AssemblyAction::Upsert(assembly));
}
```

## Pattern 2: Events
Some state transitions are only visible through the events emitted by a transaction (e.g. a gate jump, an item deposit). The handler listens for a named event type rather than watching object changes.

**Step 1 — Mirror the Move event struct**

```rust
// src/models/world/primitives/inventories/event_item_deposited.rs

#[derive(Deserialize)]
pub struct MoveItemDeposited {
    pub assembly_id: Address,
    pub assembly_key: MoveTenantItemId,
    pub character_id: Address,
    pub character_key: MoveTenantItemId,
    pub item_id: u64,
    pub type_id: u64,
    pub quantity: u32,
}
```

**Step 2 — Deserialize in `from_event`**

Events carry their payload in `event.contents` as BCS bytes. `EventMeta` captures the surrounding transaction context (digest, timestamp, checkpoint number).

```rust
impl StoredItemDeposited {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveItemDeposited = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Item Deposited event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            item_id: move_event.item_id.to_string(),
            // ...
        })
    }
}
```

**Step 3 — Detect and process the event in the handler**

`AppContext` also includes a helper function to check if an event should be indexed.

```rust
// src/handlers/world/primitives/inventories/item_deposited_handler.rs

fn is_item_deposited(&self, event: &Event) -> bool {
    self.ctx.is_world_event(event, "inventory", "ItemDepositedEvent")
}
```

```rust
for (index, ev) in events.data.iter().enumerate() {
    if self.is_item_deposited(ev) {
        let meta = base_meta.with_index(index);
        results.push(StoredItemDeposited::from_event(ev, &meta)?);
    }
}
```

---

## Pattern 3: Dynamic Fields

In Sui, a dynamic field is not embedded inside its parent object — it exists as a **separate on-chain object** whose type is `0x2::dynamic_field::Field<K, V>`. The field is "owned" by the parent object.

This is used when the parent object stores an `Inventory` as a dynamic field attached to another object (e.g. an assembly). Each `Inventory` shows up in checkpoints as its own object change.

**Detecting a dynamic field object**

The indexer checks `move_type.is_dynamic_field()` and inspects the type parameters to confirm the key and value types match what is expected.

```rust
// src/handlers/world/primitives/inventories/inventory_handler.rs

fn is_inventory(&self, obj: &Object) -> bool {
    let Some(move_type) = obj.type_() else { return false; };

    // Must be a dynamic field with exactly two type params.
    if !move_type.is_dynamic_field() || move_type.type_params().len() != 2 {
        return false;
    }

    let type_params = move_type.type_params();

    // The value type must be the world's Inventory struct.
    if !self.ctx.is_world_struct(type_params[1].as_ref(), "inventory", "Inventory") {
        return false;
    }

    // The key type must be 0x2::object::ID.
    let TypeTag::Struct(s_tag) = type_params[0].as_ref() else { return false; };
    s_tag.address.to_hex_literal() == "0x2"
        && s_tag.module.as_str() == "object"
        && s_tag.name.as_str() == "ID"
}
```

**Deserializing with `Field<K, V>`**

The BCS layout of a dynamic field object wraps the actual data in a `Field` struct from `sui_types::dynamic_field`. The `name` field is the key and `value` is the value.

```rust
// src/models/world/primitives/inventories/inventories.rs

use sui_types::dynamic_field::Field;

// The on-chain type is Field<0x2::object::ID, Inventory>.
let inventory: Field<Address, MoveInventory> =
    bcs::from_bytes(bytes).expect("Failed to deserialize Inventory object");

// inventory.name is the key (the parent object's ID in this case).
// inventory.value is the actual Inventory data.
let capacity_max = inventory.value.max_capacity;
```

---

## Pattern 4: Tables and the Table Registry

A Move `Table<K, V>` stores each entry as its own dynamic field object, owned by the table object. The table object itself only holds the table's ID and length — the entries are separate objects in the checkpoint.

This creates a problem: when the indexer encounters a dynamic field object that belongs to a table, it needs to know which parent struct that table belongs to, and whether that parent is a world contract type. This is what the `TableRegistry` solves.

### How it works end-to-end

**1. The parent config object is seen first**

When a config object containing a `Table` field appears (e.g. `FuelConfig`), the handler deserializes it to extract the table's on-chain object ID.

```rust
// src/handlers/world/primitives/fuel/fuel_config_handler.rs

use sui_types::collection_types::Table;

// Mirror the on-chain FuelConfig struct.
// The fuel_efficiency field is a Table — its .id gives the table's object ID.
#[derive(Deserialize)]
pub struct MoveFuelConfig {
    pub id: Address,
    pub fuel_efficiency: Table,
}

let fuel_config: MoveFuelConfig = bcs::from_bytes(bytes)
    .expect("Failed to deserialize FuelConfig");

let table_id = fuel_config.fuel_efficiency.id.to_canonical_string(true);
```

**2. A `StoredTableRecord` is created and registered**

The handler builds a registry record that associates the table's object ID with its parent struct (module name, struct name, package ID) and the key/value types stored in it.

```rust
let table_record = StoredTableRecord {
    table_id: table_id.clone(),   // The Table object's on-chain ID
    parent_id: fuel_config.id.to_hex(),  // The FuelConfig object's ID
    package_id: tag.address.to_canonical_string(true),
    module_name: tag.module.to_string(),   // "fuel"
    struct_name: tag.name.to_string(),     // "FuelConfig"
    key_type: TypeTag::U64.to_string(),
    value_type: TypeTag::U64.to_string(),
    checkpoint_updated,
};
```

This is written to the `system_table_registry` database table and also added to the in-memory cache in `commit`:

```rust
self.ctx.tables.add_table(conn, table).await?;
```

**3. Table entries are matched against the registry**

When the handler encounters a dynamic field object that might be a table entry, it checks the object's owner (the table object ID) against the `TableRegistry`.

```rust
fn is_fuel_config_entry(
    &self,
    obj: &Object,
    // Also check tables registered in the same checkpoint batch.
    table_updates: &HashMap<String, Arc<StoredTableRecord>>,
) -> Option<Arc<StoredTableRecord>> {

    let Some(move_type) = obj.type_() else { return None; };

    // Must be a dynamic field with U64 key and U64 value.
    if !move_type.is_dynamic_field() || move_type.type_params().len() <= 1 { return None; }
    if !matches!(move_type.type_params()[0].as_ref(), TypeTag::U64) { return None; }
    if !matches!(move_type.type_params()[1].as_ref(), TypeTag::U64) { return None; }

    // The object must be owned by a table object.
    let Owner::ObjectOwner(owner_str) = obj.owner else { return None; };
    let owner_id = owner_str.to_string();

    // Check tables seen in the current checkpoint first (not yet in DB).
    if let Some(table) = table_updates.get(&owner_id) {
        return Some(table.clone());
    }

    // Then check the persistent registry loaded at startup.
    let Some(table) = self.ctx.tables.get_record(&owner_id) else { return None; };

    // Confirm the parent struct belongs to a known world package.
    let package_id = AccountAddress::from_str(&table.package_id).ok()?;
    if table.module_name != "fuel" { return None; }
    if table.struct_name != "FuelConfig" { return None; }
    if !self.ctx.world_packages.contains(&package_id) { return None; }

    Some(table)
}
```

**4. The table entry is deserialized as a `Field<K, V>`**

Once confirmed, the entry bytes are decoded as a `Field` with the matching key and value types.

```rust
// src/models/world/primitives/fuel/fuel_config.rs

use sui_types::dynamic_field::Field;

let entry: Field<u64, u64> =
    bcs::from_bytes(bytes).expect("Failed to deserialize Fuel Config entry");

// entry.name is the key (the type_id).
// entry.value is the value (the efficiency).
```

> **Why the in-memory check comes before the database check**
>
> A config object and its table entries can appear in the same checkpoint. The table entry object might be processed before the database `commit` has written the registry record. The `table_updates` `HashMap` acts as a within-checkpoint cache so entries are not missed.

---

## Pattern 5: Inline Collections (VecMap)

A Move `VecMap<K, V>` is stored *inline* within its parent object, not as separate dynamic field objects. The entire collection is encoded in the same BCS blob as the parent and can be iterated directly after deserialization.

```rust
// src/models/world/primitives/inventories/inventories.rs

use sui_types::collection_types::VecMap;

#[derive(Deserialize)]
pub struct MoveInventory {
    pub max_capacity: u64,
    pub used_capacity: u64,
    pub items: VecMap<u64, MoveItemEntry>,  // Inlined in the parent object.
}

// After deserialization, iterate the entries directly.
for entry in inventory.value.items.contents {
    let key: u64 = entry.key;
    let value: MoveItemEntry = entry.value;
}
```

This is different from a `Table` — no registry lookup is needed because the data is all in one object.
