# World Contracts Integration

The indexer tracks data from specific "World" smart contracts deployed on Sui.

## Package Management
The indexer maintains lists of known package addresses for different environments (`mainnet`, `testnet`). Because a smart contract can be upgraded and redeployed at a new address, each environment may have multiple package IDs for the same contract.
- **Hardcoded Addresses**: Found in `src/lib.rs` in the `TESTNET_WORLD_PACKAGES` and `MAINNET_WORLD_PACKAGES` constants. New package versions are added here as they are deployed.
- **Sandbox Overrides**: In sandbox mode, the hardcoded addresses are replaced at startup by whatever is passed via `SANDBOX_WORLD_PACKAGES`. See [Container Configuration](../users/configuration.md) for details.

## Filtering Logic
The `AppContext` struct (`src/lib.rs`) is passed to every handler and provides the filtering helpers they all use:

| Method | What it checks |
|---|---|
| `is_indexed_tx(tx, objects)` | Whether a transaction touches any known package — via input objects, changed objects, events, or function calls. Used to skip irrelevant transactions quickly. |
| `is_world_object(obj, module, struct)` | Whether an object's type matches a given module/struct name from a known world package address. |
| `is_world_event(event, module, name)` | Whether an event's type matches a given module/event name from a known world package address. |
| `is_world_struct(tag, module, struct)` | Whether a raw `TypeTag` matches a given module/struct from a known world package. Used when inspecting type parameters of dynamic field objects. |

*(Note: Equivalent `is_sui_object`, `is_sui_event`, and `is_sui_struct` helpers are also available on `AppContext` for inspecting standard Sui package objects and events.)*

## Handlers
Handlers are registered in `src/main.rs` and organised under `src/handlers/world/` into sub-modules that mirror the world contract structure:

```
src/handlers/world/
├── access/          # OwnerCap creation and transfer
├── assemblies/
│   ├── assemblies/  # Assembly objects
│   ├── extensions/  # Extension freeze events
│   ├── gates/       # Gate objects, config, links, jumps, permits
│   ├── network_nodes/
│   ├── storage_units/
│   └── turrets/
├── characters/
├── killmails/
├── primitives/
│   ├── energy/      # EnergyConfig tables, production events
│   ├── fuel/        # FuelConfig tables, burning events
│   ├── inventories/ # Inventory dynamic fields, item events
│   ├── locations/
│   └── status/
└── rifts/           # Rift objects, spawn/mine/location broadcast events
```

Each handler implements two traits from `sui-indexer-alt-framework`:
- **`Processor`** — the `process` method runs per checkpoint, filters relevant data, and returns a list of typed values.
- **`Handler`** — the `commit` method receives a batch of those values and writes them to the database.

Handlers fall into two broad categories based on what they watch:

### Object Handlers
Watch for changes to specific Move objects (created, mutated, or deleted) by checking the object's type with `is_world_object`. Examples: `AssemblyHandler`, `GateHandler`, `InventoryHandler`.

### Event Handlers
Watch for named events emitted by transactions using `is_world_event`. These are used for state transitions that don't persist in an object (e.g. a gate jump, an item deposit). Examples: `GateJumpedHandler`, `ItemDepositedHandler`, `EnergyProductionStartedHandler`.

Some handlers do both — they watch for an object change **and** register metadata about it (e.g. `FuelConfigHandler` indexes the config object and also registers its internal `Table` with the `TableRegistry` so its entries can be tracked).

## Parsing Move Data

The way raw BCS bytes are decoded depends on what kind of Move value the object represents. The codebase uses five distinct patterns:

| Pattern | When to use |
|---|---|
| Plain Move object | Regular named objects (`Assembly`, `Gate`, `Character`, etc.) |
| Event | State transitions only visible as emitted events |
| Dynamic field | Objects stored as dynamic fields on a parent (e.g. `Inventory` attached to an assembly) |
| Table + TableRegistry | `Table<K, V>` entries — each entry is a separate on-chain object; the registry connects them to their parent |
| Inline `VecMap` | `VecMap<K, V>` stored inside a parent object's BCS bytes (no separate objects) |

For code examples of each pattern, see [Parsing Move Objects](./parsing-move-objects.md).
