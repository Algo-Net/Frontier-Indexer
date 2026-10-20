use anyhow::Context;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::collection_types::VecMap;
use sui_types::dynamic_field::Field;
use sui_types::object::Object;

use crate::models::world::MoveItemEntry;
use crate::models::world::StoredInventoryEntry;
use crate::schema::inventories;

#[derive(Deserialize)]
pub struct MoveInventory {
    pub max_capacity: u64,
    pub used_capacity: u64,
    pub items: VecMap<u64, MoveItemEntry>,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = inventories)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredInventory {
    pub id: String,
    pub parent_id: String,
    pub capacity_used: i64,
    pub capacity_max: i64,
    pub checkpoint_updated: i64,

    #[diesel(skip_insertion)]
    pub entries: HashMap<i64, StoredInventoryEntry>,
}

impl StoredInventory {
    pub fn from_object(
        obj: &Object,
        parent_id: String,
        checkpoint_updated: i64,
    ) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let inventory: Field<Address, MoveInventory> =
            bcs::from_bytes(bytes).context("Failed to deserialize Inventory object")?;

        let inventory_id = obj.id().to_canonical_string(true);

        let entries: HashMap<i64, StoredInventoryEntry> = inventory
            .value
            .items
            .contents
            .into_iter()
            .map(|entry| {
                (
                    entry.key as i64,
                    StoredInventoryEntry::from_object(
                        &entry.value,
                        inventory_id.clone(),
                        checkpoint_updated,
                    ),
                )
            })
            .collect();

        Ok(Self {
            id: obj.id().to_canonical_string(true),
            parent_id,
            capacity_max: inventory.value.max_capacity as i64,
            capacity_used: inventory.value.used_capacity as i64,
            checkpoint_updated,
            entries,
        })
    }
}
