use anyhow::Context;
use serde::{Deserialize, Serialize};

use diesel::prelude::*;

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::models::world::MoveLocation;
use crate::schema::items;

#[derive(Deserialize)]
pub struct MoveItem {
    pub id: Address,
    pub parent_id: Address,
    pub tenant: String,
    pub type_id: u64,
    pub item_id: u64,
    pub volume: u64,
    pub quantity: u32,
    pub location: MoveLocation,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = items)]
pub struct StoredItem {
    pub id: String,
    pub parent_id: String,
    pub location: String,
    pub item_id: String,
    pub type_id: i64,
    pub volume: i64,
    pub quantity: i64,
}

impl StoredItem {
    pub fn from_object(obj: &Object) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let item: MoveItem = bcs::from_bytes(bytes).context("Failed to deserialize Item object")?;

        let location = format!("0x{:0>64}", hex::encode(&item.location.location_hash));

        Ok(Self {
            id: item.id.to_hex(),
            parent_id: item.parent_id.to_hex(),
            location,
            item_id: item.item_id.to_string(),
            type_id: item.type_id as i64,
            volume: item.volume as i64,
            quantity: item.quantity as i64,
        })
    }
}
