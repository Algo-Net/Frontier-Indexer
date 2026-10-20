use anyhow::Context;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::models::world::MoveLocation;
use crate::models::world::MoveTenantItemId;
use crate::schema::rifts;

#[derive(Deserialize)]
pub struct MoveRift {
    pub id: Address,
    pub key: MoveTenantItemId,
    pub location: MoveLocation,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = rifts)]
pub struct StoredRift {
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub location: String,
    pub checkpoint_updated: i64,
}

impl StoredRift {
    pub fn from_object(obj: &Object, checkpoint_updated: i64) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let rift: MoveRift = bcs::from_bytes(bytes).context("Failed to deserialize Rift object")?;

        let location = format!("0x{:0>64}", hex::encode(&rift.location.location_hash));

        Ok(Self {
            id: rift.id.to_hex(),
            item_id: rift.key.item_id.to_string(),
            tenant: rift.key.tenant,
            location,
            checkpoint_updated,
        })
    }
}
