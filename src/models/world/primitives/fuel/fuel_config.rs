use anyhow::Context;
use serde::{Deserialize, Serialize};

use diesel::prelude::*;

use sui_sdk_types::Address;

use sui_indexer_alt_framework::FieldCount;
use sui_types::collection_types::Table;
use sui_types::dynamic_field::Field;
use sui_types::object::Object;

use crate::schema::fuel_config;

#[derive(Deserialize)]
pub struct MoveFuelConfig {
    pub id: Address,
    pub fuel_efficiency: Table,
}

#[derive(Deserialize, Serialize, Insertable, Queryable, Selectable, Debug, Clone, FieldCount)]
#[diesel(table_name = fuel_config)]
pub struct StoredFuelConfig {
    pub table_id: String,
    pub type_id: i64,
    pub efficiency: i64,
    pub entry_object_id: String,
    pub checkpoint_updated: i64,
}

impl StoredFuelConfig {
    pub fn from_object(
        obj: &Object,
        table_id: String,
        checkpoint_updated: i64,
    ) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let entry: Field<u64, u64> =
            bcs::from_bytes(bytes).context("Failed to deserialize Fuel Config object")?;

        Ok(Self {
            table_id,
            type_id: entry.name as i64,
            efficiency: entry.value as i64,
            entry_object_id: obj.id().to_canonical_string(true),
            checkpoint_updated,
        })
    }
}
