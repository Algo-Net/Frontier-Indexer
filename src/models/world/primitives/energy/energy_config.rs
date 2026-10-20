use anyhow::Context;
use serde::{Deserialize, Serialize};

use diesel::prelude::*;

use sui_sdk_types::Address;

use sui_indexer_alt_framework::FieldCount;
use sui_types::collection_types::Table;
use sui_types::dynamic_field::Field;
use sui_types::object::Object;

use crate::schema::energy_config;

#[derive(Deserialize)]
pub struct MoveEnergyConfig {
    pub id: Address,
    pub assembly_energy: Table,
}
#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = energy_config)]
pub struct StoredEnergyConfig {
    pub table_id: String,
    pub type_id: i64,
    pub energy_cost: i64,
    pub entry_object_id: String,
    pub checkpoint_updated: i64,
}

impl StoredEnergyConfig {
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
            bcs::from_bytes(bytes).context("Failed to deserialize Energy config object")?;

        Ok(Self {
            table_id,
            type_id: entry.name as i64,
            energy_cost: entry.value as i64,
            entry_object_id: obj.id().to_canonical_string(true),
            checkpoint_updated,
        })
    }
}
