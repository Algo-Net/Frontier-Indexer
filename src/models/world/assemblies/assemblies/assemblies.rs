use anyhow::Context;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::models::world::MoveAssemblyStatus;
use crate::models::world::MoveLocation;
use crate::models::world::MoveMetadata;
use crate::models::world::MoveTenantItemId;
use crate::schema::assemblies;

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

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
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

impl StoredAssembly {
    pub fn from_object(obj: &Object, checkpoint_updated: i64) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let assembly: MoveAssembly =
            bcs::from_bytes(bytes).context("Failed to deserialize Assembly object")?;

        let location = format!("0x{:0>64}", hex::encode(&assembly.location.location_hash));

        let energy_source_id = assembly.energy_source_id.map(|source| source.to_hex());

        let (name, description, url) = assembly
            .metadata
            .map(|meta| (Some(meta.name), Some(meta.description), Some(meta.url)))
            .unwrap_or_default();

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
