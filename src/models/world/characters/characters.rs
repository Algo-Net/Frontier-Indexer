use anyhow::Context;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::models::world::MoveMetadata;
use crate::models::world::MoveTenantItemId;
use crate::schema::characters;

#[derive(Deserialize)]
pub struct MoveCharacter {
    pub id: Address,
    pub key: MoveTenantItemId,
    pub tribe_id: u32,
    pub character_address: Address,
    pub metadata: Option<MoveMetadata>,
    pub owner_cap_id: Address,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = characters)]
pub struct StoredCharacter {
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub owner_cap_id: String,
    pub owner_address: String,
    pub tribe_id: String,
    pub name: String,
    pub description: Option<String>,
    pub url: Option<String>,
    pub checkpoint_updated: i64,
}

impl StoredCharacter {
    pub fn from_object(obj: &Object, checkpoint_updated: i64) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let character: MoveCharacter =
            bcs::from_bytes(bytes).context("Failed to deserialize Character object")?;

        let (name, description, url) = character
            .metadata
            .map(|meta| (meta.name, Some(meta.description), Some(meta.url)))
            .unwrap_or(("Unknown".to_string(), None, None));

        Ok(Self {
            id: character.id.to_hex(),
            item_id: character.key.item_id.to_string(),
            tenant: character.key.tenant,
            owner_cap_id: character.owner_cap_id.to_hex(),
            owner_address: character.character_address.to_hex(),
            tribe_id: character.tribe_id.to_string(),
            name,
            description,
            url,
            checkpoint_updated,
        })
    }
}
