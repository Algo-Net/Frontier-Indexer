use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_item_minted;

#[derive(Deserialize)]
pub struct MoveItemMinted {
    pub assembly_id: Address,
    pub assembly_key: MoveTenantItemId,
    pub character_id: Address,
    pub character_key: MoveTenantItemId,
    pub item_id: u64,
    pub type_id: u64,
    pub quantity: u32,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_item_minted)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredItemMinted {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub item_id: String,
    pub type_id: i64,
    pub quantity: i64,
    pub assembly_id: String,
    pub assembly_item_id: String,
    pub character_id: String,
    pub character_item_id: String,
}

impl StoredItemMinted {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveItemMinted =
            bcs::from_bytes(&event.contents).context("Failed to deserialize Item Minted event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            item_id: move_event.item_id.to_string(),
            type_id: move_event.type_id as i64,
            quantity: move_event.quantity as i64,
            assembly_id: move_event.assembly_id.to_hex(),
            assembly_item_id: move_event.assembly_key.item_id.to_string(),
            character_id: move_event.character_id.to_hex(),
            character_item_id: move_event.character_key.item_id.to_string(),
        })
    }
}
