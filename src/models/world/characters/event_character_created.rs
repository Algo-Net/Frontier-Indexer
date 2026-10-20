use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_character_created;

#[derive(Deserialize)]
pub struct MoveCharacterCreated {
    pub character_id: Address,
    pub key: MoveTenantItemId,
    pub tribe_id: u32,
    pub character_address: Address,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_character_created)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredCharacterCreated {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub owner_address: String,
    pub tribe_id: String,
}

impl StoredCharacterCreated {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveCharacterCreated = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Character Created event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.character_id.to_hex(),
            item_id: move_event.key.item_id.to_string(),
            tenant: move_event.key.tenant,
            owner_address: move_event.character_address.to_hex(),
            tribe_id: move_event.tribe_id.to_string(),
        })
    }
}
