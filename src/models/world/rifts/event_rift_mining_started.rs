use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_rift_mining_started;

#[derive(Deserialize)]
pub struct MoveRiftMiningStarted {
    pub rift_type_id: MoveTenantItemId,
    pub character_id: Address,
    pub character_key: MoveTenantItemId,
    pub solarsystem: u64,
    pub x: String,
    pub y: String,
    pub z: String,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_rift_mining_started)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredRiftMiningStarted {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub item_id: String,
    pub character_id: String,
    pub solar_system_id: String,
    pub x: String,
    pub y: String,
    pub z: String,
}

impl StoredRiftMiningStarted {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveRiftMiningStarted = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Rift Mining Started event.")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            item_id: move_event.rift_type_id.item_id.to_string(),
            character_id: move_event.character_id.to_hex(),
            solar_system_id: move_event.solarsystem.to_string(),
            x: move_event.x,
            y: move_event.y,
            z: move_event.z,
        })
    }
}
