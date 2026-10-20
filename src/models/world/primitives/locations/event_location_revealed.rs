use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_location_revealed;

#[derive(Deserialize)]
pub struct MoveLocationRevealed {
    pub assembly_id: Address,
    pub assembly_key: MoveTenantItemId,
    pub type_id: u64,
    pub owner_cap_id: Address,
    pub location_hash: Vec<u8>,
    pub solarsystem: u64,
    pub x: String,
    pub y: String,
    pub z: String,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_location_revealed)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredLocationRevealed {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub type_id: i64,
    pub owner_cap_id: String,
    pub location_hash: String,
    pub solar_system_id: String,
    pub x: String,
    pub y: String,
    pub z: String,
}

impl StoredLocationRevealed {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveLocationRevealed = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Location Revealed event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        let location_hash = format!("0x{:0>64}", hex::encode(&move_event.location_hash));

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.assembly_id.to_hex(),
            item_id: move_event.assembly_key.item_id.to_string(),
            type_id: move_event.type_id as i64,
            owner_cap_id: move_event.owner_cap_id.to_hex(),
            location_hash,
            solar_system_id: move_event.solarsystem.to_string(),
            x: move_event.x,
            y: move_event.y,
            z: move_event.z,
        })
    }
}
