use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_rift_location_broadcasted;

#[derive(Deserialize)]
pub struct MoveRiftLocationBroadcasted {
    pub rift_id: Address,
    pub rift_key: MoveTenantItemId,
    pub location_hash: Vec<u8>,
    pub solarsystem: u64,
    pub x: String,
    pub y: String,
    pub z: String,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_rift_location_broadcasted)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredRiftLocationBroadcasted {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub location_hash: String,
    pub solar_system_id: String,
    pub x: String,
    pub y: String,
    pub z: String,
}

impl StoredRiftLocationBroadcasted {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveRiftLocationBroadcasted = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Rift Location Broadcasted event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        let location_hash = format!("0x{:0>64}", hex::encode(&move_event.location_hash));

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.rift_id.to_hex(),
            item_id: move_event.rift_key.item_id.to_string(),
            location_hash,
            solar_system_id: move_event.solarsystem.to_string(),
            x: move_event.x,
            y: move_event.y,
            z: move_event.z,
        })
    }
}
