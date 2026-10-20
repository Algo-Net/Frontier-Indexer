use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_rift_spawned;

#[derive(Deserialize)]
pub struct MoveRiftSpawned {
    pub rift_id: Address,
    pub rift_key: MoveTenantItemId,
    pub location_hash: Vec<u8>,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_rift_spawned)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredRiftSpawned {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub location_hash: String,
}

impl StoredRiftSpawned {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveRiftSpawned =
            bcs::from_bytes(&event.contents).context("Failed to deserialize Rift Spawned event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        let location_hash = format!("0x{:0>64}", hex::encode(&move_event.location_hash));

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.rift_id.to_hex(),
            item_id: move_event.rift_key.item_id.to_string(),
            tenant: move_event.rift_key.tenant,
            location_hash,
        })
    }
}
