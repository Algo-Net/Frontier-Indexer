use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_gate_unlinked;

#[derive(Deserialize)]
pub struct MoveGateUnlinked {
    pub source_gate_id: Address,
    pub source_gate_key: MoveTenantItemId,
    pub destination_gate_id: Address,
    pub destination_gate_key: MoveTenantItemId,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_gate_unlinked)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredGateUnlinked {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub departure_id: String,
    pub departure_item_id: String,
    pub destination_id: String,
    pub destination_item_id: String,
}

impl StoredGateUnlinked {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveGateUnlinked = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Gate Unlinked event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            departure_id: move_event.source_gate_id.to_hex(),
            departure_item_id: move_event.source_gate_key.item_id.to_string(),
            destination_id: move_event.destination_gate_id.to_hex(),
            destination_item_id: move_event.destination_gate_key.item_id.to_string(),
        })
    }
}
