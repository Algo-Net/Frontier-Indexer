use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveAction;
use crate::models::world::MoveStatus;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_status_changed;

#[derive(Deserialize)]
pub struct MoveStatusChanged {
    pub assembly_id: Address,
    pub assembly_key: MoveTenantItemId,
    pub status: MoveStatus,
    pub action: MoveAction,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_status_changed)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredStatusChanged {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub status: String,
    pub action: String,
}

impl StoredStatusChanged {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveStatusChanged = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Status Changed event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.assembly_id.to_hex(),
            item_id: move_event.assembly_key.item_id.to_string(),
            status: move_event.status.as_ref().to_string(),
            action: move_event.action.as_ref().to_string(),
        })
    }
}
