use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveStatus;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_gate_created;

#[derive(Deserialize)]
pub struct MoveGateCreated {
    pub assembly_id: Address,
    pub assembly_key: MoveTenantItemId,
    pub owner_cap_id: Address,
    pub type_id: u64,
    pub location_hash: Vec<u8>,
    pub status: MoveStatus,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_gate_created)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredGateCreated {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub type_id: i64,
    pub owner_cap_id: String,
}

impl StoredGateCreated {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveGateCreated =
            bcs::from_bytes(&event.contents).context("Failed to deserialize Gate Created event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.assembly_id.to_hex(),
            item_id: move_event.assembly_key.item_id.to_string(),
            tenant: move_event.assembly_key.tenant,
            type_id: move_event.type_id as i64,
            owner_cap_id: move_event.owner_cap_id.to_hex(),
        })
    }
}
