use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_turret_created;

#[derive(Deserialize)]
pub struct MoveTurretCreated {
    pub turret_id: Address,
    pub turret_key: MoveTenantItemId,
    pub owner_cap_id: Address,
    pub type_id: u64,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_turret_created)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredTurretCreated {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub type_id: i64,
    pub owner_cap_id: String,
}

impl StoredTurretCreated {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveTurretCreated = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Turret Created event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.turret_id.to_hex(),
            item_id: move_event.turret_key.item_id.to_string(),
            tenant: move_event.turret_key.tenant,
            type_id: move_event.type_id as i64,
            owner_cap_id: move_event.owner_cap_id.to_hex(),
        })
    }
}
