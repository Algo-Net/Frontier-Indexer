use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::schema::events_energy_released;

#[derive(Deserialize)]
pub struct MoveEnergyReleased {
    pub energy_source_id: Address,
    pub assembly_type_id: u64,
    pub energy_released: u64,
    pub total_reserved_energy: u64,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_energy_released)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredEnergyReleased {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub type_id: i64,
    pub released: i64,
    pub reserved_total: i64,
}

impl StoredEnergyReleased {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveEnergyReleased = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Energy Released event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.energy_source_id.to_hex(),
            type_id: move_event.assembly_type_id as i64,
            released: move_event.energy_released as i64,
            reserved_total: move_event.total_reserved_energy as i64,
        })
    }
}
