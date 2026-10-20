use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::schema::events_energy_reserved;

#[derive(Deserialize)]
pub struct MoveEnergyReserved {
    pub energy_source_id: Address,
    pub assembly_type_id: u64,
    pub energy_reserved: u64,
    pub total_reserved_energy: u64,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_energy_reserved)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredEnergyReserved {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub type_id: i64,
    pub reserved: i64,
    pub reserved_total: i64,
}

impl StoredEnergyReserved {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveEnergyReserved = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Energy Reserved event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.energy_source_id.to_hex(),
            type_id: move_event.assembly_type_id as i64,
            reserved: move_event.energy_reserved as i64,
            reserved_total: move_event.total_reserved_energy as i64,
        })
    }
}
