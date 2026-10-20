use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::schema::events_energy_production_stopped;

#[derive(Deserialize)]
pub struct MoveEnergyProductionStopped {
    pub energy_source_id: Address,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_energy_production_stopped)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredEnergyProductionStopped {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
}

impl StoredEnergyProductionStopped {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveEnergyProductionStopped = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Energy Production Started event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.energy_source_id.to_hex(),
        })
    }
}
