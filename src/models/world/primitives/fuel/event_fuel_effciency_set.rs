use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::schema::events_fuel_efficiency_set;

#[derive(Deserialize)]
pub struct MoveFuelEfficiencySet {
    pub fuel_type_id: u64,
    pub efficiency: u64,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_fuel_efficiency_set)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredFuelEfficiencySet {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub type_id: i64,
    pub efficiency: i64,
}

impl StoredFuelEfficiencySet {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveFuelEfficiencySet = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Fuel Efficiency Set event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            type_id: move_event.fuel_type_id as i64,
            efficiency: move_event.efficiency as i64,
        })
    }
}
