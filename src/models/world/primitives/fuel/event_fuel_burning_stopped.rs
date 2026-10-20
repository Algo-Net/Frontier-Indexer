use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::Serialize;

use sui_indexer_alt_framework::FieldCount;

use crate::handlers::EventMeta;
use crate::models::world::MoveFuelEvent;
use crate::schema::events_fuel_burning_stopped;

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_fuel_burning_stopped)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredFuelBurningStopped {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub type_id: i64,
    pub quantity: i64,
    pub quantity_old: i64,
    pub burning: bool,
}

impl StoredFuelBurningStopped {
    pub fn from_event(event: &MoveFuelEvent, meta: &EventMeta) -> anyhow::Result<Self> {
        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: event.assembly_id.to_hex(),
            item_id: event.assembly_key.item_id.to_string(),
            type_id: event.type_id as i64,
            quantity: event.new_quantity as i64,
            quantity_old: event.old_quantity as i64,
            burning: event.is_burning,
        })
    }
}
