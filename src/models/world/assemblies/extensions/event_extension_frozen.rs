use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::schema::events_extension_frozen;

#[derive(Deserialize)]
pub struct MoveExtensionFrozen {
    pub assembly_id: Address,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_extension_frozen)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredExtensionFrozen {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
}

impl StoredExtensionFrozen {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveExtensionFrozen = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Extension Frozen event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.assembly_id.to_hex(),
        })
    }
}
