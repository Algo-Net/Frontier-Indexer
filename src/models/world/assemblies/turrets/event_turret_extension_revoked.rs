use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::event::Event;

use crate::handlers::EventMeta;
use crate::models::MoveTypeName;
use crate::models::world::MoveTenantItemId;
use crate::schema::events_turret_extension_revoked;

#[derive(Deserialize)]
pub struct MoveTurretExtensionRevoked {
    pub assembly_id: Address,
    pub assembly_key: MoveTenantItemId,
    pub revoked_extension: MoveTypeName,
    pub owner_cap_id: Address,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_turret_extension_revoked)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredTurretExtensionRevoked {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub package_id: String,
    pub module_name: String,
    pub struct_name: String,
}

impl StoredTurretExtensionRevoked {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveTurretExtensionRevoked = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Turret Extension Revoked event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        let (package_id, module_name, struct_name) =
            move_event.revoked_extension.to_components()?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.assembly_id.to_hex(),
            item_id: move_event.assembly_key.item_id.to_string(),
            package_id,
            module_name,
            struct_name,
        })
    }
}
