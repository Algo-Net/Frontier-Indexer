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
use crate::schema::events_turret_extension_authorized;

#[derive(Deserialize)]
pub struct MoveTurretExtensionAuthorized {
    pub assembly_id: Address,
    pub assembly_key: MoveTenantItemId,
    pub extension_type: MoveTypeName,
    pub previous_extension: Option<MoveTypeName>,
    pub owner_cap_id: Address,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_turret_extension_authorized)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredTurretExtensionAuthorized {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub item_id: String,
    pub package_id: String,
    pub module_name: String,
    pub struct_name: String,
    pub package_id_old: Option<String>,
    pub module_name_old: Option<String>,
    pub struct_name_old: Option<String>,
}

impl StoredTurretExtensionAuthorized {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveTurretExtensionAuthorized = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Turret Extension Authorized event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        let (package_id, module_name, struct_name) = move_event.extension_type.to_components()?;

        let (package_id_old, module_name_old, struct_name_old) = match move_event.previous_extension
        {
            Some(extension) => {
                let (package_id, module_name, struct_name) = extension.to_components()?;
                (Some(package_id), Some(module_name), Some(struct_name))
            }
            None => (None, None, None),
        };

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.assembly_id.to_hex(),
            item_id: move_event.assembly_key.item_id.to_string(),
            package_id,
            module_name,
            struct_name,
            package_id_old,
            module_name_old,
            struct_name_old,
        })
    }
}
