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
use crate::schema::events_gate_permit_issued;

#[derive(Deserialize)]
pub struct MoveGatePermitIssued {
    pub jump_permit_id: Address,
    pub source_gate_id: Address,
    pub source_gate_key: MoveTenantItemId,
    pub destination_gate_id: Address,
    pub destination_gate_key: MoveTenantItemId,
    pub character_id: Address,
    pub character_key: MoveTenantItemId,
    pub route_hash: Vec<u8>,
    pub expires_at_timestamp_ms: u64,
    pub extension_type: MoveTypeName,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = events_gate_permit_issued)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredGatePermitIssued {
    pub event_id: String,
    pub occurred_at: DateTime<Utc>,
    pub id: String,
    pub character_id: String,
    pub character_item_id: String,
    pub departure_id: String,
    pub departure_item_id: String,
    pub destination_id: String,
    pub destination_item_id: String,
    pub link_hash: String,
    pub package_id: String,
    pub module_name: String,
    pub struct_name: String,
    pub expires_at: DateTime<Utc>,
}

impl StoredGatePermitIssued {
    pub fn from_event(event: &Event, meta: &EventMeta) -> anyhow::Result<Self> {
        let move_event: MoveGatePermitIssued = bcs::from_bytes(&event.contents)
            .context("Failed to deserialize Gate Permit Issued event")?;

        let occurred_at = DateTime::from_timestamp_millis(meta.checkpoint_timestamp_ms())
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        let link_hash = format!("0x{:0>64}", hex::encode(&move_event.route_hash));

        let (package_id, module_name, struct_name) = move_event.extension_type.to_components()?;

        let expires_at = DateTime::from_timestamp_millis(move_event.expires_at_timestamp_ms as i64)
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            event_id: meta.event_digest(),
            occurred_at,
            id: move_event.jump_permit_id.to_hex(),
            character_id: move_event.character_id.to_hex(),
            character_item_id: move_event.character_key.item_id.to_string(),
            departure_id: move_event.source_gate_id.to_hex(),
            departure_item_id: move_event.source_gate_key.item_id.to_string(),
            destination_id: move_event.destination_gate_id.to_hex(),
            destination_item_id: move_event.destination_gate_key.item_id.to_string(),
            link_hash,
            package_id,
            module_name,
            struct_name,
            expires_at,
        })
    }
}
