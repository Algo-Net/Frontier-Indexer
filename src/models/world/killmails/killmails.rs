use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};
use strum_macros::{AsRefStr, Display, EnumString};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::models::world::MoveTenantItemId;
use crate::schema::killmails;

#[derive(Deserialize, Debug, Clone, Copy, Display, EnumString, AsRefStr)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
#[strum(serialize_all = "SCREAMING_SNAKE_CASE")]
pub enum MoveLossType {
    Ship = 0,
    Structure = 1,
}

#[derive(Deserialize)]
pub struct MoveKillmail {
    pub id: Address,
    pub key: MoveTenantItemId,
    pub killer_id: MoveTenantItemId,
    pub victim_id: MoveTenantItemId,
    pub reported_by_character_id: MoveTenantItemId,
    pub kill_timestamp: u64,
    pub loss_type: MoveLossType,
    pub solar_system_id: MoveTenantItemId,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = killmails)]
pub struct StoredKillmail {
    pub id: String,
    pub kill_id: String,
    pub tenant: String,
    pub occurred_at: DateTime<Utc>,
    pub solar_system_id: String,
    pub loss_type: String,
    pub killer_id: String,
    pub victim_id: String,
    pub reporter_id: String,
}

impl StoredKillmail {
    pub fn from_object(obj: &Object) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let killmail: MoveKillmail =
            bcs::from_bytes(bytes).context("Failed to deserialize Killmail object")?;

        let occurred_at = DateTime::from_timestamp(killmail.kill_timestamp as i64, 0)
            .context("Failed to parse killmail timestamp into DateTime")?;

        Ok(Self {
            id: killmail.id.to_hex(),
            kill_id: killmail.key.item_id.to_string(),
            tenant: killmail.key.tenant,
            occurred_at,
            solar_system_id: killmail.solar_system_id.item_id.to_string(),
            loss_type: killmail.loss_type.as_ref().to_string(),
            killer_id: killmail.killer_id.item_id.to_string(),
            victim_id: killmail.victim_id.item_id.to_string(),
            reporter_id: killmail.reported_by_character_id.item_id.to_string(),
        })
    }
}
