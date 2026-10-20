use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::schema::gate_permits;

#[derive(Deserialize)]
pub struct MoveGatePermit {
    pub id: Address,
    pub character_id: Address,
    pub route_hash: Vec<u8>,
    pub expires_at_timestamp_ms: u64,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = gate_permits)]
pub struct StoredGatePermit {
    pub id: String,
    pub character_id: String,
    pub link_hash: String,
    pub expires_at: DateTime<Utc>,
}

impl StoredGatePermit {
    pub fn from_object(obj: &Object) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let permit: MoveGatePermit =
            bcs::from_bytes(bytes).context("Failed to deserialize Gate Permit object")?;

        let link_hash = format!("0x{:0>64}", hex::encode(&permit.route_hash));

        let expires_at = DateTime::from_timestamp_millis(permit.expires_at_timestamp_ms as i64)
            .context("Failed to parse checkpoint timestamp into DateTime")?;

        Ok(Self {
            id: permit.id.to_hex(),
            character_id: permit.character_id.to_hex(),
            link_hash,
            expires_at,
        })
    }
}
