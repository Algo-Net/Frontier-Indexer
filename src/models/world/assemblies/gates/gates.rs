use anyhow::Context;
use serde::{Deserialize, Serialize};

use diesel::prelude::*;

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::models::Freezable;
use crate::models::MoveTypeName;
use crate::models::world::MoveAssemblyStatus;
use crate::models::world::MoveLocation;
use crate::models::world::MoveMetadata;
use crate::models::world::MoveTenantItemId;
use crate::schema::gates;

#[derive(Deserialize)]
pub struct MoveGate {
    pub id: Address,
    pub key: MoveTenantItemId,
    pub owner_cap_id: Address,
    pub type_id: u64,
    pub linked_gate_id: Option<Address>,
    pub status: MoveAssemblyStatus,
    pub location: MoveLocation,
    pub energy_source_id: Option<Address>,
    pub metadata: Option<MoveMetadata>,
    pub extension: Option<MoveTypeName>,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = gates)]
pub struct StoredGate {
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub type_id: i64,
    pub owner_cap_id: String,
    pub location: String,
    pub status: String,
    pub energy_source_id: Option<String>,
    pub linked_id: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub package_id: Option<String>,
    pub module_name: Option<String>,
    pub struct_name: Option<String>,
    pub checkpoint_updated: i64,
}

impl StoredGate {
    pub fn from_object(obj: &Object, checkpoint_updated: i64) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let gate: MoveGate = bcs::from_bytes(bytes).context("Failed to deserialize Gate object")?;

        let location = format!("0x{:0>64}", hex::encode(&gate.location.location_hash));

        let energy_source_id = gate.energy_source_id.map(|source| source.to_hex());

        let linked_id = gate.linked_gate_id.map(|id| id.to_hex());

        let (name, description, url) = gate
            .metadata
            .map(|meta| (Some(meta.name), Some(meta.description), Some(meta.url)))
            .unwrap_or_default();

        let (package_id, module_name, struct_name) = match gate.extension {
            Some(extension) => {
                let (package_id, module_name, struct_name) = extension.to_components()?;
                (Some(package_id), Some(module_name), Some(struct_name))
            }
            None => (None, None, None),
        };

        Ok(Self {
            id: gate.id.to_hex(),
            item_id: gate.key.item_id.to_string(),
            tenant: gate.key.tenant,
            type_id: gate.type_id as i64,
            owner_cap_id: gate.owner_cap_id.to_hex(),
            location,
            status: gate.status.status.as_ref().to_string(),
            energy_source_id,
            linked_id,
            name,
            description,
            url,
            package_id,
            module_name,
            struct_name,
            checkpoint_updated,
        })
    }
}

impl Freezable for StoredGate {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn package_id(&self) -> anyhow::Result<String> {
        self.package_id
            .clone()
            .context("Package_id was not available on gate")
    }

    fn module_name(&self) -> anyhow::Result<String> {
        self.module_name
            .clone()
            .context("Module_name was not available on gate")
    }

    fn struct_name(&self) -> anyhow::Result<String> {
        self.struct_name
            .clone()
            .context("Struct_name was not available on gate")
    }
}
