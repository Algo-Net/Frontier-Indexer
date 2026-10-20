use anyhow::Context;
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;
use sui_types::object::Owner;

use crate::schema::owner_caps;

#[derive(Deserialize)]
pub struct MoveOwnerCap {
    pub id: Address,
    pub authorized_object_id: Address,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = owner_caps)]
pub struct StoredOwnerCap {
    pub id: String,
    pub object_id: String,
    pub owner_address: String,
    pub package_id: String,
    pub module_name: String,
    pub struct_name: String,
    pub checkpoint_updated: i64,
}

impl StoredOwnerCap {
    pub fn from_object(obj: &Object, checkpoint_updated: i64) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let owner_cap: MoveOwnerCap =
            bcs::from_bytes(bytes).context("Failed to deserialize OwnerCap object")?;

        let owner_address = match obj.owner {
            Owner::AddressOwner(address) => address.to_string(),
            Owner::ObjectOwner(address) => address.to_string(),
            Owner::Party { .. } => "party".to_string(),
            Owner::Shared { .. } => "shared".to_string(),
            Owner::Immutable => "immutable".to_string(),
            Owner::ConsensusAddressOwner { owner, .. } => owner.to_string(),
        };

        let (package_id, module_name, struct_name) = move_obj
            .type_()
            .type_params()
            .first()
            .and_then(|tag| {
                if let sui_types::TypeTag::Struct(s_tag) = tag.as_ref() {
                    Some((
                        s_tag.address.to_canonical_string(true),
                        s_tag.module.to_string(),
                        s_tag.name.to_string(),
                    ))
                } else {
                    None
                }
            })
            .unwrap_or_else(|| ("".to_string(), "".to_string(), "".to_string()));

        Ok(Self {
            id: owner_cap.id.to_hex(),
            object_id: owner_cap.authorized_object_id.to_hex(),
            owner_address,
            package_id,
            module_name,
            struct_name,
            checkpoint_updated,
        })
    }
}
