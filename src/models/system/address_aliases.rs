use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;
use sui_types::object::Owner;

use crate::schema::address_aliases;

#[derive(Deserialize)]
pub struct MoveAddressAliases {
    pub id: Address,
    aliases: Vec<Address>,
}

#[derive(Serialize, Debug, Clone, FieldCount)]
pub struct AddressAliases {
    pub id: String,
    pub aliases: Vec<String>,
    pub checkpoint_updated: i64,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = address_aliases)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredAddressAliases {
    pub address: String,
    pub alias: String,
    pub checkpoint_updated: i64,
}

impl AddressAliases {
    pub fn from_object(obj: &Object, checkpoint_updated: i64) -> Self {
        let move_obj = obj.data.try_as_move().expect("Object is not a Move object");
        let bytes = move_obj.contents();

        let address_aliases: MoveAddressAliases =
            bcs::from_bytes(bytes).expect("Failed to deserialize AccountAliases object");

        let id = match obj.owner {
            Owner::AddressOwner(address) => address.to_string(),
            Owner::ObjectOwner(address) => address.to_string(),
            Owner::Party { .. } => "party".to_string(),
            Owner::Shared { .. } => "shared".to_string(),
            Owner::Immutable => "immutable".to_string(),
            Owner::ConsensusAddressOwner { owner, .. } => owner.to_string(),
        };

        let aliases = address_aliases
            .aliases
            .iter()
            .map(|entry| entry.to_hex())
            .collect();

        Self {
            id,
            aliases,
            checkpoint_updated,
        }
    }

    pub fn to_aliases(self) -> Vec<StoredAddressAliases> {
        self.aliases
            .iter()
            .map(|entry| StoredAddressAliases {
                address: self.id.clone(),
                alias: entry.clone(),
                checkpoint_updated: self.checkpoint_updated,
            })
            .collect()
    }
}
