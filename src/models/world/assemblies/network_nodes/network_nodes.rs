use anyhow::Context;
use chrono::{DateTime, Utc};
use diesel::prelude::*;
use serde::{Deserialize, Serialize};

use sui_indexer_alt_framework::FieldCount;
use sui_sdk_types::Address;
use sui_types::object::Object;

use crate::models::world::MoveAssemblyStatus;
use crate::models::world::MoveEnergySource;
use crate::models::world::MoveFuel;
use crate::models::world::MoveLocation;
use crate::models::world::MoveMetadata;
use crate::models::world::MoveTenantItemId;
use crate::schema::network_nodes;

use crate::AppContext;

#[derive(Deserialize)]
pub struct MoveNetworkNode {
    pub id: Address,
    pub key: MoveTenantItemId,
    pub owner_cap_id: Address,
    pub type_id: u64,
    pub status: MoveAssemblyStatus,
    pub location: MoveLocation,
    pub fuel: MoveFuel,
    pub energy_source: MoveEnergySource,
    pub metadata: Option<MoveMetadata>,
    pub connected_assembly_ids: Vec<Address>,
}

#[derive(Insertable, Serialize, Debug, Clone, FieldCount)]
#[diesel(table_name = network_nodes)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct StoredNetworkNode {
    pub id: String,
    pub item_id: String,
    pub tenant: String,
    pub type_id: i64,
    pub owner_cap_id: String,
    pub location: String,
    pub status: String,
    pub energy_production: i64,
    pub energy_capacity: i64,
    pub energy_reserved: i64,
    pub connected_ids: Vec<String>,
    pub burning: bool,
    pub burn_rate: i64,
    pub burn_start: DateTime<Utc>,
    pub burn_updated: DateTime<Utc>,
    pub burn_elapsed: i64,
    pub fuel_capacity: i64,
    pub fuel_duration: i64,
    pub fuel_quantity: i64,
    pub fuel_type: Option<i64>,
    pub fuel_volume: Option<i64>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub url: Option<String>,
    pub checkpoint_updated: i64,
}

impl StoredNetworkNode {
    pub fn from_object(
        ctx: &AppContext,
        obj: &Object,
        checkpoint_updated: i64,
    ) -> anyhow::Result<Self> {
        let move_obj = obj
            .data
            .try_as_move()
            .context("Object is not a Move object")?;

        let bytes = move_obj.contents();

        let network_node: MoveNetworkNode =
            bcs::from_bytes(bytes).context("Failed fo deserialize Network Node object")?;

        let location = format!(
            "0x{:0>64}",
            hex::encode(&network_node.location.location_hash)
        );

        let connected_ids = network_node
            .connected_assembly_ids
            .iter()
            .map(|entry| entry.to_hex())
            .collect();

        let burn_start = DateTime::from_timestamp_millis(network_node.fuel.burn_start_time as i64)
            .context("Failed to parse burn start timestamp into DateTime")?;

        let burn_updated = DateTime::from_timestamp_millis(network_node.fuel.last_updated as i64)
            .context("Failed to parse burn updated timestamp into DateTime")?;

        let fuel_duration: i64 = match network_node.fuel.type_id {
            Some(type_id) => {
                let stored = network_node.fuel.quantity as i128;

                let burn_time = network_node.fuel.burn_rate_in_ms as i128;

                // let efficiency = Look up from the efficiency table else 10.
                let efficiency = ctx.fuels.get_value(&(type_id as i64)) as i128;

                let burn_start_time = network_node.fuel.burn_start_time as i128;

                let updated_at = network_node.fuel.last_updated as i128;

                let elapsed_time_ms = if updated_at > burn_start_time {
                    updated_at - burn_start_time
                } else {
                    0
                };

                let previous_cycle_elapsed_time =
                    network_node.fuel.previous_cycle_elapsed_time as i128;

                let mut remaining_fuel: i128 = 0;

                if network_node.fuel.is_burning {
                    remaining_fuel = ((burn_time * efficiency) / 100) - elapsed_time_ms;
                } else {
                    if previous_cycle_elapsed_time > 0 {
                        remaining_fuel =
                            ((burn_time * efficiency) / 100) - previous_cycle_elapsed_time;
                    }
                }

                ((((stored * burn_time * efficiency) / 100) + remaining_fuel) / 1000) as i64
            }
            None => 0,
        };

        let fuel_type = network_node.fuel.type_id.map(|type_id| type_id as i64);

        let fuel_volume = network_node
            .fuel
            .unit_volume
            .map(|unit_volume| unit_volume as i64);

        let (name, description, url) = network_node
            .metadata
            .map(|meta| (Some(meta.name), Some(meta.description), Some(meta.url)))
            .unwrap_or_default();

        Ok(Self {
            id: network_node.id.to_hex(),
            item_id: network_node.key.item_id.to_string(),
            tenant: network_node.key.tenant,
            type_id: network_node.type_id as i64,
            owner_cap_id: network_node.owner_cap_id.to_hex(),
            location,
            status: network_node.status.status.as_ref().to_string(),
            energy_production: network_node.energy_source.current_energy_production as i64,
            energy_capacity: network_node.energy_source.max_energy_production as i64,
            energy_reserved: network_node.energy_source.total_reserved_energy as i64,
            connected_ids,
            burning: network_node.fuel.is_burning,
            burn_rate: network_node.fuel.burn_rate_in_ms as i64,
            burn_start,
            burn_updated,
            burn_elapsed: network_node.fuel.previous_cycle_elapsed_time as i64,
            fuel_capacity: network_node.fuel.max_capacity as i64,
            fuel_duration,
            fuel_quantity: network_node.fuel.quantity as i64,
            fuel_type,
            fuel_volume,
            name,
            description,
            url,
            checkpoint_updated,
        })
    }
}
