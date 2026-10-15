use async_trait::async_trait;
use serde::Serialize;

use crate::handlers::world::*;
use crate::models::system::*;
use crate::models::world::*;
use crate::transports::Routing;

pub struct RedisTransport {
    manager: redis::aio::ConnectionManager,
    channel_prefix: String,
}

impl RedisTransport {
    pub async fn connect(url: &str, channel_prefix: impl Into<String>) -> anyhow::Result<Self> {
        let client = redis::Client::open(url)?;
        let manager = redis::aio::ConnectionManager::new(client).await?;

        Ok(Self {
            manager,
            channel_prefix: channel_prefix.into(),
        })
    }

    async fn send<I: Serialize + Send + Sync + 'static>(
        &self,
        channel: String,
        item: &I,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}", self.channel_prefix, channel);
        let payload = serde_json::to_string(item)?;
        let mut conn = self.manager.clone();
        redis::AsyncCommands::publish::<_, _, ()>(&mut conn, &channel, &payload).await?;
        Ok(())
    }
}

// Address Aliases
#[async_trait]
impl Routing<AddressAliases> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &AddressAliases) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "address_aliases", item.id, "updated");
        self.send(channel, item).await
    }
}

// Transaction Digests
#[async_trait]
impl Routing<StoredTransactionDigest> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTransactionDigest,
    ) -> anyhow::Result<()> {
        self.send("transactions:digest".to_string(), item).await
    }
}

// Owner Caps
#[async_trait]
impl Routing<StoredOwnerCapCreated> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredOwnerCapCreated,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "owner_cap", item.object_id, "created");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<OwnerCapAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &OwnerCapAction) -> anyhow::Result<()> {
        match action {
            OwnerCapAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "owner_cap", item.object_id, "updated");
                self.send(channel, item).await
            }
            OwnerCapAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "owner_cap", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredOwnerCapTransferred> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredOwnerCapTransferred,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "owner_cap", item.id, "transferred");
        self.send(channel, item).await
    }
}

// Assemblies
#[async_trait]
impl Routing<StoredAssemblyCreated> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredAssemblyCreated,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "assembly", item.id, "created");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<AssemblyAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &AssemblyAction) -> anyhow::Result<()> {
        match action {
            AssemblyAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "assembly", item.id, "updated");
                self.send(channel, item).await
            }
            AssemblyAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "assembly", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

// Extensions
#[async_trait]
impl Routing<StoredExtensionFrozen> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredExtensionFrozen,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "extension", item.id, "frozen");
        self.send(channel, item).await
    }
}

// Gates
#[async_trait]
impl Routing<GateConfigAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &GateConfigAction) -> anyhow::Result<()> {
        match action {
            GateConfigAction::Register(_table) => Ok(()),
            GateConfigAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "gate_config", item.type_id, "updated");
                self.send(channel, item).await
            }
            GateConfigAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "gate_config", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGateCreated> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateCreated) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "gate", item.id, "created");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredGateExtensionAuthorized> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredGateExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "gate", item.id, "extension_authorized");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredGateExtensionRevoked> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredGateExtensionRevoked,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "gate", item.id, "extension_revoked");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<GateAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &GateAction) -> anyhow::Result<()> {
        match action {
            GateAction::Freeze(item) => {
                let channel = format!("{}:{}:{}", "gate", item.id, "extension_frozen");
                self.send(channel, item).await
            }
            GateAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "gate", item.id, "updated");
                self.send(channel, item).await
            }
            GateAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "gate", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGateJumped> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateJumped) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "gate", item.departure_id, "jumped");
        _ = self.send(channel, item).await;

        let channel = format!("{}:{}:{}", "gate", item.destination_id, "jumped");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredGateLinked> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateLinked) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "gate", item.departure_id, "linked");
        _ = self.send(channel, item).await;

        let channel = format!("{}:{}:{}", "gate", item.destination_id, "linked");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<GatePermitAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &GatePermitAction) -> anyhow::Result<()> {
        match action {
            GatePermitAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "permit", item.id, "issued");
                self.send(channel, item).await
            }
            GatePermitAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "permit", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGatePermitIssued> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredGatePermitIssued,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "permit", item.character_id, "issued");
        _ = self.send(channel, item).await;

        let channel = format!("{}:{}:{}", "permit", item.departure_id, "issued");
        _ = self.send(channel, item).await;

        let channel = format!("{}:{}:{}", "permit", item.destination_id, "issued");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredGateUnlinked> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateUnlinked) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "gate", item.departure_id, "unlinked");
        _ = self.send(channel, item).await;

        let channel = format!("{}:{}:{}", "gate", item.destination_id, "unlinked");
        self.send(channel, item).await
    }
}

// Network Nodes
#[async_trait]
impl Routing<StoredNetworkNodeCreated> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredNetworkNodeCreated,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "network_node", item.id, "created");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<NetworkNodeAction> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        action: &NetworkNodeAction,
    ) -> anyhow::Result<()> {
        match action {
            NetworkNodeAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "network_node", item.id, "updated");
                self.send(channel, item).await
            }
            NetworkNodeAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "network_node", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

// Storage Units
#[async_trait]
impl Routing<StoredStorageUnitCreated> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStorageUnitCreated,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "storage_unit", item.id, "created");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredStorageUnitExtensionAuthorized> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStorageUnitExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "storage_unit", item.id, "extension_authorized");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredStorageUnitExtensionRevoked> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStorageUnitExtensionRevoked,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "storage_unit", item.id, "extension_revoked");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StorageUnitAction> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        action: &StorageUnitAction,
    ) -> anyhow::Result<()> {
        match action {
            StorageUnitAction::Freeze(item) => {
                let channel = format!("{}:{}:{}", "storage_unit", item.id, "extension_frozen");
                self.send(channel, item).await
            }
            StorageUnitAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "storage_unit", item.id, "updated");
                self.send(channel, item).await
            }
            StorageUnitAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "storage_unit", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

// Turrets
#[async_trait]
impl Routing<StoredTurretCreated> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTurretCreated,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "turret", item.id, "created");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredTurretExtensionAuthorized> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTurretExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "turret", item.id, "extension_authorized");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredTurretExtensionRevoked> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTurretExtensionRevoked,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "turret", item.id, "extension_revoked");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<TurretAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &TurretAction) -> anyhow::Result<()> {
        match action {
            TurretAction::Freeze(item) => {
                let channel = format!("{}:{}:{}", "turret", item.id, "extension_frozen");
                self.send(channel, item).await
            }
            TurretAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "turret", item.id, "updated");
                self.send(channel, item).await
            }
            TurretAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "turret", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

// Characters
#[async_trait]
impl Routing<StoredCharacterCreated> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredCharacterCreated,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "character", item.id, "created");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<CharacterAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &CharacterAction) -> anyhow::Result<()> {
        match action {
            CharacterAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "character", item.id, "updated");
                self.send(channel, item).await
            }
            CharacterAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "character", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

// Killmails
#[async_trait]
impl Routing<StoredKillmail> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredKillmail) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "killmail", item.id, "created");
        self.send(channel, item).await
    }
}

// Energy
#[async_trait]
impl Routing<EnergyConfigAction> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        action: &EnergyConfigAction,
    ) -> anyhow::Result<()> {
        match action {
            EnergyConfigAction::Register(_table) => Ok(()),
            EnergyConfigAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "energy_config", item.type_id, "updated");
                self.send(channel, item).await
            }
            EnergyConfigAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "energy_config", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredEnergyProductionStarted> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyProductionStarted,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "energy", item.id, "started");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyProductionStopped> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyProductionStopped,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "energy", item.id, "stopped");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyReleased> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyReleased,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "energy", item.id, "released");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyReserved> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyReserved,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "energy", item.id, "reserved");
        self.send(channel, item).await
    }
}

// Fuel
#[async_trait]
impl Routing<StoredFuelBurningStarted> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelBurningStarted,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel", item.id, "started");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelBurningStopped> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelBurningStopped,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel", item.id, "stopped");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelBurningUpdated> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelBurningUpdated,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel", item.id, "updated");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<FuelConfigAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &FuelConfigAction) -> anyhow::Result<()> {
        match action {
            FuelConfigAction::Register(_table) => Ok(()),
            FuelConfigAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "fuel_config", item.type_id, "updated");
                self.send(channel, item).await
            }
            FuelConfigAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "fuel_config", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredFuelDeleted> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredFuelDeleted) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel", item.id, "deleted");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelDeposited> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelDeposited,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel", item.id, "deposited");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelEfficiencyRemoved> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelEfficiencyRemoved,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel_config", item.type_id, "removed");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelEfficiencySet> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelEfficiencySet,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel_config", item.type_id, "set");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelWithdrawn> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelWithdrawn,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "fuel", item.id, "withdrawn");
        self.send(channel, item).await
    }
}

// Inventories
#[async_trait]
impl Routing<InventoryAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &InventoryAction) -> anyhow::Result<()> {
        match action {
            InventoryAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "inventory", item.id, "updated");
                self.send(channel, item).await
            }
            InventoryAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "inventory", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredItemBurned> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredItemBurned) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "item", item.assembly_id, "burned");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredItemDeposited> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredItemDeposited,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "item", item.assembly_id, "deposited");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredItemDestroyed> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredItemDestroyed,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "item", item.assembly_id, "destroyed");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<ItemAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &ItemAction) -> anyhow::Result<()> {
        match action {
            ItemAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "item", item.id, "updated");
                self.send(channel, item).await
            }
            ItemAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "item", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredItemMinted> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredItemMinted) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "item", item.assembly_id, "minted");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredItemWithdrawn> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredItemWithdrawn,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "item", item.assembly_id, "withdrawn");
        self.send(channel, item).await
    }
}

// Locations
#[async_trait]
impl Routing<StoredLocationRevealed> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredLocationRevealed,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "location", item.id, "revealed");
        self.send(channel, item).await
    }
}

// Status
#[async_trait]
impl Routing<StoredStatusChanged> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStatusChanged,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "status", item.id, "updated");
        self.send(channel, item).await
    }
}

// Rifts
#[async_trait]
impl Routing<RiftAction> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, action: &RiftAction) -> anyhow::Result<()> {
        match action {
            RiftAction::Upsert(item) => {
                let channel = format!("{}:{}:{}", "rift", item.id, "updated");
                self.send(channel, item).await
            }
            RiftAction::Delete(id_str) => {
                let channel = format!("{}:{}:{}", "rift", id_str, "deleted");
                self.send(channel, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredRiftLocationBroadcasted> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredRiftLocationBroadcasted,
    ) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "rift", item.id, "revealed");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredRiftMiningStarted> for RedisTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredRiftMiningStarted,
    ) -> anyhow::Result<()> {
        // Note: This messsage broadcasts to the in-game id of the rift since they didnt include the on-chain id.
        // Should get fixed in world v1 once available.
        let channel = format!("{}:{}:{}", "rift", item.item_id, "mined");
        self.send(channel, item).await
    }
}

#[async_trait]
impl Routing<StoredRiftSpawned> for RedisTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredRiftSpawned) -> anyhow::Result<()> {
        let channel = format!("{}:{}:{}", "rift", item.id, "spawned");
        self.send(channel, item).await
    }
}
