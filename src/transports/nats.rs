use async_nats::Client;
use async_trait::async_trait;
use serde::Serialize;

use crate::handlers::world::*;
use crate::models::system::*;
use crate::models::world::*;
use crate::transports::Routing;

pub struct NatsTransport {
    client: Client,
    subject_prefix: String,
}

impl NatsTransport {
    pub async fn connect(url: &str, subject_prefix: impl Into<String>) -> anyhow::Result<Self> {
        Ok(Self {
            client: async_nats::connect(url).await?,
            subject_prefix: subject_prefix.into(),
        })
    }

    async fn send<I: Serialize + Send + Sync + 'static>(
        &self,
        routing: String,
        item: &I,
    ) -> anyhow::Result<()> {
        let subject = format!("{}.{}", self.subject_prefix, routing);
        let payload = serde_json::to_vec(item)?;
        self.client.publish(subject, payload.into()).await?;
        Ok(())
    }
}

// Address Aliases
#[async_trait]
impl Routing<AddressAliases> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &AddressAliases) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "address_aliases", item.id, "updated");
        self.send(routing, item).await
    }
}

// Transaction Digests
#[async_trait]
impl Routing<StoredTransactionDigest> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTransactionDigest,
    ) -> anyhow::Result<()> {
        self.send("transactions.digest".to_string(), item).await
    }
}

// Owner Caps
#[async_trait]
impl Routing<StoredOwnerCapCreated> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredOwnerCapCreated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "owner_cap", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<OwnerCapAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &OwnerCapAction) -> anyhow::Result<()> {
        match action {
            OwnerCapAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "owner_cap", item.id, "updated");
                self.send(routing, item).await
            }
            OwnerCapAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "owner_cap", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredOwnerCapTransferred> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredOwnerCapTransferred,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "owner_cap", item.id, "transferred");
        self.send(routing, item).await
    }
}

// Assemblies
#[async_trait]
impl Routing<StoredAssemblyCreated> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredAssemblyCreated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "assembly", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<AssemblyAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &AssemblyAction) -> anyhow::Result<()> {
        match action {
            AssemblyAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "assembly", item.id, "updated");
                self.send(routing, item).await
            }
            AssemblyAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "assembly", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

// Extensions
#[async_trait]
impl Routing<StoredExtensionFrozen> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredExtensionFrozen,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "extension", item.id, "frozen");
        self.send(routing, item).await
    }
}

// Gates
#[async_trait]
impl Routing<GateConfigAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &GateConfigAction) -> anyhow::Result<()> {
        match action {
            GateConfigAction::Register(_table) => Ok(()),
            GateConfigAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "gate_config", item.type_id, "updated");
                self.send(routing, item).await
            }
            GateConfigAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "gate_config", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGateCreated> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateCreated) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredGateExtensionAuthorized> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredGateExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.id, "extension_authorized");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredGateExtensionRevoked> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredGateExtensionRevoked,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.id, "extension_revoked");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<GateAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &GateAction) -> anyhow::Result<()> {
        match action {
            GateAction::Freeze(item) => {
                let routing = format!("{}.{}.{}", "gate", item.id, "extension_frozen");
                self.send(routing, item).await
            }
            GateAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "gate", item.id, "updated");
                self.send(routing, item).await
            }
            GateAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "gate", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGateJumped> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateJumped) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.departure_id, "jumped");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "gate", item.destination_id, "jumped");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredGateLinked> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateLinked) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.departure_id, "linked");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "gate", item.destination_id, "linked");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<GatePermitAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &GatePermitAction) -> anyhow::Result<()> {
        match action {
            GatePermitAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "permit", item.id, "issued");
                self.send(routing, item).await
            }
            GatePermitAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "permit", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGatePermitIssued> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredGatePermitIssued,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "permit", item.character_id, "issued");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "permit", item.departure_id, "issued");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "permit", item.destination_id, "issued");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredGateUnlinked> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateUnlinked) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.departure_id, "unlinked");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "gate", item.destination_id, "unlinked");
        self.send(routing, item).await
    }
}

// Network Nodes
#[async_trait]
impl Routing<StoredNetworkNodeCreated> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredNetworkNodeCreated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "network_node", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<NetworkNodeAction> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        action: &NetworkNodeAction,
    ) -> anyhow::Result<()> {
        match action {
            NetworkNodeAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "network_node", item.id, "updated");
                self.send(routing, item).await
            }
            NetworkNodeAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "network_node", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

// Storage Units
#[async_trait]
impl Routing<StoredStorageUnitCreated> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStorageUnitCreated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "storage_unit", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredStorageUnitExtensionAuthorized> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStorageUnitExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "storage_unit", item.id, "extension_authorized");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredStorageUnitExtensionRevoked> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStorageUnitExtensionRevoked,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "storage_unit", item.id, "extension_revoked");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StorageUnitAction> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        action: &StorageUnitAction,
    ) -> anyhow::Result<()> {
        match action {
            StorageUnitAction::Freeze(item) => {
                let routing = format!("{}.{}.{}", "storage_unit", item.id, "extension_frozen");
                self.send(routing, item).await
            }
            StorageUnitAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "storage_unit", item.id, "updated");
                self.send(routing, item).await
            }
            StorageUnitAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "storage_unit", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

// Turrets
#[async_trait]
impl Routing<StoredTurretCreated> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTurretCreated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "turret", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredTurretExtensionAuthorized> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTurretExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "turret", item.id, "extension_authorized");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredTurretExtensionRevoked> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTurretExtensionRevoked,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "turret", item.id, "extension_revoked");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<TurretAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &TurretAction) -> anyhow::Result<()> {
        match action {
            TurretAction::Freeze(item) => {
                let routing = format!("{}.{}.{}", "turret", item.id, "extension_frozen");
                self.send(routing, item).await
            }
            TurretAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "turret", item.id, "updated");
                self.send(routing, item).await
            }
            TurretAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "turret", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

// Characters
#[async_trait]
impl Routing<StoredCharacterCreated> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredCharacterCreated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "character", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<CharacterAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &CharacterAction) -> anyhow::Result<()> {
        match action {
            CharacterAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "character", item.id, "updated");
                self.send(routing, item).await
            }
            CharacterAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "character", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

// Killmails
#[async_trait]
impl Routing<StoredKillmail> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredKillmail) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "killmail", item.id, "created");
        self.send(routing, item).await
    }
}

// Energy
#[async_trait]
impl Routing<EnergyConfigAction> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        action: &EnergyConfigAction,
    ) -> anyhow::Result<()> {
        match action {
            EnergyConfigAction::Register(_table) => Ok(()),
            EnergyConfigAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "energy_config", item.type_id, "updated");
                self.send(routing, item).await
            }
            EnergyConfigAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "energy_config", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredEnergyProductionStarted> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyProductionStarted,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "energy", item.id, "started");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyProductionStopped> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyProductionStopped,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "energy", item.id, "stopped");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyReleased> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyReleased,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "energy", item.id, "released");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyReserved> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredEnergyReserved,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "energy", item.id, "reserved");
        self.send(routing, item).await
    }
}

// Fuel
#[async_trait]
impl Routing<StoredFuelBurningStarted> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelBurningStarted,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel", item.id, "started");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelBurningStopped> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelBurningStopped,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel", item.id, "stopped");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelBurningUpdated> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelBurningUpdated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel", item.id, "updated");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<FuelConfigAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &FuelConfigAction) -> anyhow::Result<()> {
        match action {
            FuelConfigAction::Register(_table) => Ok(()),
            FuelConfigAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "fuel_config", item.type_id, "updated");
                self.send(routing, item).await
            }
            FuelConfigAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "fuel_config", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredFuelDeleted> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredFuelDeleted) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel", item.id, "deleted");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelDeposited> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelDeposited,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel", item.id, "deposited");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelEfficiencyRemoved> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelEfficiencyRemoved,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel_config", item.type_id, "removed");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelEfficiencySet> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelEfficiencySet,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel_config", item.type_id, "set");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelWithdrawn> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredFuelWithdrawn,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel", item.id, "withdrawn");
        self.send(routing, item).await
    }
}

// Inventories
#[async_trait]
impl Routing<InventoryAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &InventoryAction) -> anyhow::Result<()> {
        match action {
            InventoryAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "inventory", item.id, "updated");
                self.send(routing, item).await
            }
            InventoryAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "inventory", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredItemBurned> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredItemBurned) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "item", item.assembly_id, "burned");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredItemDeposited> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredItemDeposited,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "item", item.assembly_id, "deposited");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredItemDestroyed> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredItemDestroyed,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "item", item.assembly_id, "destroyed");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<ItemAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &ItemAction) -> anyhow::Result<()> {
        match action {
            ItemAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "item", item.id, "updated");
                self.send(routing, item).await
            }
            ItemAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "item", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredItemMinted> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredItemMinted) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "item", item.assembly_id, "minted");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredItemWithdrawn> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredItemWithdrawn,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "item", item.assembly_id, "withdrawn");
        self.send(routing, item).await
    }
}

// Locations
#[async_trait]
impl Routing<StoredLocationRevealed> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredLocationRevealed,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "location", item.id, "revealed");
        self.send(routing, item).await
    }
}

// Status
#[async_trait]
impl Routing<StoredStatusChanged> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredStatusChanged,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "status", item.id, "updated");
        self.send(routing, item).await
    }
}

// Rifts
#[async_trait]
impl Routing<RiftAction> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, action: &RiftAction) -> anyhow::Result<()> {
        match action {
            RiftAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "rift", item.id, "updated");
                self.send(routing, item).await
            }
            RiftAction::Delete(id_str) => {
                let routing = format!("{}.{}.{}", "rift", id_str, "deleted");
                self.send(routing, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredRiftLocationBroadcasted> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredRiftLocationBroadcasted,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "rift", item.id, "revealed");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredRiftMiningStarted> for NatsTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredRiftMiningStarted,
    ) -> anyhow::Result<()> {
        // Note: This messsage broadcasts to the in-game id of the rift since they didnt include the on-chain id.
        // Should get fixed in world v1 once available.
        let routing = format!("{}.{}.{}", "rift", item.item_id, "mined");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredRiftSpawned> for NatsTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredRiftSpawned) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "rift", item.id, "spawned");
        self.send(routing, item).await
    }
}
