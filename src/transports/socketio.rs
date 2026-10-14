use async_trait::async_trait;
use serde::Serialize;
use socketioxide::SocketIo;

use crate::handlers::world::*;
use crate::models::system::*;
use crate::models::world::*;
use crate::transports::Routing;

pub struct SocketIoTransport {
    io: SocketIo,
}

impl SocketIoTransport {
    pub fn new(io: SocketIo) -> Self {
        Self { io }
    }

    async fn send<I: Serialize + Send + Sync + 'static>(
        &self,
        room: String,
        event: String,
        item: &I,
    ) -> anyhow::Result<()> {
        let _ = self.io.to(room.clone()).emit(&event.clone(), item);
        let _ = self.io.to("*").emit(format!("{}:{}", room, event), item);
        Ok(())
    }
}

// Address Alias
#[async_trait]
impl Routing<AddressAliases> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &AddressAliases) -> anyhow::Result<()> {
        let room = item.id.clone();
        let event = format!("{}:{}", pipeline, "updated");
        self.send(room, event, item).await
    }
}

// Transaction Digests
#[async_trait]
impl Routing<StoredTransactionDigest> for SocketIoTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredTransactionDigest,
    ) -> anyhow::Result<()> {
        self.send("transactions".to_string(), "digest".to_string(), item)
            .await
    }
}

// Owner Caps
#[async_trait]
impl Routing<StoredOwnerCapCreated> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredOwnerCapCreated,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<OwnerCapAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &OwnerCapAction) -> anyhow::Result<()> {
        match action {
            OwnerCapAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            OwnerCapAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredOwnerCapTransferred> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredOwnerCapTransferred,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Assemblies
#[async_trait]
impl Routing<StoredAssemblyCreated> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredAssemblyCreated,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<AssemblyAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &AssemblyAction) -> anyhow::Result<()> {
        match action {
            AssemblyAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            AssemblyAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

// Extensions
#[async_trait]
impl Routing<StoredExtensionFrozen> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredExtensionFrozen,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Gates
#[async_trait]
impl Routing<GateConfigAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &GateConfigAction) -> anyhow::Result<()> {
        match action {
            GateConfigAction::Register(_table) => Ok(()),
            GateConfigAction::Upsert(item) => {
                let event = format!("{}:{}", pipeline, "updated");
                self.send(pipeline.to_string(), event, item).await
            }
            GateConfigAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(pipeline.to_string(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGateCreated> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredGateCreated) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredGateExtensionAuthorized> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredGateExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredGateExtensionRevoked> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredGateExtensionRevoked,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<GateAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &GateAction) -> anyhow::Result<()> {
        match action {
            GateAction::Freeze(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "extension_frozen");
                self.send(room, event, item).await
            }
            GateAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            GateAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGateJumped> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredGateJumped) -> anyhow::Result<()> {
        let room = item.departure_id.clone();
        _ = self.send(room, pipeline.to_string(), item).await;

        let room = item.destination_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredGateLinked> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredGateLinked) -> anyhow::Result<()> {
        let room = item.departure_id.clone();
        let _ = self.send(room, pipeline.to_string(), item).await;

        let room = item.destination_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<GatePermitAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &GatePermitAction) -> anyhow::Result<()> {
        match action {
            GatePermitAction::Upsert(item) => {
                let room = item.character_id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            GatePermitAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredGatePermitIssued> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredGatePermitIssued,
    ) -> anyhow::Result<()> {
        let room = item.departure_id.clone();
        _ = self.send(room, pipeline.to_string(), item).await;

        let room = item.destination_id.clone();
        _ = self.send(room, pipeline.to_string(), item).await;

        let room = item.character_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredGateUnlinked> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredGateUnlinked) -> anyhow::Result<()> {
        let room = item.departure_id.clone();
        _ = self.send(room, pipeline.to_string(), item).await;

        let room = item.destination_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Network Nodes
#[async_trait]
impl Routing<StoredNetworkNodeCreated> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredNetworkNodeCreated,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<NetworkNodeAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &NetworkNodeAction) -> anyhow::Result<()> {
        match action {
            NetworkNodeAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            NetworkNodeAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

// Storage Units
#[async_trait]
impl Routing<StoredStorageUnitCreated> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredStorageUnitCreated,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredStorageUnitExtensionAuthorized> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredStorageUnitExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredStorageUnitExtensionRevoked> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredStorageUnitExtensionRevoked,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StorageUnitAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &StorageUnitAction) -> anyhow::Result<()> {
        match action {
            StorageUnitAction::Freeze(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "extension_frozen");
                self.send(room, event, item).await
            }
            StorageUnitAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            StorageUnitAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

// Turrets
#[async_trait]
impl Routing<StoredTurretCreated> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredTurretCreated) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredTurretExtensionAuthorized> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredTurretExtensionAuthorized,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredTurretExtensionRevoked> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredTurretExtensionRevoked,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<TurretAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &TurretAction) -> anyhow::Result<()> {
        match action {
            TurretAction::Freeze(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "extension_frozen");
                self.send(room, event, item).await
            }
            TurretAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            TurretAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

// Characters
#[async_trait]
impl Routing<StoredCharacterCreated> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredCharacterCreated,
    ) -> anyhow::Result<()> {
        let room = item.owner_address.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<CharacterAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &CharacterAction) -> anyhow::Result<()> {
        match action {
            CharacterAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            CharacterAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

// Killmails
#[async_trait]
impl Routing<StoredKillmail> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredKillmail) -> anyhow::Result<()> {
        let event = format!("{}:{}", pipeline, "updated");
        self.send(pipeline.to_string(), event, item).await
    }
}

// Energy
#[async_trait]
impl Routing<EnergyConfigAction> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        action: &EnergyConfigAction,
    ) -> anyhow::Result<()> {
        match action {
            EnergyConfigAction::Register(_table) => Ok(()),
            EnergyConfigAction::Upsert(item) => {
                let event = format!("{}:{}", pipeline, "updated");
                self.send(pipeline.to_string(), event, item).await
            }
            EnergyConfigAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(pipeline.to_string(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredEnergyProductionStarted> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredEnergyProductionStarted,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyProductionStopped> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredEnergyProductionStopped,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyReleased> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredEnergyReleased,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredEnergyReserved> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredEnergyReserved,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Fuel
#[async_trait]
impl Routing<StoredFuelBurningStarted> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredFuelBurningStarted,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredFuelBurningStopped> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredFuelBurningStopped,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredFuelBurningUpdated> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredFuelBurningUpdated,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<FuelConfigAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &FuelConfigAction) -> anyhow::Result<()> {
        match action {
            FuelConfigAction::Register(_table) => Ok(()),
            FuelConfigAction::Upsert(item) => {
                let event = format!("{}:{}", pipeline, "updated");
                self.send(pipeline.to_string(), event, item).await
            }
            FuelConfigAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(pipeline.to_string(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredFuelDeleted> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredFuelDeleted) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredFuelDeposited> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredFuelDeposited) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredFuelEfficiencyRemoved> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredFuelEfficiencyRemoved,
    ) -> anyhow::Result<()> {
        let event = format!("{}:{}", pipeline, "deleted");
        self.send(pipeline.to_string(), event, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelEfficiencySet> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredFuelEfficiencySet,
    ) -> anyhow::Result<()> {
        let event = format!("{}:{}", pipeline, "updated");
        self.send(pipeline.to_string(), event, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelWithdrawn> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredFuelWithdrawn) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Inventories
#[async_trait]
impl Routing<InventoryAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &InventoryAction) -> anyhow::Result<()> {
        match action {
            InventoryAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            InventoryAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredItemBurned> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredItemBurned) -> anyhow::Result<()> {
        let room = item.assembly_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredItemDeposited> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredItemDeposited) -> anyhow::Result<()> {
        let room = item.assembly_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredItemDestroyed> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredItemDestroyed) -> anyhow::Result<()> {
        let room = item.assembly_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<ItemAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &ItemAction) -> anyhow::Result<()> {
        match action {
            ItemAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            ItemAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredItemMinted> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredItemMinted) -> anyhow::Result<()> {
        let room = item.assembly_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredItemWithdrawn> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredItemWithdrawn) -> anyhow::Result<()> {
        let room = item.assembly_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Locations
#[async_trait]
impl Routing<StoredLocationRevealed> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredLocationRevealed,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Status
#[async_trait]
impl Routing<StoredStatusChanged> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredStatusChanged) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

// Rifts
#[async_trait]
impl Routing<RiftAction> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, action: &RiftAction) -> anyhow::Result<()> {
        match action {
            RiftAction::Upsert(item) => {
                let room = item.id.clone();
                let event = format!("{}:{}", pipeline, "updated");
                self.send(room, event, item).await
            }
            RiftAction::Delete(id_str) => {
                let event = format!("{}:{}", pipeline, "deleted");
                self.send(id_str.clone(), event, id_str).await
            }
        }
    }
}

#[async_trait]
impl Routing<StoredRiftLocationBroadcasted> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredRiftLocationBroadcasted,
    ) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredRiftMiningStarted> for SocketIoTransport {
    async fn send(
        &self,
        pipeline: &'static str,
        item: &StoredRiftMiningStarted,
    ) -> anyhow::Result<()> {
        // Note: This messsage broadcasts to the in-game id of the rift since they didnt include the on-chain id.
        // Should get fixed in world v1 once available.
        let room = item.item_id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}

#[async_trait]
impl Routing<StoredRiftSpawned> for SocketIoTransport {
    async fn send(&self, pipeline: &'static str, item: &StoredRiftSpawned) -> anyhow::Result<()> {
        let room = item.id.clone();
        self.send(room, pipeline.to_string(), item).await
    }
}
