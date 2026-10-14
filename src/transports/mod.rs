use async_trait::async_trait;
use serde::Serialize;

use crate::handlers::world::*;
use crate::models::system::*;
use crate::models::world::*;

pub mod amqp;
pub mod init;
pub mod nats;
pub mod redis;
pub mod socketio;

pub use amqp::*;
pub use init::*;
pub use nats::*;
pub use redis::*;
pub use socketio::*;

#[async_trait]
pub trait Routing<I>: Send + Sync + 'static
where
    I: Serialize + Send + Sync,
{
    async fn send(&self, _pipeline: &'static str, _item: &I) -> anyhow::Result<()> {
        Ok(())
    }
}

mod private {
    use super::*;

    pub trait Router: Send + Sync + 'static {}

    impl<T> Router for T where
        T: super::Routing<AddressAliases>
            + super::Routing<StoredTransactionDigest>
            + super::Routing<StoredOwnerCapCreated>
            + super::Routing<OwnerCapAction>
            + super::Routing<StoredOwnerCapTransferred>
            + super::Routing<StoredAssemblyCreated>
            + super::Routing<AssemblyAction>
            + super::Routing<StoredExtensionFrozen>
            + super::Routing<GateConfigAction>
            + super::Routing<StoredGateCreated>
            + super::Routing<StoredGateExtensionAuthorized>
            + super::Routing<StoredGateExtensionRevoked>
            + super::Routing<GateAction>
            + super::Routing<StoredGateJumped>
            + super::Routing<StoredGateLinked>
            + super::Routing<GatePermitAction>
            + super::Routing<StoredGatePermitIssued>
            + super::Routing<StoredGateUnlinked>
            + super::Routing<StoredNetworkNodeCreated>
            + super::Routing<NetworkNodeAction>
            + super::Routing<StoredStorageUnitCreated>
            + super::Routing<StoredStorageUnitExtensionAuthorized>
            + super::Routing<StoredStorageUnitExtensionRevoked>
            + super::Routing<StorageUnitAction>
            + super::Routing<StoredTurretCreated>
            + super::Routing<StoredTurretExtensionAuthorized>
            + super::Routing<StoredTurretExtensionRevoked>
            + super::Routing<TurretAction>
            + super::Routing<StoredCharacterCreated>
            + super::Routing<CharacterAction>
            + super::Routing<StoredKillmail>
            + super::Routing<EnergyConfigAction>
            + super::Routing<StoredEnergyProductionStarted>
            + super::Routing<StoredEnergyProductionStopped>
            + super::Routing<StoredEnergyReleased>
            + super::Routing<StoredEnergyReserved>
            + super::Routing<StoredFuelBurningStarted>
            + super::Routing<StoredFuelBurningStopped>
            + super::Routing<StoredFuelBurningUpdated>
            + super::Routing<FuelConfigAction>
            + super::Routing<StoredFuelDeleted>
            + super::Routing<StoredFuelDeposited>
            + super::Routing<StoredFuelEfficiencyRemoved>
            + super::Routing<StoredFuelEfficiencySet>
            + super::Routing<StoredFuelWithdrawn>
            + super::Routing<InventoryAction>
            + super::Routing<StoredItemBurned>
            + super::Routing<StoredItemDeposited>
            + super::Routing<StoredItemDestroyed>
            + super::Routing<ItemAction>
            + super::Routing<StoredItemMinted>
            + super::Routing<StoredItemWithdrawn>
            + super::Routing<StoredLocationRevealed>
            + super::Routing<StoredStatusChanged>
            + super::Routing<RiftAction>
            + super::Routing<StoredRiftLocationBroadcasted>
            + super::Routing<StoredRiftMiningStarted>
            + super::Routing<StoredRiftSpawned>
            /* Add application specific routing requirements for transports here */
    {
    }
}

#[async_trait]
pub trait Transport<I>: private::Router {
    async fn send(&self, pipeline: &'static str, item: &I) -> anyhow::Result<()>;
}

#[async_trait]
impl<T, I> Transport<I> for T
where
    T: private::Router + Routing<I>,
    I: Serialize + Send + Sync + 'static,
{
    async fn send(&self, pipeline: &'static str, item: &I) -> anyhow::Result<()> {
        <Self as Routing<I>>::send(self, pipeline, item).await
    }
}
