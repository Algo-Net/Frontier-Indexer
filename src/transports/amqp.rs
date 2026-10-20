use tokio::sync::RwLock;

use async_trait::async_trait;
use serde::Serialize;

use crate::handlers::world::*;
use crate::models::system::*;
use crate::models::world::*;
use crate::transports::Routing;

pub struct AmqpTransport {
    pool: deadpool_lapin::Pool,
    channel: RwLock<Option<lapin::Channel>>,
    exchange: String,
}

impl AmqpTransport {
    pub async fn connect(
        addr: &str,
        exchange: impl Into<String>,
        pool_size: usize,
    ) -> anyhow::Result<Self> {
        let exchange = exchange.into();
        let mut cfg = deadpool_lapin::Config::default();

        cfg.url = Some(addr.to_string());
        cfg.pool = Some(deadpool_lapin::PoolConfig {
            max_size: pool_size,
            ..Default::default()
        });

        let pool = cfg.create_pool(Some(deadpool_lapin::Runtime::Tokio1))?;

        let transport = Self {
            pool,
            channel: RwLock::new(None),
            exchange,
        };

        transport.get_channel().await?;

        Ok(transport)
    }

    async fn get_channel(&self) -> anyhow::Result<lapin::Channel> {
        {
            let guard = self.channel.read().await;
            if let Some(channel) = guard.as_ref() {
                if channel.status().connected() {
                    return Ok(channel.clone());
                }
            }
        }

        let mut guard = self.channel.write().await;
        if let Some(channel) = guard.as_ref() {
            if channel.status().connected() {
                return Ok(channel.clone());
            }
        }

        let connection = self.pool.get().await?;
        let channel = connection.create_channel().await?;

        channel
            .exchange_declare(
                &self.exchange,
                lapin::ExchangeKind::Topic,
                lapin::options::ExchangeDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                lapin::types::FieldTable::default(),
            )
            .await?;

        *guard = Some(channel.clone());
        Ok(channel)
    }

    async fn send<I: Serialize + Send + Sync + 'static>(
        &self,
        routing: String,
        item: &I,
    ) -> anyhow::Result<()> {
        let amqp_key = format!("indexer.{}", routing);
        let payload = serde_json::to_vec(item)?;
        let channel = self.get_channel().await?;

        channel
            .basic_publish(
                &self.exchange,
                &amqp_key,
                lapin::options::BasicPublishOptions::default(),
                &payload,
                lapin::BasicProperties::default()
                    .with_content_type("application/json".into())
                    .with_delivery_mode(2),
            )
            .await?
            .await?; // second await = broker publisher-confirm ack

        Ok(())
    }
}

// Address Aliases
#[async_trait]
impl Routing<AddressAliases> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &AddressAliases) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "address_aliases", item.id, "updated");
        self.send(routing, item).await
    }
}

// Transaction Digests
#[async_trait]
impl Routing<StoredTransactionDigest> for AmqpTransport {
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
impl Routing<StoredOwnerCapCreated> for AmqpTransport {
    async fn send(
        &self,
        _pipeline: &'static str,
        item: &StoredOwnerCapCreated,
    ) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "owner_cap", item.object_id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<OwnerCapAction> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, action: &OwnerCapAction) -> anyhow::Result<()> {
        match action {
            OwnerCapAction::Upsert(item) => {
                let routing = format!("{}.{}.{}", "owner_cap", item.object_id, "updated");
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
impl Routing<StoredOwnerCapTransferred> for AmqpTransport {
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
impl Routing<StoredAssemblyCreated> for AmqpTransport {
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
impl Routing<AssemblyAction> for AmqpTransport {
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
impl Routing<StoredExtensionFrozen> for AmqpTransport {
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
impl Routing<GateConfigAction> for AmqpTransport {
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
impl Routing<StoredGateCreated> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateCreated) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.id, "created");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredGateExtensionAuthorized> for AmqpTransport {
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
impl Routing<StoredGateExtensionRevoked> for AmqpTransport {
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
impl Routing<GateAction> for AmqpTransport {
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
impl Routing<StoredGateJumped> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateJumped) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.departure_id, "jumped");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "gate", item.destination_id, "jumped");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredGateLinked> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateLinked) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.departure_id, "linked");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "gate", item.destination_id, "linked");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<GatePermitAction> for AmqpTransport {
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
impl Routing<StoredGatePermitIssued> for AmqpTransport {
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
impl Routing<StoredGateUnlinked> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredGateUnlinked) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "gate", item.departure_id, "unlinked");
        _ = self.send(routing, item).await;

        let routing = format!("{}.{}.{}", "gate", item.destination_id, "unlinked");
        self.send(routing, item).await
    }
}

// Network Nodes
#[async_trait]
impl Routing<StoredNetworkNodeCreated> for AmqpTransport {
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
impl Routing<NetworkNodeAction> for AmqpTransport {
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
impl Routing<StoredStorageUnitCreated> for AmqpTransport {
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
impl Routing<StoredStorageUnitExtensionAuthorized> for AmqpTransport {
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
impl Routing<StoredStorageUnitExtensionRevoked> for AmqpTransport {
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
impl Routing<StorageUnitAction> for AmqpTransport {
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
impl Routing<StoredTurretCreated> for AmqpTransport {
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
impl Routing<StoredTurretExtensionAuthorized> for AmqpTransport {
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
impl Routing<StoredTurretExtensionRevoked> for AmqpTransport {
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
impl Routing<TurretAction> for AmqpTransport {
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
impl Routing<StoredCharacterCreated> for AmqpTransport {
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
impl Routing<CharacterAction> for AmqpTransport {
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
impl Routing<StoredKillmail> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredKillmail) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "killmail", item.id, "created");
        self.send(routing, item).await
    }
}

// Energy
#[async_trait]
impl Routing<EnergyConfigAction> for AmqpTransport {
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
impl Routing<StoredEnergyProductionStarted> for AmqpTransport {
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
impl Routing<StoredEnergyProductionStopped> for AmqpTransport {
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
impl Routing<StoredEnergyReleased> for AmqpTransport {
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
impl Routing<StoredEnergyReserved> for AmqpTransport {
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
impl Routing<StoredFuelBurningStarted> for AmqpTransport {
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
impl Routing<StoredFuelBurningStopped> for AmqpTransport {
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
impl Routing<StoredFuelBurningUpdated> for AmqpTransport {
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
impl Routing<FuelConfigAction> for AmqpTransport {
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
impl Routing<StoredFuelDeleted> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredFuelDeleted) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "fuel", item.id, "deleted");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredFuelDeposited> for AmqpTransport {
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
impl Routing<StoredFuelEfficiencyRemoved> for AmqpTransport {
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
impl Routing<StoredFuelEfficiencySet> for AmqpTransport {
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
impl Routing<StoredFuelWithdrawn> for AmqpTransport {
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
impl Routing<InventoryAction> for AmqpTransport {
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
impl Routing<StoredItemBurned> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredItemBurned) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "item", item.assembly_id, "burned");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredItemDeposited> for AmqpTransport {
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
impl Routing<StoredItemDestroyed> for AmqpTransport {
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
impl Routing<ItemAction> for AmqpTransport {
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
impl Routing<StoredItemMinted> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredItemMinted) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "item", item.assembly_id, "minted");
        self.send(routing, item).await
    }
}

#[async_trait]
impl Routing<StoredItemWithdrawn> for AmqpTransport {
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
impl Routing<StoredLocationRevealed> for AmqpTransport {
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
impl Routing<StoredStatusChanged> for AmqpTransport {
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
impl Routing<RiftAction> for AmqpTransport {
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
impl Routing<StoredRiftLocationBroadcasted> for AmqpTransport {
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
impl Routing<StoredRiftMiningStarted> for AmqpTransport {
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
impl Routing<StoredRiftSpawned> for AmqpTransport {
    async fn send(&self, _pipeline: &'static str, item: &StoredRiftSpawned) -> anyhow::Result<()> {
        let routing = format!("{}.{}.{}", "rift", item.id, "spawned");
        self.send(routing, item).await
    }
}
