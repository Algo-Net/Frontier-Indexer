use anyhow::Context;
use async_trait::async_trait;
use move_core_types::account_address::AccountAddress;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::str::FromStr;
use std::sync::Arc;

use diesel::prelude::*;
use diesel::query_dsl::methods::FilterDsl;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;

use sui_pg_db::FieldCount;
use sui_types::TypeTag;
use sui_types::effects::{IDOperation, TransactionEffectsAPI};
use sui_types::object::Object;
use sui_types::object::Owner;
use sui_types::storage::ObjectKey;

use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::models::system::StoredTableRecord;
use crate::models::world::MoveEnergyConfig;
use crate::models::world::StoredEnergyConfig;
use crate::transports::Transport;

use crate::AppContext;

pub struct EnergyConfigHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<EnergyConfigAction>>,
}

impl EnergyConfigHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<EnergyConfigAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_energy_config(&self, obj: &Object) -> bool {
        let module_name = "energy";
        let struct_name = "EnergyConfig";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }

    fn is_energy_config_entry(
        &self,
        obj: &Object,
        table_updates: &HashMap<String, Arc<StoredTableRecord>>,
    ) -> Option<Arc<StoredTableRecord>> {
        let owner_module_name = "energy";
        let owner_struct_name = "EnergyConfig";

        let Some(move_type) = obj.type_() else {
            return None;
        };

        if !move_type.is_dynamic_field() || move_type.type_params().len() != 2 {
            return None;
        }

        if !matches!(move_type.type_params()[0].as_ref(), TypeTag::U64) {
            return None;
        }

        if !matches!(move_type.type_params()[1].as_ref(), TypeTag::U64) {
            return None;
        }

        let Owner::ObjectOwner(owner_str) = obj.owner else {
            return None;
        };

        let owner_id = owner_str.to_string();

        // Check the entry against tables added in the same checkpoint.
        if let Some(table) = table_updates.get(&owner_id) {
            return Some(table.clone());
        }

        // Check the entry against the table registry.
        let Some(table) = self.ctx.tables.get_record(&owner_id) else {
            return None;
        };

        let package_id = AccountAddress::from_str(&table.package_id).ok()?;

        if table.module_name != owner_module_name {
            return None;
        }

        if table.struct_name != owner_struct_name {
            return None;
        }

        if !self.ctx.world_packages.contains(&package_id) {
            return None;
        }

        Some(table)
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum EnergyConfigAction {
    Register(StoredTableRecord),
    Upsert(StoredEnergyConfig),
    Delete(String),
}

#[async_trait]
impl Processor for EnergyConfigHandler {
    const NAME: &'static str = "energy_config";
    type Value = EnergyConfigAction;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let mut results = vec![];
        let checkpoint_updated = checkpoint.summary.sequence_number as i64;

        let mut table_updates = HashMap::new();

        for tx in &checkpoint.transactions {
            if !self.ctx.is_indexed_tx(tx, &checkpoint.object_set) {
                continue;
            }

            // First pass to catch all tables and deletions.
            for change in &tx.effects.object_changes() {
                let object_id = change.id;

                match change.id_operation {
                    IDOperation::Created | IDOperation::None => {
                        let Some(version) = change.output_version else {
                            continue;
                        };

                        let key = ObjectKey(object_id, version);

                        let Some(obj) = checkpoint.object_set.get(&key) else {
                            continue;
                        };

                        if self.is_energy_config(obj) {
                            let move_obj = obj
                                .data
                                .try_as_move()
                                .context("Object is not a Move object")?;

                            let bytes = move_obj.contents();

                            let energy_config: MoveEnergyConfig = bcs::from_bytes(bytes)
                                .context("Failed to deserialize EnergyConfig object")?;

                            let move_type = move_obj.type_();

                            let tag = move_type
                                .other()
                                .context("Failed to get appropriate move type for EnergyConfig")?;

                            let table_id =
                                energy_config.assembly_energy.id.to_canonical_string(true);

                            let table_record = StoredTableRecord {
                                table_id: table_id.clone(),
                                parent_id: energy_config.id.to_hex(),
                                package_id: tag.address.to_canonical_string(true),
                                module_name: tag.module.to_string(),
                                struct_name: tag.name.to_string(),
                                key_type: TypeTag::U64.to_string(),
                                value_type: TypeTag::U64.to_string(),
                                checkpoint_updated,
                            };

                            table_updates.insert(table_id, Arc::new(table_record.clone()));
                            results.push(EnergyConfigAction::Register(table_record));
                        }
                    }
                    IDOperation::Deleted => {
                        results.push(EnergyConfigAction::Delete(object_id.to_string()));
                    }
                }
            }

            // Second pass to catch all table entries.
            for change in &tx.effects.object_changes() {
                let object_id = change.id;

                match change.id_operation {
                    IDOperation::Created | IDOperation::None => {
                        let Some(version) = change.output_version else {
                            continue;
                        };

                        let key = ObjectKey(object_id, version);

                        let Some(obj) = checkpoint.object_set.get(&key) else {
                            continue;
                        };

                        if let Some(table) = self.is_energy_config_entry(obj, &table_updates) {
                            let config = StoredEnergyConfig::from_object(
                                obj,
                                table.table_id.clone(),
                                checkpoint_updated,
                            )?;

                            results.push(EnergyConfigAction::Upsert(config));
                        }
                    }
                    IDOperation::Deleted => {} // Already handled in first pass
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for EnergyConfigHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                EnergyConfigAction::Register(table) => {
                    let current = batch.entry(table.table_id.clone());

                    match current {
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        _ => (),
                    }
                }
                EnergyConfigAction::Upsert(config) => {
                    let current = batch.entry(config.type_id.to_string());

                    match current {
                        Entry::Occupied(mut entry) => {
                            let EnergyConfigAction::Upsert(current) = entry.get() else {
                                continue;
                            };

                            if config.checkpoint_updated > current.checkpoint_updated {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                EnergyConfigAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        _ => (),
                    }
                }
            }
        }
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        use crate::schema::energy_config::dsl::*;

        let mut to_upsert: Vec<&StoredEnergyConfig> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                EnergyConfigAction::Register(table) => {
                    self.ctx.tables.add_table(conn, table).await?;
                }
                EnergyConfigAction::Upsert(config) => to_upsert.push(config),
                EnergyConfigAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(energy_config)
                .values(to_upsert)
                .on_conflict((type_id, table_id))
                .do_update()
                .set((
                    energy_cost.eq(excluded(energy_cost)),
                    entry_object_id.eq(excluded(entry_object_id)),
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;
        }

        if !to_delete.is_empty() {
            diesel::delete(energy_config)
                .filter(entry_object_id.eq_any(to_delete))
                .execute(conn)
                .await?;
        }

        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
