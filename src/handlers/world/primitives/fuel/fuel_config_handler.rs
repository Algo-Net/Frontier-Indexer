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
use crate::models::world::MoveFuelConfig;
use crate::models::world::StoredFuelConfig;
use crate::transports::Transport;

use crate::AppContext;

pub struct FuelConfigHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<FuelConfigAction>>,
}

impl FuelConfigHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<FuelConfigAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_fuel_config(&self, obj: &Object) -> bool {
        let module_name = "fuel";
        let struct_name = "FuelConfig";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }

    fn is_fuel_config_entry(
        &self,
        obj: &Object,
        table_updates: &HashMap<String, Arc<StoredTableRecord>>,
    ) -> Option<Arc<StoredTableRecord>> {
        let owner_module_name = "fuel";
        let owner_struct_name = "FuelConfig";

        let Some(move_type) = obj.type_() else {
            return None;
        };

        if !move_type.is_dynamic_field() || move_type.type_params().len() <= 1 {
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

        // Check the entry agains the table registry.
        let Some(table) = self.ctx.tables.get_record(&owner_id) else {
            return None;
        };

        let package_id = AccountAddress::from_str(&table.package_id)
            .expect("Failed to parse package_id stored in table registry.");

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
pub enum FuelConfigAction {
    Register(StoredTableRecord),
    Upsert(StoredFuelConfig),
    Delete(String),
}

#[async_trait]
impl Processor for FuelConfigHandler {
    const NAME: &'static str = "fuel_config";
    type Value = FuelConfigAction;

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

                        if self.is_fuel_config(obj) {
                            let move_obj =
                                obj.data.try_as_move().expect("Object is not a Move object");
                            let bytes = move_obj.contents();

                            let fuel_config: MoveFuelConfig = bcs::from_bytes(bytes)
                                .expect("Failed to deserialize FuelConfig object");

                            let move_type = move_obj.type_();

                            let tag = move_type
                                .other()
                                .expect("Failed to get appropriate move type for FuelConfig");

                            let table_id = fuel_config.fuel_efficiency.id.to_canonical_string(true);

                            let table_record = StoredTableRecord {
                                table_id: table_id.clone(),
                                parent_id: fuel_config.id.to_hex(),
                                package_id: tag.address.to_canonical_string(true),
                                module_name: tag.module.to_string(),
                                struct_name: tag.name.to_string(),
                                key_type: TypeTag::U64.to_string(),
                                value_type: TypeTag::U64.to_string(),
                                checkpoint_updated,
                            };

                            table_updates.insert(table_id, Arc::new(table_record.clone()));
                            results.push(FuelConfigAction::Register(table_record));
                        }
                    }
                    IDOperation::Deleted => {
                        results.push(FuelConfigAction::Delete(object_id.to_string()));
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

                        if let Some(table) = self.is_fuel_config_entry(obj, &table_updates) {
                            let config = StoredFuelConfig::from_object(
                                obj,
                                table.table_id.clone(),
                                checkpoint_updated,
                            )?;

                            results.push(FuelConfigAction::Upsert(config));
                        }
                    }
                    IDOperation::Deleted => {} // Already handled in first pass.
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for FuelConfigHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                FuelConfigAction::Register(table) => {
                    let current = batch.entry(table.table_id.clone());

                    match current {
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        _ => (),
                    }
                }
                FuelConfigAction::Upsert(config) => {
                    let current = batch.entry(config.type_id.to_string());

                    match current {
                        Entry::Occupied(mut entry) => {
                            let FuelConfigAction::Upsert(current) = entry.get() else {
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
                FuelConfigAction::Delete(id_str) => {
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
        use crate::schema::fuel_config::dsl::*;

        let mut to_upsert: Vec<&StoredFuelConfig> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                FuelConfigAction::Register(table) => {
                    self.ctx.tables.add_table(conn, table).await?;
                }
                FuelConfigAction::Upsert(config) => to_upsert.push(config),
                FuelConfigAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(fuel_config)
                .values(to_upsert.clone())
                .on_conflict((type_id, table_id))
                .do_update()
                .set((
                    efficiency.eq(excluded(efficiency)),
                    entry_object_id.eq(excluded(entry_object_id)),
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;

            for record in to_upsert {
                self.ctx.fuels.add_fuel(record);
            }
        }

        if !to_delete.is_empty() {
            let ids = diesel::delete(fuel_config)
                .filter(entry_object_id.eq_any(to_delete))
                .returning(type_id)
                .get_results(conn)
                .await?;

            for id in ids {
                self.ctx.fuels.remove_fuel(id);
            }
        }

        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
