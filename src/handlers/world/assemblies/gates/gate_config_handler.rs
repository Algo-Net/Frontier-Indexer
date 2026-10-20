use anyhow::Context;
use async_trait::async_trait;
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

use move_core_types::account_address::AccountAddress;
use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::models::system::StoredTableRecord;
use crate::models::world::MoveGateConfig;
use crate::models::world::StoredGateConfig;
use crate::transports::Transport;

use crate::AppContext;

pub struct GateConfigHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<GateConfigAction>>,
}

impl GateConfigHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<GateConfigAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_gate_config(&self, obj: &Object) -> bool {
        let module_name = "gate";
        let struct_name = "GateConfig";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }

    fn is_gate_config_entry(
        &self,
        obj: &Object,
        table_updates: &HashMap<String, Arc<StoredTableRecord>>,
    ) -> Option<Arc<StoredTableRecord>> {
        let owner_module_name = "gate";
        let owner_struct_name = "GateConfig";

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
pub enum GateConfigAction {
    Register(StoredTableRecord),
    Upsert(StoredGateConfig),
    Delete(String),
}

#[async_trait]
impl Processor for GateConfigHandler {
    const NAME: &'static str = "gate_config";
    type Value = GateConfigAction;

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

                        if self.is_gate_config(obj) {
                            let move_obj = obj
                                .data
                                .try_as_move()
                                .context("Object is not a Move object")?;
                            let bytes = move_obj.contents();

                            let gate_config: MoveGateConfig = bcs::from_bytes(bytes)
                                .context("Failed to deserialize Gate Config object")?;

                            let move_type = move_obj.type_();

                            let tag = move_type
                                .other()
                                .context("Failed to get appropriate move type for GateConfig")?;

                            let table_id = gate_config
                                .max_distance_by_type
                                .id
                                .to_canonical_string(true);

                            let table_record = StoredTableRecord {
                                table_id: table_id.clone(),
                                parent_id: gate_config.id.to_hex(),
                                package_id: tag.address.to_canonical_string(true),
                                module_name: tag.module.to_string(),
                                struct_name: tag.name.to_string(),
                                key_type: TypeTag::U64.to_string(),
                                value_type: TypeTag::U64.to_string(),
                                checkpoint_updated,
                            };

                            table_updates.insert(table_id, Arc::new(table_record.clone()));
                            results.push(GateConfigAction::Register(table_record));
                        }
                    }
                    IDOperation::Deleted => {
                        let Some(version) = change.input_version else {
                            continue;
                        };

                        let key = ObjectKey(object_id, version);

                        let Some(obj) = checkpoint.object_set.get(&key) else {
                            continue;
                        };

                        if self.is_gate_config(obj) {
                            results.push(GateConfigAction::Delete(object_id.to_string()));
                        }
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

                        if let Some(table) = self.is_gate_config_entry(obj, &table_updates) {
                            let config = StoredGateConfig::from_object(
                                obj,
                                table.table_id.clone(),
                                checkpoint_updated,
                            )?;

                            results.push(GateConfigAction::Upsert(config));
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
impl Handler for GateConfigHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                GateConfigAction::Register(table) => {
                    let current = batch.entry(table.table_id.clone());

                    match current {
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        _ => (),
                    }
                }
                GateConfigAction::Upsert(config) => {
                    let current = batch.entry(config.type_id.to_string());

                    match current {
                        Entry::Occupied(mut entry) => {
                            let GateConfigAction::Upsert(current) = entry.get() else {
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
                GateConfigAction::Delete(id_str) => {
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
        use crate::schema::gate_config::dsl::*;

        let mut to_upsert: Vec<&StoredGateConfig> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                GateConfigAction::Register(table) => {
                    self.ctx.tables.add_table(conn, table).await?;
                }
                GateConfigAction::Upsert(config) => to_upsert.push(config),
                GateConfigAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(gate_config)
                .values(to_upsert)
                .on_conflict((type_id, table_id))
                .do_update()
                .set((
                    distance.eq(excluded(distance)),
                    entry_object_id.eq(excluded(entry_object_id)),
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;
        }

        if !to_delete.is_empty() {
            diesel::delete(gate_config)
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
