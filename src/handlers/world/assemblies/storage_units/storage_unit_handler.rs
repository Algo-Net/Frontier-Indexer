use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use diesel::prelude::*;
use diesel::query_dsl::methods::FilterDsl;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;

use sui_pg_db::FieldCount;
use sui_types::effects::{IDOperation, TransactionEffectsAPI};
use sui_types::object::Object;
use sui_types::object::Owner;
use sui_types::storage::ObjectKey;

use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::models::world::StoredExtensionFreeze;
use crate::models::world::StoredStorageUnit;
use crate::transports::Transport;

use crate::AppContext;

pub struct StorageUnitHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<StorageUnitAction>>,
}

impl StorageUnitHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<StorageUnitAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_storage_unit(&self, obj: &Object) -> bool {
        let module_name = "storage_unit";
        let struct_name = "StorageUnit";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }

    fn is_extension_freeze(&self, obj: &Object) -> bool {
        let key_module = "extension_freeze";
        let key_struct = "ExtensionFrozenKey";

        let value_module = "extension_freeze";
        let value_struct = "ExtensionFrozen";

        let Some(move_type) = obj.type_() else {
            return false;
        };

        if !move_type.is_dynamic_field() || move_type.type_params().len() != 2 {
            return false;
        };

        if !self
            .ctx
            .is_world_struct(move_type.type_params()[0].as_ref(), key_module, key_struct)
        {
            return false;
        }

        self.ctx.is_world_struct(
            move_type.type_params()[1].as_ref(),
            value_module,
            value_struct,
        )
    }

    fn get_extension_freeze_storage_unit(
        &self,
        obj: &Object,
        storage_units: &HashMap<String, Arc<StoredStorageUnit>>,
    ) -> Option<Arc<StoredStorageUnit>> {
        let Owner::ObjectOwner(owner_str) = obj.owner else {
            return None;
        };

        let owner_id = owner_str.to_string();

        storage_units.get(&owner_id).cloned()
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum StorageUnitAction {
    Upsert(StoredStorageUnit),
    Freeze(StoredExtensionFreeze),
    Delete(String),
}

#[async_trait]
impl Processor for StorageUnitHandler {
    const NAME: &'static str = "storage_units";
    type Value = StorageUnitAction;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let mut results = vec![];
        let checkpoint_updated = checkpoint.summary.sequence_number as i64;

        let mut storage_units = HashMap::new();
        let mut freezes: Vec<&Object> = Vec::new();

        for tx in &checkpoint.transactions {
            if !self.ctx.is_indexed_tx(tx, &checkpoint.object_set) {
                continue;
            }

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

                        if self.is_storage_unit(obj) {
                            let storage_unit =
                                StoredStorageUnit::from_object(obj, checkpoint_updated)?;
                            storage_units
                                .insert(storage_unit.id.clone(), Arc::new(storage_unit.clone()));
                            results.push(StorageUnitAction::Upsert(storage_unit));
                        }

                        if self.is_extension_freeze(obj) {
                            freezes.push(obj);
                        }
                    }
                    IDOperation::Deleted => {
                        results.push(StorageUnitAction::Delete(object_id.to_string()));
                    }
                }
            }
        }

        for obj in freezes {
            if let Some(storage_unit) = self.get_extension_freeze_storage_unit(obj, &storage_units)
            {
                let freeze = StoredExtensionFreeze::from_object(obj, storage_unit)?;
                results.push(StorageUnitAction::Freeze(freeze));
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for StorageUnitHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                StorageUnitAction::Upsert(storage_unit) => {
                    let current = batch.entry(storage_unit.id.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), StorageUnitAction::Delete(_)) {
                                continue;
                            }

                            let StorageUnitAction::Upsert(current) = entry.get() else {
                                continue;
                            };

                            if storage_unit.checkpoint_updated > current.checkpoint_updated {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                StorageUnitAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), StorageUnitAction::Upsert(_)) {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                StorageUnitAction::Freeze(freeze) => {
                    let current = batch.entry(freeze.id.clone());

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
        let mut to_upsert: Vec<&StoredStorageUnit> = vec![];
        let mut to_delete: Vec<String> = vec![];
        let mut to_freeze: Vec<&StoredExtensionFreeze> = vec![];

        for action in batch.values() {
            match action {
                StorageUnitAction::Upsert(storage_unit) => to_upsert.push(storage_unit),
                StorageUnitAction::Delete(id_str) => to_delete.push(id_str.clone()),
                StorageUnitAction::Freeze(freeze) => to_freeze.push(freeze),
            }
        }

        if !to_upsert.is_empty() {
            use crate::schema::storage_units::dsl::*;

            diesel::insert_into(storage_units)
                .values(to_upsert)
                .on_conflict(id)
                .do_update()
                .set((
                    item_id.eq(excluded(item_id)),
                    tenant.eq(excluded(tenant)),
                    type_id.eq(excluded(type_id)),
                    owner_cap_id.eq(excluded(owner_cap_id)),
                    location.eq(excluded(location)),
                    status.eq(excluded(status)),
                    energy_source_id.eq(excluded(energy_source_id)),
                    name.eq(excluded(name)),
                    description.eq(excluded(description)),
                    url.eq(excluded(url)),
                    package_id.eq(excluded(package_id)),
                    module_name.eq(excluded(module_name)),
                    struct_name.eq(excluded(struct_name)),
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;
        }

        if !to_freeze.is_empty() {
            use crate::schema::extension_freezes::dsl::*;

            diesel::insert_into(extension_freezes)
                .values(to_freeze)
                .on_conflict(id)
                .do_nothing()
                .execute(conn)
                .await?;
        }

        if !to_delete.is_empty() {
            use crate::schema::{extension_freezes, storage_units};

            diesel::delete(storage_units::dsl::storage_units)
                .filter(storage_units::dsl::id.eq_any(to_delete.clone()))
                .execute(conn)
                .await?;

            diesel::delete(extension_freezes::dsl::extension_freezes)
                .filter(extension_freezes::dsl::owner_id.eq_any(to_delete))
                .execute(conn)
                .await?;
        }

        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
