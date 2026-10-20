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
use sui_types::storage::ObjectKey;

use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::models::world::StoredOwnerCap;
use crate::transports::Transport;

use crate::AppContext;

pub struct OwnerCapHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<OwnerCapAction>>,
}

impl OwnerCapHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<OwnerCapAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_owner_cap(&self, obj: &Object) -> bool {
        let module_name = "access";
        let struct_name = "OwnerCap";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum OwnerCapAction {
    Upsert(StoredOwnerCap),
    Delete(String),
}

#[async_trait]
impl Processor for OwnerCapHandler {
    const NAME: &'static str = "owner_caps";
    type Value = OwnerCapAction;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let mut results = vec![];
        let checkpoint_updated = checkpoint.summary.sequence_number as i64;

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

                        if self.is_owner_cap(obj) {
                            let owner_cap = StoredOwnerCap::from_object(obj, checkpoint_updated)?;
                            results.push(OwnerCapAction::Upsert(owner_cap));
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

                        if self.is_owner_cap(obj) {
                            results.push(OwnerCapAction::Delete(object_id.to_string()));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for OwnerCapHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                OwnerCapAction::Upsert(owner_cap) => {
                    let current = batch.entry(owner_cap.id.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), OwnerCapAction::Delete(_)) {
                                continue;
                            }

                            let OwnerCapAction::Upsert(current) = entry.get() else {
                                continue;
                            };

                            if owner_cap.checkpoint_updated > current.checkpoint_updated {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                OwnerCapAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), OwnerCapAction::Upsert(_)) {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
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
        use crate::schema::owner_caps::dsl::*;

        let mut to_upsert: Vec<&StoredOwnerCap> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                OwnerCapAction::Upsert(owner_cap) => to_upsert.push(owner_cap),
                OwnerCapAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(owner_caps)
                .values(to_upsert)
                .on_conflict(id)
                .do_update()
                .set((
                    object_id.eq(excluded(object_id)),
                    owner_address.eq(excluded(owner_address)),
                    package_id.eq(excluded(package_id)),
                    module_name.eq(excluded(module_name)),
                    struct_name.eq(excluded(struct_name)),
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;
        }

        // Deletions happen last in case an object was updated before deletion.
        if !to_delete.is_empty() {
            diesel::delete(owner_caps)
                .filter(id.eq_any(to_delete))
                .execute(conn)
                .await?;
        }

        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
