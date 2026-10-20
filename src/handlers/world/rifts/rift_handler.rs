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
use crate::models::world::StoredRift;
use crate::transports::Transport;

use crate::AppContext;

pub struct RiftHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<RiftAction>>,
}

impl RiftHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<RiftAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_rift(&self, obj: &Object) -> bool {
        let module_name = "rift";
        let struct_name = "Rift";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum RiftAction {
    Upsert(StoredRift),
    Delete(String),
}

#[async_trait]
impl Processor for RiftHandler {
    const NAME: &'static str = "rifts";
    type Value = RiftAction;

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

                        if self.is_rift(obj) {
                            let rift = StoredRift::from_object(obj, checkpoint_updated)?;
                            results.push(RiftAction::Upsert(rift));
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

                        if self.is_rift(obj) {
                            results.push(RiftAction::Delete(object_id.to_string()));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for RiftHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                RiftAction::Upsert(rift) => {
                    let current = batch.entry(rift.id.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), RiftAction::Delete(_)) {
                                continue;
                            }

                            let RiftAction::Upsert(current) = entry.get() else {
                                continue;
                            };

                            if rift.checkpoint_updated > current.checkpoint_updated {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                RiftAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), RiftAction::Upsert(_)) {
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
        use crate::schema::rifts::dsl::*;

        let mut to_upsert: Vec<&StoredRift> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                RiftAction::Upsert(rift) => to_upsert.push(rift),
                RiftAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(rifts)
                .values(to_upsert)
                .on_conflict(id)
                .do_update()
                .set((
                    item_id.eq(excluded(item_id)),
                    tenant.eq(excluded(tenant)),
                    location.eq(excluded(location)),
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;
        }

        if !to_delete.is_empty() {
            diesel::delete(rifts)
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
