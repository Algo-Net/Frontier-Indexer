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
use crate::models::world::StoredAssembly;
use crate::transports::Transport;

use crate::AppContext;

pub struct AssemblyHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<AssemblyAction>>,
}

impl AssemblyHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<AssemblyAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_assembly(&self, obj: &Object) -> bool {
        let module_name = "assembly";
        let struct_name = "Assembly";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum AssemblyAction {
    Upsert(StoredAssembly),
    Delete(String),
}

#[async_trait]
impl Processor for AssemblyHandler {
    const NAME: &'static str = "assemblies";
    type Value = AssemblyAction;

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

                        if self.is_assembly(obj) {
                            let assembly = StoredAssembly::from_object(obj, checkpoint_updated)?;
                            results.push(AssemblyAction::Upsert(assembly));
                        }
                    }
                    IDOperation::Deleted => {
                        results.push(AssemblyAction::Delete(object_id.to_string()));
                    }
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for AssemblyHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                AssemblyAction::Upsert(assembly) => {
                    let current = batch.entry(assembly.id.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), AssemblyAction::Delete(_)) {
                                continue;
                            }

                            let AssemblyAction::Upsert(current) = entry.get() else {
                                continue;
                            };

                            if assembly.checkpoint_updated > current.checkpoint_updated {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                AssemblyAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), AssemblyAction::Upsert(_)) {
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
        use crate::schema::assemblies::dsl::*;

        let mut to_upsert: Vec<&StoredAssembly> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                AssemblyAction::Upsert(assembly) => to_upsert.push(assembly),
                AssemblyAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(assemblies)
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
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;
        }

        if !to_delete.is_empty() {
            diesel::delete(assemblies)
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
