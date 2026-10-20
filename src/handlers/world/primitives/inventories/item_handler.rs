use async_trait::async_trait;
use serde::Serialize;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use diesel::prelude::*;
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
use crate::models::world::StoredItem;
use crate::transports::Transport;

use crate::AppContext;

pub struct ItemHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<ItemAction>>,
}

impl ItemHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<ItemAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_item(&self, obj: &Object) -> bool {
        let module_name = "inventory";
        let struct_name = "Item";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum ItemAction {
    Upsert(StoredItem),
    Delete(String),
}

#[async_trait]
impl Processor for ItemHandler {
    const NAME: &'static str = "items";
    type Value = ItemAction;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let mut results = vec![];

        for tx in &checkpoint.transactions {
            if !self.ctx.is_indexed_tx(tx, &checkpoint.object_set) {
                continue;
            }

            for change in &tx.effects.object_changes() {
                let object_id = change.id;

                match change.id_operation {
                    IDOperation::Created => {
                        let Some(version) = change.output_version else {
                            continue;
                        };

                        let key = ObjectKey(object_id, version);

                        let Some(obj) = checkpoint.object_set.get(&key) else {
                            continue;
                        };

                        if self.is_item(obj) {
                            let item = StoredItem::from_object(obj)?;
                            results.push(ItemAction::Upsert(item));
                        }
                    }
                    IDOperation::None => {} // Items are immutable, no need to handle updates.
                    IDOperation::Deleted => {
                        let Some(version) = change.input_version else {
                            continue;
                        };

                        let key = ObjectKey(object_id, version);

                        let Some(obj) = checkpoint.object_set.get(&key) else {
                            continue;
                        };

                        if self.is_item(obj) {
                            results.push(ItemAction::Delete(object_id.to_string()));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for ItemHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                ItemAction::Upsert(item) => {
                    let current = batch.entry(item.id.clone());

                    match current {
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                        _ => (),
                    }
                }
                ItemAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), ItemAction::Upsert(_)) {
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
        use crate::schema::items::dsl::*;

        let mut to_upsert: Vec<&StoredItem> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                ItemAction::Upsert(item) => to_upsert.push(item),
                ItemAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(items)
                .values(to_upsert)
                .on_conflict(id)
                .do_nothing()
                .execute(conn)
                .await?;
        }

        // Deletions happen last in case an object was updated before deletion.
        if !to_delete.is_empty() {
            diesel::delete(items)
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
