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
use crate::models::world::StoredInventory;
use crate::models::world::StoredInventoryEntry;
use crate::transports::Transport;

use crate::AppContext;

pub struct InventoryHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<InventoryAction>>,
}

impl InventoryHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<InventoryAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_inventory(&self, obj: &Object) -> bool {
        let value_module = "inventory";
        let value_struct = "Inventory";

        let Some(move_type) = obj.type_() else {
            return false;
        };

        if !move_type.is_dynamic_field() || move_type.type_params().len() != 2 {
            return false;
        }

        let type_params = move_type.type_params();

        if !self
            .ctx
            .is_world_struct(type_params[1].as_ref(), value_module, value_struct)
        {
            return false;
        }

        let TypeTag::Struct(s_tag) = type_params[0].as_ref() else {
            return false;
        };

        s_tag.address.to_hex_literal() == "0x2"
            && s_tag.module.as_str() == "object"
            && s_tag.name.as_str() == "ID"
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum InventoryAction {
    Upsert(StoredInventory),
    Delete(String),
}

#[async_trait]
impl Processor for InventoryHandler {
    const NAME: &'static str = "inventories";
    type Value = InventoryAction;

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

                        if self.is_inventory(obj) {
                            let Owner::ObjectOwner(owner) = obj.owner else {
                                continue;
                            };

                            let parent_id = owner.to_string();
                            let inventory =
                                StoredInventory::from_object(obj, parent_id, checkpoint_updated);
                            results.push(InventoryAction::Upsert(inventory));
                        }
                    }
                    IDOperation::Deleted => {
                        results.push(InventoryAction::Delete(object_id.to_string()));
                    }
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for InventoryHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                InventoryAction::Upsert(inventory) => {
                    let current = batch.entry(inventory.id.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), InventoryAction::Delete(_)) {
                                continue;
                            }

                            let InventoryAction::Upsert(current) = entry.get() else {
                                continue;
                            };

                            if inventory.checkpoint_updated > current.checkpoint_updated {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                InventoryAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), InventoryAction::Upsert(_)) {
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
        let mut to_upsert: Vec<&StoredInventory> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                InventoryAction::Upsert(inventory) => to_upsert.push(inventory),
                InventoryAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            {
                use crate::schema::inventories::dsl::*;
                diesel::insert_into(inventories)
                    .values(to_upsert.clone())
                    .on_conflict(id)
                    .do_update()
                    .set((
                        parent_id.eq(excluded(parent_id)),
                        capacity_used.eq(excluded(capacity_used)),
                        capacity_max.eq(excluded(capacity_max)),
                        checkpoint_updated.eq(excluded(checkpoint_updated)),
                    ))
                    .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                    .execute(conn)
                    .await?;
            }

            for inventory in to_upsert {
                use crate::schema::inventory_entries::dsl::*;

                let keys: Vec<i64> = inventory.entries.keys().cloned().collect();
                let values: Vec<StoredInventoryEntry> =
                    inventory.entries.values().cloned().collect();

                if !values.is_empty() {
                    diesel::insert_into(inventory_entries)
                        .values(values)
                        .on_conflict((inventory_id, type_id))
                        .do_update()
                        .set((
                            item_id.eq(excluded(item_id)),
                            volume.eq(excluded(volume)),
                            quantity.eq(excluded(quantity)),
                            checkpoint_updated.eq(excluded(checkpoint_updated)),
                        ))
                        .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                        .execute(conn)
                        .await?;
                }

                diesel::delete(inventory_entries)
                    .filter(inventory_id.eq(inventory.id.clone()))
                    .filter(type_id.ne_all(keys))
                    .filter(checkpoint_updated.le(inventory.checkpoint_updated))
                    .execute(conn)
                    .await?;
            }
        }

        if !to_delete.is_empty() {
            {
                use crate::schema::inventories::dsl::*;
                diesel::delete(inventories)
                    .filter(id.eq_any(to_delete.clone()))
                    .execute(conn)
                    .await?;
            }

            {
                use crate::schema::inventory_entries::dsl::*;
                diesel::delete(inventory_entries)
                    .filter(inventory_id.eq_any(to_delete))
                    .execute(conn)
                    .await?;
            }
        }

        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
