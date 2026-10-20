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
use crate::models::world::StoredCharacter;
use crate::transports::Transport;

use crate::AppContext;

pub struct CharacterHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<CharacterAction>>,
}

impl CharacterHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<CharacterAction>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_character(&self, obj: &Object) -> bool {
        let module_name = "character";
        let struct_name = "Character";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }
}

#[derive(Serialize, Clone, FieldCount)]
pub enum CharacterAction {
    Upsert(StoredCharacter),
    Delete(String),
}

#[async_trait]
impl Processor for CharacterHandler {
    const NAME: &'static str = "characters";
    type Value = CharacterAction;

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

                        if self.is_character(obj) {
                            let character = StoredCharacter::from_object(obj, checkpoint_updated)?;
                            results.push(CharacterAction::Upsert(character));
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

                        if self.is_character(obj) {
                            results.push(CharacterAction::Delete(object_id.to_string()));
                        }
                    }
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for CharacterHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            match value.clone() {
                CharacterAction::Upsert(character) => {
                    let current = batch.entry(character.id.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), CharacterAction::Delete(_)) {
                                continue;
                            }

                            let CharacterAction::Upsert(current) = entry.get() else {
                                continue;
                            };

                            if character.checkpoint_updated > current.checkpoint_updated {
                                entry.insert(value);
                            }
                        }
                        Entry::Vacant(entry) => {
                            entry.insert(value);
                        }
                    }
                }
                CharacterAction::Delete(id_str) => {
                    let current = batch.entry(id_str.clone());

                    match current {
                        Entry::Occupied(mut entry) => {
                            if matches!(entry.get(), CharacterAction::Upsert(_)) {
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
        use crate::schema::characters::dsl::*;

        let mut to_upsert: Vec<&StoredCharacter> = vec![];
        let mut to_delete: Vec<String> = vec![];

        for action in batch.values() {
            match action {
                CharacterAction::Upsert(character) => to_upsert.push(character),
                CharacterAction::Delete(id_str) => to_delete.push(id_str.clone()),
            }
        }

        if !to_upsert.is_empty() {
            diesel::insert_into(characters)
                .values(to_upsert)
                .on_conflict(id)
                .do_update()
                .set((
                    item_id.eq(excluded(item_id)),
                    tenant.eq(excluded(tenant)),
                    owner_cap_id.eq(excluded(owner_cap_id)),
                    owner_address.eq(excluded(owner_address)),
                    tribe_id.eq(excluded(tribe_id)),
                    name.eq(excluded(name)),
                    description.eq(excluded(description)),
                    url.eq(excluded(url)),
                    checkpoint_updated.eq(excluded(checkpoint_updated)),
                ))
                .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                .execute(conn)
                .await?;
        }

        // Deletions happen last in case an object was updated before deletion.
        if !to_delete.is_empty() {
            diesel::delete(characters)
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
