use async_trait::async_trait;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;

use diesel::prelude::*;
use diesel::query_dsl::methods::FilterDsl;
use diesel::upsert::excluded;
use diesel_async::RunQueryDsl;

use sui_types::effects::{IDOperation, TransactionEffectsAPI};
use sui_types::object::Object;
use sui_types::storage::ObjectKey;

use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::models::system::AddressAliases;
use crate::transports::Transport;

use crate::AppContext;

pub struct AddressAliasHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<AddressAliases>>,
}

impl AddressAliasHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<AddressAliases>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_account_aliases(&self, obj: &Object) -> bool {
        let module_name = "address_alias";
        let struct_name = "AddressAliases";
        self.ctx.is_sui_object(obj, module_name, struct_name)
    }
}

#[async_trait]
impl Processor for AddressAliasHandler {
    const NAME: &'static str = "address_aliases";
    type Value = AddressAliases;

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

                        if self.is_account_aliases(obj) {
                            let address_aliases =
                                AddressAliases::from_object(obj, checkpoint_updated)?;
                            results.push(address_aliases);
                        }
                    }
                    IDOperation::Deleted => {}
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for AddressAliasHandler {
    type Store = Db;
    type Batch = HashMap<String, Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        for value in values {
            let current = batch.entry(value.id.clone());

            match current {
                Entry::Occupied(mut entry) => {
                    if value.checkpoint_updated > entry.get().checkpoint_updated {
                        entry.insert(value);
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(value);
                }
            }
        }
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        use crate::schema::address_aliases::dsl::*;

        if !batch.is_empty() {
            for entry in batch.values() {
                let aliases = entry.aliases.clone();
                let values = entry.clone().to_aliases();

                diesel::insert_into(address_aliases)
                    .values(values)
                    .on_conflict((address, alias))
                    .do_update()
                    .set(checkpoint_updated.eq(excluded(checkpoint_updated)))
                    .filter(checkpoint_updated.lt(excluded(checkpoint_updated)))
                    .execute(conn)
                    .await?;

                diesel::delete(address_aliases)
                    .filter(address.eq(entry.id.clone()))
                    .filter(alias.ne_all(aliases))
                    .filter(checkpoint_updated.lt(entry.checkpoint_updated))
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
