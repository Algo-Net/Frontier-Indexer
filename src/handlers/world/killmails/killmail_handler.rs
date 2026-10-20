use async_trait::async_trait;
use std::sync::Arc;

use diesel_async::RunQueryDsl;

use sui_types::effects::{IDOperation, TransactionEffectsAPI};
use sui_types::object::Object;
use sui_types::storage::ObjectKey;

use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::models::world::StoredKillmail;
use crate::transports::Transport;

use crate::AppContext;

pub struct KillmailHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<StoredKillmail>>,
}

impl KillmailHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<StoredKillmail>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_killmail(&self, obj: &Object) -> bool {
        let module_name = "killmail";
        let struct_name = "Killmail";
        self.ctx.is_world_object(obj, module_name, struct_name)
    }
}

#[async_trait]
impl Processor for KillmailHandler {
    const NAME: &'static str = "killmails";
    type Value = StoredKillmail;

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

                        if self.is_killmail(obj) {
                            let killmail = StoredKillmail::from_object(obj)?;
                            results.push(killmail);
                        }
                    }
                    IDOperation::None => {} // Killmails are immutable, no need to handle updates.
                    IDOperation::Deleted => {} // Killmails are kept even if (somehow) deleted.
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for KillmailHandler {
    type Store = Db;
    type Batch = Vec<Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        batch.extend(values);
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        use crate::schema::killmails::dsl::*;

        diesel::insert_into(killmails)
            .values(batch)
            .on_conflict((id, occurred_at))
            .do_nothing()
            .execute(conn)
            .await?;

        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
