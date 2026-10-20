use async_trait::async_trait;
use std::sync::Arc;

use diesel_async::RunQueryDsl;

use sui_types::event::Event;

use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::handlers::EventMeta;
use crate::models::world::StoredItemDestroyed;
use crate::transports::Transport;

use crate::AppContext;

pub struct ItemDestroyedHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<StoredItemDestroyed>>,
}

impl ItemDestroyedHandler {
    pub fn new(ctx: &AppContext, transports: Vec<Arc<dyn Transport<StoredItemDestroyed>>>) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_item_destroyed(&self, event: &Event) -> bool {
        let module_name = "inventory";
        let event_name = "ItemDestroyedEvent";
        self.ctx.is_world_event(event, module_name, event_name)
    }
}

#[async_trait]
impl Processor for ItemDestroyedHandler {
    const NAME: &'static str = "item_destroyed";
    type Value = StoredItemDestroyed;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let mut results = vec![];

        for tx in &checkpoint.transactions {
            if !self.ctx.is_indexed_tx(tx, &checkpoint.object_set) {
                continue;
            }

            let Some(events) = &tx.events else { continue };

            let base_meta = EventMeta::from_checkpoint_tx(checkpoint, tx);

            for (index, ev) in events.data.iter().enumerate() {
                if self.is_item_destroyed(ev) {
                    let meta = base_meta.with_index(index);
                    let event = StoredItemDestroyed::from_event(ev, &meta)?;
                    results.push(event);
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for ItemDestroyedHandler {
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
        use crate::schema::events_item_destroyed::dsl::*;

        diesel::insert_into(events_item_destroyed)
            .values(batch)
            .on_conflict((event_id, occurred_at))
            .do_nothing()
            .execute(conn)
            .await?;

        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
