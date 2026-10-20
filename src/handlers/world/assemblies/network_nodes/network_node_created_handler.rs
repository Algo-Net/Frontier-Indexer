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
use crate::models::world::StoredNetworkNodeCreated;
use crate::transports::Transport;

use crate::AppContext;

pub struct NetworkNodeCreatedHandler {
    ctx: AppContext,
    emitter: Arc<Emitter<StoredNetworkNodeCreated>>,
}

impl NetworkNodeCreatedHandler {
    pub fn new(
        ctx: &AppContext,
        transports: Vec<Arc<dyn Transport<StoredNetworkNodeCreated>>>,
    ) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            ctx: ctx.clone(),
            emitter: Arc::new(emitter),
        }
    }

    fn is_network_node_created(&self, event: &Event) -> bool {
        let module_name = "network_node";
        let event_name = "NetworkNodeCreatedEvent";
        self.ctx.is_world_event(event, module_name, event_name)
    }
}

#[async_trait]
impl Processor for NetworkNodeCreatedHandler {
    const NAME: &'static str = "network_node_created";
    type Value = StoredNetworkNodeCreated;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let mut results = vec![];

        for tx in &checkpoint.transactions {
            if !self.ctx.is_indexed_tx(tx, &checkpoint.object_set) {
                continue;
            }

            let Some(events) = &tx.events else { continue };

            let base_meta = EventMeta::from_checkpoint_tx(checkpoint, tx);

            for (index, ev) in events.data.iter().enumerate() {
                if self.is_network_node_created(ev) {
                    let meta = base_meta.with_index(index);
                    let event = StoredNetworkNodeCreated::from_event(ev, &meta)?;
                    results.push(event);
                }
            }
        }

        Ok(results)
    }
}

#[async_trait]
impl Handler for NetworkNodeCreatedHandler {
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
        use crate::schema::events_network_node_created::dsl::*;

        diesel::insert_into(events_network_node_created)
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
