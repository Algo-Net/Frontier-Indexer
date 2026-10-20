use async_trait::async_trait;
use std::sync::Arc;

use sui_indexer_alt_framework::pipeline::Processor;
use sui_indexer_alt_framework::pipeline::sequential::Handler;
use sui_indexer_alt_framework::postgres::{Connection, Db};
use sui_indexer_alt_framework::types::full_checkpoint_content::Checkpoint;

use crate::handlers::Emitter;
use crate::models::system::StoredTransactionDigest;
use crate::transports::Transport;

use crate::AppContext;

pub struct TransactionDigestHandler {
    emitter: Arc<Emitter<StoredTransactionDigest>>,
}

impl TransactionDigestHandler {
    pub fn new(
        _ctx: &AppContext,
        transports: Vec<Arc<dyn Transport<StoredTransactionDigest>>>,
    ) -> Self {
        let emitter = Emitter::new(transports);

        Self {
            emitter: Arc::new(emitter),
        }
    }
}

#[async_trait]
impl Processor for TransactionDigestHandler {
    const NAME: &'static str = "transaction_digests";
    type Value = StoredTransactionDigest;

    async fn process(&self, checkpoint: &Arc<Checkpoint>) -> anyhow::Result<Vec<Self::Value>> {
        let checkpoint_seq = checkpoint.summary.sequence_number as i64;

        let digests = checkpoint
            .transactions
            .iter()
            .map(|tx| StoredTransactionDigest {
                digest: tx.transaction.digest().to_string(),
                checkpoint: checkpoint_seq,
            })
            .collect();

        Ok(digests)
    }
}

#[async_trait]
impl Handler for TransactionDigestHandler {
    type Store = Db;
    type Batch = Vec<Self::Value>;

    fn batch(&self, batch: &mut Self::Batch, values: std::vec::IntoIter<Self::Value>) {
        batch.extend(values);
    }

    async fn commit<'a>(
        &self,
        batch: &Self::Batch,
        _conn: &mut Connection<'a>,
    ) -> anyhow::Result<usize> {
        // Transaction digests are not persisted to the database and are only emitted.
        Ok(batch.len())
    }

    async fn post_commit(&self, batch: &Self::Batch) {
        self.emitter.dispatch(Self::NAME, batch);
    }
}
