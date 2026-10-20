use futures::future::join_all;
use serde::Serialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

use crate::transports::Transport;

const EMITTER_QUEUE_CAPACITY: usize = 1024;

pub trait Dispatchable<I> {
    fn dispatch_iter(&self) -> Box<dyn Iterator<Item = &I> + Send + '_>;
    fn is_empty(&self) -> bool;
}

impl<I: Send + Sync> Dispatchable<I> for Vec<I> {
    fn dispatch_iter(&self) -> Box<dyn Iterator<Item = &I> + Send + '_> {
        Box::new(self.iter())
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

impl<I: Send + Sync> Dispatchable<I> for HashMap<String, I> {
    fn dispatch_iter(&self) -> Box<dyn Iterator<Item = &I> + Send + '_> {
        Box::new(self.values())
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }
}

pub struct Emitter<I: Serialize + Send + Sync + 'static> {
    tx: Option<mpsc::Sender<(&'static str, Vec<I>)>>,
}

impl<I: Serialize + Clone + Send + Sync + 'static> Emitter<I> {
    pub fn new(transports: Vec<Arc<dyn Transport<I>>>) -> Self {
        if transports.is_empty() {
            return Self { tx: None };
        }

        let (tx, mut rx) = mpsc::channel::<(&'static str, Vec<I>)>(EMITTER_QUEUE_CAPACITY);

        tokio::spawn(async move {
            while let Some((pipeline, items)) = rx.recv().await {
                for item in &items {
                    Self::dispatch_entry(&transports, pipeline, item).await;
                }
            }
        });

        Self { tx: Some(tx) }
    }

    pub fn dispatch<B>(&self, pipeline: &'static str, batch: &B)
    where
        B: Dispatchable<I>,
    {
        let Some(tx) = &self.tx else {
            return;
        };

        if batch.is_empty() {
            return;
        }

        let items: Vec<I> = batch.dispatch_iter().cloned().collect();

        if let Err(err) = tx.try_send((pipeline, items)) {
            tracing::warn!(
                pipeline,
                "Emitter queue full or closed, dropping batch: {}",
                err
            );
        }
    }

    async fn dispatch_entry(
        transports: &[Arc<dyn Transport<I>>],
        pipeline: &'static str,
        item: &I,
    ) {
        let futs: Vec<_> = transports.iter().map(|t| t.send(pipeline, item)).collect();

        let results = join_all(futs).await;

        for result in results {
            if let Err(err) = result {
                tracing::warn!(pipeline, "Transport send failed: {:?}", err);
            }
        }
    }
}
