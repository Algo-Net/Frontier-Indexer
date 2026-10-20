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
    sender: Option<mpsc::Sender<(&'static str, Vec<I>)>>,
}

impl<I: Serialize + Clone + Send + Sync + 'static> Emitter<I> {
    pub fn new(transports: Vec<Arc<dyn Transport<I>>>) -> Self {
        if transports.is_empty() {
            return Self { sender: None };
        }

        let (sender, mut receiver) =
            mpsc::channel::<(&'static str, Vec<I>)>(EMITTER_QUEUE_CAPACITY);

        tokio::spawn(async move {
            while let Some((pipeline, items)) = receiver.recv().await {
                for item in &items {
                    Self::dispatch_entry(&transports, pipeline, item).await;
                }
            }
        });

        Self {
            sender: Some(sender),
        }
    }

    pub fn dispatch<B>(&self, pipeline: &'static str, batch: &B)
    where
        B: Dispatchable<I>,
    {
        let Some(tx) = &self.sender else {
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
        let futs: Vec<_> = transports
            .iter()
            .map(|transport| {
                tokio::time::timeout(
                    tokio::time::Duration::from_secs(5),
                    transport.send(pipeline, item),
                )
            })
            .collect();

        let results = join_all(futs).await;

        for result in results {
            if let Err(err) = result {
                tracing::warn!(pipeline, "Transport send failed: {:?}", err);
            }
        }
    }
}
