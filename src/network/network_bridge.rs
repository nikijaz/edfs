use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::{
    domain::ChunkHash,
    filesystem::tree::NodeOperation,
    network::event::SwarmCommand,
    port::{ChunkProvider, OperationBroadcaster},
};

const BATCH_IDLE: Duration = Duration::from_secs(1);
const MAX_BATCH_OPERATIONS: usize = 64;

#[derive(Clone)]
pub struct NetworkBridge {
    tx: mpsc::Sender<SwarmCommand>,
    operations: mpsc::Sender<Vec<NodeOperation>>,
}

impl NetworkBridge {
    pub fn new(tx: mpsc::Sender<SwarmCommand>) -> Self {
        let (operations, mut rx) = mpsc::channel::<Vec<NodeOperation>>(256);
        let tx_clone = tx.clone();

        tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut batch = first;
                let deadline = tokio::time::sleep(BATCH_IDLE);
                tokio::pin!(deadline);

                while batch.len() < MAX_BATCH_OPERATIONS {
                    tokio::select! {
                        Some(operations) = rx.recv() => batch.extend(operations),
                        _ = &mut deadline => break,
                    }
                }

                while !batch.is_empty() {
                    let count = batch.len().min(MAX_BATCH_OPERATIONS);
                    let remaining = batch.split_off(count);
                    let operations = std::mem::replace(&mut batch, remaining);
                    if tx_clone
                        .send(SwarmCommand::BroadcastOperations { operations })
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });

        Self { tx, operations }
    }
}

impl OperationBroadcaster for NetworkBridge {
    async fn broadcast(&self, operations: Vec<NodeOperation>) {
        if !operations.is_empty() {
            self.operations.send(operations).await.ok();
        }
    }
}

impl ChunkProvider for NetworkBridge {
    async fn fetch(&self, hash: &ChunkHash) -> Option<Vec<u8>> {
        let (reply, rx) = oneshot::channel();
        if self
            .tx
            .send(SwarmCommand::FetchChunk {
                hash: hash.clone(),
                reply: Some(reply),
            })
            .await
            .is_err()
        {
            return None;
        }
        rx.await.ok().flatten()
    }
}
