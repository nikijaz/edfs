use std::future::Future;

use crate::{domain::ChunkHash, filesystem::tree::NodeOperation};

pub trait ChunkProvider {
    fn fetch(&self, hash: &ChunkHash) -> impl Future<Output = Option<Vec<u8>>> + Send;
}

pub trait OperationBroadcaster {
    fn broadcast(&self, operations: Vec<NodeOperation>) -> impl Future<Output = ()> + Send;
}

pub trait FileSystemGateway: ChunkProvider + OperationBroadcaster + Send + Sync {}

impl<T: ChunkProvider + OperationBroadcaster + Send + Sync> FileSystemGateway for T {}
