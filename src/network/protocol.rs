use serde::{Deserialize, Serialize};

use crate::{
    domain::ChunkHash,
    filesystem::tree::{NodeOperation, NodeOperationId},
};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ProtocolError {
    ChunkNotFound,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Request {
    PullChunk { hash: ChunkHash },
    CheckChunks { hashes: Vec<ChunkHash> },
    PullMissingOperations { known_ids: Vec<NodeOperationId> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Response {
    ChunkData(Vec<u8>),
    HeldChunks { hashes: Vec<ChunkHash> },
    MissingOperations { operations: Vec<NodeOperation> },
    Error(ProtocolError),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gossip {
    pub operations: Vec<NodeOperation>,
}
