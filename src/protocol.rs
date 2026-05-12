use crdt_tree::OpMove;
use fuser::INodeNo;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

use crate::storage::{Data, Hash, tree::StorageTree};

#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    CapacityFull,
    ChunkNotFound,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    PushChunk { hash: Hash, data: Data },
    PullChunk { hash: Hash },
    PullTree,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ack,
    Data(Vec<u8>),
    TreeState(StorageTree),
    Error(Error),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum Meta {
    File {
        name: String,
        size: u64,
        hashes: Vec<Hash>,
    },
    Directory {
        name: String,
    },
}

impl Meta {
    pub fn name(&self) -> &str {
        match self {
            Meta::File { name, .. } => name,
            Meta::Directory { name } => name,
        }
    }
}

pub type NodeOpMove = OpMove<INodeNo, Meta, PeerId>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gossip {
    pub operation: Vec<NodeOpMove>,
}

pub enum SwarmCommand {
    BroadcastOperation {
        operation: Vec<NodeOpMove>,
    },
    FetchChunk {
        hash: Hash,
        reply: oneshot::Sender<Option<Data>>,
    },
}
