use fuser::INodeNo;
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

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ack,
    Data(Vec<u8>),
    TreeState(StorageTree),
    Error(Error),
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum FsEvent {
    CreateDirectory {
        path: String,
    },
    SetFile {
        path: String,
        size: u64,
        hashes: Vec<Hash>,
    },
    Delete {
        path: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Gossip {
    pub mtime: u128,
    pub event: FsEvent,
}

pub enum SwarmCommand {
    ApplyFsEvent {
        event: FsEvent,
        reply: oneshot::Sender<Option<INodeNo>>,
    },
    FetchChunk {
        hash: Hash,
        reply: oneshot::Sender<Option<Data>>,
    },
}
