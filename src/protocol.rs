use serde::{Deserialize, Serialize};

use crate::storage::{Data, Hash};

#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    CapacityFull,
    ChunkNotFound,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    PushChunk { hash: Hash, data: Data },
    PullChunk { hash: Hash },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ack,
    Data(Vec<u8>),
    Error(Error),
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Gossip {
    UpdateFile {
        path: String,
        size: u64,
        hashes: Vec<Hash>,
    },
    CreateDirectory {
        path: String,
    },
    Delete {
        path: String,
    },
}
