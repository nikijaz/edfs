use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub enum Error {
    CapacityFull,
    ChunkNotFound,
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Request {
    PushChunk { hash: Vec<u8>, data: Vec<u8> },
    PullChunk { hash: Vec<u8> },
}

#[derive(Debug, Serialize, Deserialize)]
pub enum Response {
    Ack,
    Data(Vec<u8>),
    Error(Error),
}
