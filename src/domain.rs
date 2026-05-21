use serde::{Deserialize, Serialize};
use sha2::Digest;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct NodeId(Uuid);

impl NodeId {
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }

    pub const fn as_uuid(self) -> Uuid {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ChunkHash([u8; 32]);

impl ChunkHash {
    pub const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }
}

pub struct Chunk {
    hash: ChunkHash,
    data: Vec<u8>,
}

impl Chunk {
    pub fn new(data: Vec<u8>) -> Self {
        let hash = ChunkHash::from_bytes(sha2::Sha256::digest(&data).into());
        Self { hash, data }
    }

    pub fn hash(&self) -> &ChunkHash {
        &self.hash
    }

    pub fn data(&self) -> &[u8] {
        &self.data
    }

    pub fn into_parts(self) -> (ChunkHash, Vec<u8>) {
        (self.hash, self.data)
    }

    pub fn reference(&self) -> ChunkRef {
        ChunkRef {
            hash: self.hash.clone(),
            size: self.data.len() as u64,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkRef {
    pub hash: ChunkHash,
    pub size: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum NodeMeta {
    Directory {
        name: String,
    },
    RegularFile {
        name: String,
        size: u64,
        chunk_refs: Vec<ChunkRef>,
    },
    Symlink {
        name: String,
        target: String,
    },
}

impl NodeMeta {
    pub fn name(&self) -> &str {
        match self {
            NodeMeta::Directory { name }
            | NodeMeta::RegularFile { name, .. }
            | NodeMeta::Symlink { name, .. } => name,
        }
    }
}
