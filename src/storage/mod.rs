use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::UNIX_EPOCH,
};

use fuser::{FileAttr, INodeNo};
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::storage::{data::StorageData, tree::StorageTree};

mod data;
pub mod tree;

pub type Hash = Vec<u8>;
pub type Data = Vec<u8>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum INode {
    File {
        ino: INodeNo,
        pino: INodeNo,
        name: String,
        author: PeerId,
        mtime: u128,
        size: u64,
        hashes: Vec<Hash>,
    },
    Directory {
        ino: INodeNo,
        pino: INodeNo,
        name: String,
        author: PeerId,
        mtime: u128,
        children: HashMap<String, INodeNo>,
    },
    Tombstone {
        ino: INodeNo,
        mtime: u128,
        author: PeerId,
    },
}

impl INode {
    pub fn attr(&self) -> FileAttr {
        match self {
            INode::File {
                ino: inode, size, ..
            } => FileAttr {
                ino: inode.clone(),
                size: size.clone(),
                blocks: (size + 511) / 512,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: fuser::FileType::RegularFile,
                perm: 0o777,
                nlink: 1,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 512,
                flags: 0,
            },
            INode::Directory { ino: inode, .. } => FileAttr {
                ino: inode.clone(),
                size: 4096,
                blocks: 8,
                atime: UNIX_EPOCH,
                mtime: UNIX_EPOCH,
                ctime: UNIX_EPOCH,
                crtime: UNIX_EPOCH,
                kind: fuser::FileType::Directory,
                perm: 0o777,
                nlink: 2,
                uid: 0,
                gid: 0,
                rdev: 0,
                blksize: 4096,
                flags: 0,
            },
            _ => panic!("Invalid node type: .attr()"),
        }
    }
}

pub struct Storage {
    pub tree: Arc<RwLock<StorageTree>>,
    pub content: Arc<RwLock<StorageData>>,
}

impl Storage {
    pub fn new(max_memory: usize) -> Self {
        Self {
            tree: Arc::new(RwLock::new(StorageTree::new())),
            content: Arc::new(RwLock::new(StorageData::new(max_memory))),
        }
    }
}
