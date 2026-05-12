use std::{
    sync::{Arc, RwLock},
    time::UNIX_EPOCH,
};

use fuser::{FileAttr, INodeNo};
use libp2p::PeerId;

use crate::protocol::Meta;
use crate::storage::{data::StorageData, tree::StorageTree};

mod data;
pub mod tree;

pub type Hash = Vec<u8>;
pub type Data = Vec<u8>;

pub fn meta_attr(meta: &Meta, ino: INodeNo) -> FileAttr {
    match meta {
        Meta::File { size, .. } => FileAttr {
            ino,
            size: *size,
            blocks: size.div_ceil(512),
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
        Meta::Directory { .. } => FileAttr {
            ino,
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
    }
}

pub struct Storage {
    pub tree: Arc<RwLock<StorageTree>>,
    pub content: Arc<RwLock<StorageData>>,
}

impl Storage {
    pub fn new(max_memory: usize, peer_id: PeerId) -> Self {
        Self {
            tree: Arc::new(RwLock::new(StorageTree::new(peer_id))),
            content: Arc::new(RwLock::new(StorageData::new(max_memory))),
        }
    }
}
