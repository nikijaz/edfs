use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::UNIX_EPOCH,
};

use fuser::{FileAttr, INodeNo};

use crate::storage::{data::StorageData, tree::StorageTree};

mod data;
mod tree;

pub type FileName = String;
pub type Hash = Vec<u8>;
pub type Data = Vec<u8>;

pub enum INode {
    File {
        inode: INodeNo,
        parent: INodeNo,
        name: FileName,
        size: u64,
        hashes: Vec<Hash>,
    },
    Directory {
        inode: INodeNo,
        parent: INodeNo,
        name: FileName,
        children: HashMap<FileName, INodeNo>,
    },
}

pub trait INodeAttr {
    fn attr(&self) -> FileAttr;
}

impl INodeAttr for INode {
    fn attr(&self) -> FileAttr {
        match self {
            INode::File { inode, size, .. } => FileAttr {
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
            INode::Directory { inode, .. } => FileAttr {
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
