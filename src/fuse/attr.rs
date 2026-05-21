use std::time::UNIX_EPOCH;

use async_fuser::{FileAttr, FileType, INodeNo};

use crate::{config::FUSE_BLOCK_SIZE_BYTES, domain::NodeMeta};

pub fn to_attr(meta: &NodeMeta, ino: INodeNo) -> FileAttr {
    let (kind, size) = match meta {
        NodeMeta::Directory { .. } => (FileType::Directory, u64::from(FUSE_BLOCK_SIZE_BYTES)),
        NodeMeta::RegularFile { size, .. } => (FileType::RegularFile, *size),
        NodeMeta::Symlink { target, .. } => (FileType::Symlink, target.len() as u64),
    };
    FileAttr {
        ino,
        size,
        blocks: size.div_ceil(u64::from(FUSE_BLOCK_SIZE_BYTES)),
        atime: UNIX_EPOCH,
        mtime: UNIX_EPOCH,
        ctime: UNIX_EPOCH,
        crtime: UNIX_EPOCH,
        kind,
        perm: 0o777,
        nlink: if kind == FileType::Directory { 2 } else { 1 },
        uid: 0,
        gid: 0,
        rdev: 0,
        blksize: FUSE_BLOCK_SIZE_BYTES,
        flags: 0,
    }
}
