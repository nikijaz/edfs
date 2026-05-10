use std::time::Duration;

use fuser::{Errno, Filesystem, Generation};

use crate::{
    CHUNK_SIZE_BYTES,
    storage::{INode, INodeAttr, Storage},
};

struct Fuse {
    storage: Storage,
}

impl Filesystem for Fuse {
    fn lookup(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let tree = self.storage.tree.read().unwrap();
        let node = tree
            .get_child(parent, &name.to_string_lossy())
            .and_then(|inode| tree.get(inode));

        match node {
            Some(node) => reply.entry(&Duration::from_secs(1), &node.attr(), Generation(0)),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: Option<fuser::FileHandle>,
        reply: fuser::ReplyAttr,
    ) {
        let tree = self.storage.tree.read().unwrap();
        let node = tree.get(ino);

        match node {
            Some(node) => reply.attr(&Duration::from_secs(1), &node.attr()),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn setattr(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _mode: Option<u32>,
        _uid: Option<u32>,
        _gid: Option<u32>,
        size: Option<u64>,
        _atime: Option<fuser::TimeOrNow>,
        _mtime: Option<fuser::TimeOrNow>,
        _ctime: Option<std::time::SystemTime>,
        fh: Option<fuser::FileHandle>,
        _crtime: Option<std::time::SystemTime>,
        _chgtime: Option<std::time::SystemTime>,
        _bkuptime: Option<std::time::SystemTime>,
        _flags: Option<fuser::BsdFileFlags>,
        reply: fuser::ReplyAttr,
    ) {
        if let Some(size) = size {
            if let Some(_) = fh {
                // TODO: FH
            } else {
                let mut tree = self.storage.tree.write().unwrap();

                if let Some(INode::File { hashes, .. }) = tree.get(ino) {
                    let mut hashes = hashes.clone();
                    let needed_chunks = (size + CHUNK_SIZE_BYTES - 1) / CHUNK_SIZE_BYTES;
                    hashes.truncate(needed_chunks as usize);
                    tree.update(ino, size, hashes);

                    // TODO: Gossip
                }
            }
        }

        self.getattr(_req, ino, fh, reply);
    }

    fn mkdir(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        {
            let mut tree = self.storage.tree.write().unwrap();
            tree.add(parent, name.to_string_lossy().to_string(), true);
        }

        // TODO: Optimise
        self.lookup(_req, parent, name, reply);
    }

    fn readdir(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        fh: fuser::FileHandle,
        offset: u64,
        reply: fuser::ReplyDirectory,
    ) {
        let tree = self.storage.tree.read().unwrap();
        if let Some(children) = tree.get_children(ino) {}
    }

    fn rmdir(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
    }

    fn create(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
        reply: fuser::ReplyCreate,
    ) {
    }

    fn open(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _flags: fuser::OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
    }

    fn read(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        fh: fuser::FileHandle,
        offset: u64,
        size: u32,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyData,
    ) {
    }

    fn write(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        fh: fuser::FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: fuser::WriteFlags,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        reply: fuser::ReplyWrite,
    ) {
    }

    fn flush(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        fh: fuser::FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
    }

    fn release(
        &self,
        _req: &fuser::Request,
        _ino: fuser::INodeNo,
        fh: fuser::FileHandle,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
    }

    fn unlink(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
    }
}
