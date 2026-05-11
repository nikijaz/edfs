#![allow(unused)]

use std::{
    collections::HashMap,
    sync::{Arc, RwLock, atomic::AtomicU64},
    time::Duration,
};

use fuser::{Errno, FileHandle, Filesystem, FopenFlags, Generation, INodeNo};
use tokio::sync::{mpsc, oneshot};

use crate::{
    CHUNK_SIZE_BYTES,
    protocol::{FsEvent, SwarmCommand},
    storage::{INode, Storage},
};

macro_rules! rsess {
    ($self:expr) => {
        $self.sessions.read().unwrap()
    };
}

macro_rules! wsess {
    ($self:expr) => {
        $self.sessions.write().unwrap()
    };
}

macro_rules! rtree {
    ($self:expr) => {
        $self.storage.tree.read().unwrap()
    };
}

macro_rules! rcont {
    ($self:expr) => {
        $self.storage.content.read().unwrap()
    };
}

macro_rules! wcont {
    ($self:expr) => {
        $self.storage.content.write().unwrap()
    };
}

struct FileSession {
    pub node: INode,
    pub dirty: bool,
}

pub(crate) struct Fuse {
    storage: Arc<Storage>,
    swarm: mpsc::Sender<SwarmCommand>,
    sessions: RwLock<HashMap<FileHandle, FileSession>>,
    last_fh: AtomicU64,
}

impl Fuse {
    pub fn new(storage: Arc<Storage>, swarm: mpsc::Sender<SwarmCommand>) -> Self {
        Self {
            storage,
            swarm,
            sessions: RwLock::new(HashMap::new()),
            last_fh: AtomicU64::new(1),
        }
    }
}

const DURATION: Duration = Duration::from_secs(1);
const GENERATION: Generation = Generation(0);
const FOPEN_FLAGS: FopenFlags = FopenFlags::empty();

impl Filesystem for Fuse {
    fn lookup(
        &self,
        _req: &fuser::Request,
        pino: fuser::INodeNo,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEntry,
    ) {
        let name = name.to_string_lossy().to_string();

        let tree = rtree!(self);
        let node = tree.get_child(&pino, &name).and_then(|ino| tree.get(&ino));

        match node {
            Some(node) => reply.entry(&DURATION, &node.attr(), GENERATION),
            None => reply.error(Errno::ENOENT),
        }
    }

    fn getattr(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        fh: Option<fuser::FileHandle>,
        reply: fuser::ReplyAttr,
    ) {
        if let Some(fh) = fh {
            if let Some(session) = rsess!(self).get(&fh) {
                reply.attr(&DURATION, &session.node.attr());
                return;
            }
        }

        let tree = rtree!(self);
        let node = tree.get(&ino);

        match node {
            Some(node) => reply.attr(&DURATION, &node.attr()),
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
            let needed_chunks = (size + CHUNK_SIZE_BYTES - 1) / CHUNK_SIZE_BYTES;

            if let Some(fh) = fh {
                if let Some(session) = wsess!(self).get_mut(&fh) {
                    if let INode::File {
                        size: size_old,
                        hashes,
                        ..
                    } = &mut session.node
                    {
                        *size_old = size;
                        hashes.truncate(needed_chunks as usize);
                        session.dirty = true;
                    }
                }
            } else {
                let (path, mut hashes) = {
                    let tree = rtree!(self);
                    let hashes = match tree.get(&ino) {
                        Some(INode::File { hashes, .. }) => hashes.clone(),
                        _ => {
                            reply.error(Errno::ENOENT);
                            return;
                        }
                    };
                    (tree.get_path(ino), hashes)
                };
                hashes.truncate(needed_chunks as usize);

                let event = FsEvent::SetFile { path, size, hashes };
                let (tx, rx) = oneshot::channel();
                if self
                    .swarm
                    .blocking_send(SwarmCommand::ApplyFsEvent { event, reply: tx })
                    .is_ok()
                {
                    let _ = rx.blocking_recv();
                }
            }
        }

        self.getattr(_req, ino, fh, reply);
    }

    fn mkdir(
        &self,
        _req: &fuser::Request,
        pino: fuser::INodeNo,
        name: &std::ffi::OsStr,
        _mode: u32,
        _umask: u32,
        reply: fuser::ReplyEntry,
    ) {
        let name = name.to_string_lossy().to_string();

        let path = {
            let tree = rtree!(self);
            if pino != INodeNo::ROOT && tree.get(&pino).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if pino == INodeNo::ROOT {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(pino), name)
            }
        };

        let event = FsEvent::CreateDirectory { path };
        let (tx, rx) = oneshot::channel();
        if self
            .swarm
            .blocking_send(SwarmCommand::ApplyFsEvent { event, reply: tx })
            .is_err()
        {
            reply.error(Errno::EIO);
            return;
        }

        if let Ok(Some(ino)) = rx.blocking_recv() {
            if let Some(node) = rtree!(self).get(&ino) {
                reply.entry(&DURATION, &node.attr(), GENERATION);
            } else {
                reply.error(Errno::ENOENT);
            }
        } else {
            reply.error(Errno::EIO);
        }
    }

    fn readdir(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _fh: fuser::FileHandle,
        offset: u64,
        mut reply: fuser::ReplyDirectory,
    ) {
        let tree = rtree!(self);
        let node = tree.get(&ino);

        let (parent, children) = match node {
            Some(INode::Directory {
                pino: parent,
                children,
                ..
            }) => (*parent, children.clone()),
            _ => {
                reply.error(Errno::ENOENT);
                return;
            }
        };

        let mut entries = vec![
            (ino, fuser::FileType::Directory, ".".to_string()),
            (parent, fuser::FileType::Directory, "..".to_string()),
        ];

        for (name, cino) in children {
            let kind = match tree.get(&cino) {
                Some(INode::Directory { .. }) => fuser::FileType::Directory,
                Some(INode::File { .. }) => fuser::FileType::RegularFile,
                _ => continue,
            };
            entries.push((cino, kind, name));
        }

        for (i, (child_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(child_ino, (i as u64) + 1, kind, name) {
                break;
            }
        }
        reply.ok();
    }

    fn rmdir(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let name = name.to_string_lossy().to_string();

        let path = {
            let tree = rtree!(self);
            if parent != INodeNo::ROOT && tree.get(&parent).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if parent == INodeNo::ROOT {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(parent), name)
            }
        };

        let event = FsEvent::Delete { path };
        let (tx, rx) = oneshot::channel();
        if self
            .swarm
            .blocking_send(SwarmCommand::ApplyFsEvent { event, reply: tx })
            .is_err()
        {
            reply.error(Errno::EIO);
            return;
        }
        let _ = rx.blocking_recv();
        reply.ok();
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
        let name = name.to_string_lossy().to_string();

        let path = {
            let tree = rtree!(self);
            if parent != INodeNo::ROOT && tree.get(&parent).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if parent == INodeNo::ROOT {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(parent), name)
            }
        };

        let event = FsEvent::SetFile {
            path,
            size: 0,
            hashes: Vec::new(),
        };
        let (tx, rx) = oneshot::channel();
        if self
            .swarm
            .blocking_send(SwarmCommand::ApplyFsEvent { event, reply: tx })
            .is_err()
        {
            reply.error(Errno::EIO);
            return;
        }

        if let Ok(Some(ino)) = rx.blocking_recv() {
            let tree = rtree!(self);
            if let Some(node) = tree.get(&ino) {
                let fh = self
                    .last_fh
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                wsess!(self).insert(
                    FileHandle(fh),
                    FileSession {
                        node: node.clone(),
                        dirty: false,
                    },
                );
                reply.created(
                    &DURATION,
                    &node.attr(),
                    GENERATION,
                    FileHandle(fh),
                    FOPEN_FLAGS,
                );
            } else {
                reply.error(Errno::ENOENT);
            }
        } else {
            reply.error(Errno::EIO);
        }
    }

    fn open(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        _flags: fuser::OpenFlags,
        reply: fuser::ReplyOpen,
    ) {
        let tree = rtree!(self);
        if let Some(node) = tree.get(&ino) {
            match node {
                INode::File { .. } => {
                    let fh = self
                        .last_fh
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    wsess!(self).insert(
                        fuser::FileHandle(fh),
                        FileSession {
                            node: node.clone(),
                            dirty: false,
                        },
                    );
                    reply.opened(fuser::FileHandle(fh), FOPEN_FLAGS);
                }
                INode::Directory { .. } => reply.error(Errno::EISDIR),
                _ => reply.error(Errno::ENOENT),
            }
        } else {
            reply.error(Errno::ENOENT);
        }
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
        let (file_size, hashes) = {
            let sessions = rsess!(self);
            if let Some(session) = sessions.get(&fh) {
                if let INode::File { size, hashes, .. } = &session.node {
                    (*size, hashes.clone())
                } else {
                    (0, Vec::new())
                }
            } else {
                let tree = rtree!(self);
                if let Some(INode::File { size, hashes, .. }) = tree.get(&ino) {
                    (*size, hashes.clone())
                } else {
                    (0, Vec::new())
                }
            }
        };

        if offset >= file_size {
            reply.data(&[]);
            return;
        }

        let read_size = std::cmp::min(size as u64, file_size - offset);
        if read_size == 0 {
            reply.data(&[]);
            return;
        }

        let chunk_size = CHUNK_SIZE_BYTES;
        let start_index = (offset / chunk_size) as usize;
        let end_index = ((offset + read_size - 1) / chunk_size) as usize;

        let mut fetch_tasks = Vec::new();

        for i in start_index..=end_index {
            if i >= hashes.len() {
                fetch_tasks.push(None);
                continue;
            }

            let hash = hashes[i].clone();
            let chunk_data = rcont!(self).get(&hash).cloned();

            if let Some(data) = chunk_data {
                let (tx, rx) = oneshot::channel();
                let _ = tx.send(Some(data));
                fetch_tasks.push(Some((hash, rx)));
            } else {
                let (tx, rx) = oneshot::channel();
                if self
                    .swarm
                    .blocking_send(SwarmCommand::FetchChunk {
                        hash: hash.clone(),
                        reply: tx,
                    })
                    .is_ok()
                {
                    fetch_tasks.push(Some((hash, rx)));
                } else {
                    fetch_tasks.push(None);
                }
            }
        }

        let mut output_buffer =
            Vec::with_capacity(((end_index - start_index + 1) as u64 * chunk_size) as usize);

        for task in fetch_tasks {
            match task {
                Some((hash, rx)) => match rx.blocking_recv() {
                    Ok(Some(data)) => {
                        wcont!(self).insert(hash.clone(), data.clone());
                        output_buffer.extend(data);
                    }
                    _ => {
                        output_buffer.extend(vec![0; chunk_size as usize]);
                    }
                },
                None => {
                    output_buffer.extend(vec![0; chunk_size as usize]);
                }
            }
        }

        let slice_start = (offset % chunk_size) as usize;
        let slice_end = slice_start + read_size as usize;
        reply.data(&output_buffer[slice_start..slice_end]);
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
        let chunk_size = CHUNK_SIZE_BYTES;

        if data.is_empty() {
            reply.written(0);
            return;
        }

        let mut new_size = 0;
        let mut hashes = Vec::new();

        {
            let sessions = rsess!(self);
            if let Some(session) = sessions.get(&fh) {
                if let INode::File {
                    size: s, hashes: h, ..
                } = &session.node
                {
                    new_size = *s;
                    hashes = h.clone();
                }
            } else {
                reply.error(Errno::EBADF);
                return;
            }
        }

        if offset + data.len() as u64 > new_size {
            new_size = offset + data.len() as u64;
        }

        let needed_chunks = (new_size + chunk_size - 1) / chunk_size;
        hashes.resize(needed_chunks as usize, vec![0; 32]);

        let start_chunk = (offset / chunk_size) as usize;
        let end_chunk = ((offset + data.len() as u64 - 1) / chunk_size) as usize;

        let mut data_offset = 0;

        for i in start_chunk..=end_chunk {
            let chunk_offset = (i as u64) * chunk_size;
            let mut chunk_data = if i < hashes.len() && !hashes[i].iter().all(|&b| b == 0) {
                rcont!(self)
                    .get(&hashes[i])
                    .cloned()
                    .unwrap_or_else(|| vec![0; chunk_size as usize])
            } else {
                vec![0; chunk_size as usize]
            };

            let write_start = if offset > chunk_offset {
                (offset - chunk_offset) as usize
            } else {
                0
            };
            let write_end =
                std::cmp::min(chunk_size as usize, write_start + data.len() - data_offset);

            let write_len = write_end - write_start;
            chunk_data[write_start..write_end]
                .copy_from_slice(&data[data_offset..data_offset + write_len]);
            data_offset += write_len;

            chunk_data.truncate(chunk_size as usize);

            use sha2::Digest;
            let new_hash = sha2::Sha256::digest(&chunk_data).to_vec();
            wcont!(self).insert(new_hash.clone(), chunk_data);
            if i < hashes.len() {
                hashes[i] = new_hash;
            } else {
                hashes.push(new_hash);
            }
        }

        {
            let mut sessions = wsess!(self);
            if let Some(session) = sessions.get_mut(&fh) {
                if let INode::File {
                    size: s, hashes: h, ..
                } = &mut session.node
                {
                    *s = new_size;
                    *h = hashes;
                    session.dirty = true;
                }
            }
        }

        reply.written(data.len() as u32);
    }

    fn flush(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        fh: fuser::FileHandle,
        _lock_owner: fuser::LockOwner,
        reply: fuser::ReplyEmpty,
    ) {
        let mut dirty = false;
        let mut size = 0;
        let mut hashes = Vec::new();

        {
            let sessions = rsess!(self);
            if let Some(session) = sessions.get(&fh) {
                if session.dirty {
                    if let INode::File {
                        size: s, hashes: h, ..
                    } = &session.node
                    {
                        size = *s;
                        hashes = h.clone();
                        dirty = true;
                    }
                }
            }
        }

        if dirty {
            let path = {
                let tree = rtree!(self);
                if tree.get(&ino).is_none() {
                    if let Some(s) = wsess!(self).get_mut(&fh) {
                        s.dirty = false;
                    }
                    reply.ok();
                    return;
                }
                tree.get_path(ino)
            };

            let event = FsEvent::SetFile { path, size, hashes };
            let (tx, rx) = oneshot::channel();
            if self
                .swarm
                .blocking_send(SwarmCommand::ApplyFsEvent { event, reply: tx })
                .is_ok()
            {
                let _ = rx.blocking_recv();
            }

            if let Some(s) = wsess!(self).get_mut(&fh) {
                s.dirty = false;
            }
        }
        reply.ok();
    }

    fn release(
        &self,
        _req: &fuser::Request,
        ino: fuser::INodeNo,
        fh: fuser::FileHandle,
        _flags: fuser::OpenFlags,
        _lock_owner: Option<fuser::LockOwner>,
        _flush: bool,
        reply: fuser::ReplyEmpty,
    ) {
        wsess!(self).remove(&fh);
        reply.ok();
    }

    fn unlink(
        &self,
        _req: &fuser::Request,
        parent: fuser::INodeNo,
        name: &std::ffi::OsStr,
        reply: fuser::ReplyEmpty,
    ) {
        let name = name.to_string_lossy().to_string();

        let path = {
            let tree = rtree!(self);
            if parent != INodeNo::ROOT && tree.get(&parent).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if parent == INodeNo::ROOT {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(parent), name)
            }
        };

        let event = FsEvent::Delete { path };
        let (tx, rx) = oneshot::channel();
        if self
            .swarm
            .blocking_send(SwarmCommand::ApplyFsEvent { event, reply: tx })
            .is_err()
        {
            reply.error(Errno::EIO);
            return;
        }

        let _ = rx.blocking_recv();
        reply.ok();
    }
}
