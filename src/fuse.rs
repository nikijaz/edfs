use fuser::{Errno, FileHandle, Filesystem};
use tokio::sync::{mpsc, oneshot};

use crate::{
    CHUNK_SIZE_BYTES,
    protocol::{Meta, SwarmCommand},
    storage::{Storage, meta_attr, tree::ROOT_ID},
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

macro_rules! wtree {
    ($self:expr) => {
        $self.storage.tree.write().unwrap()
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
    pub meta: Meta,
    pub dirty: bool,
}

const DURATION: std::time::Duration = std::time::Duration::from_secs(1);
const GENERATION: fuser::Generation = fuser::Generation(1);

pub struct Fuse {
    storage: std::sync::Arc<Storage>,
    swarm: mpsc::Sender<SwarmCommand>,
    sessions: std::sync::RwLock<std::collections::HashMap<FileHandle, FileSession>>,
    last_fh: std::sync::atomic::AtomicU64,
}

impl Fuse {
    pub fn new(storage: std::sync::Arc<Storage>, swarm: mpsc::Sender<SwarmCommand>) -> Self {
        Self {
            storage,
            swarm,
            sessions: std::sync::RwLock::new(std::collections::HashMap::new()),
            last_fh: std::sync::atomic::AtomicU64::new(1),
        }
    }
}

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
        let node = tree
            .get_child(&pino, &name)
            .and_then(|ino| tree.get(&ino).map(|m| (ino, m)));

        match node {
            Some((ino, meta)) => reply.entry(&DURATION, &meta_attr(&meta, ino), GENERATION),
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
        if let Some(fh) = fh
            && let Some(session) = rsess!(self).get(&fh)
        {
            reply.attr(&DURATION, &meta_attr(&session.meta, ino));
            return;
        }

        let tree = rtree!(self);
        let meta = tree.get(&ino);

        match meta {
            Some(meta) => reply.attr(&DURATION, &meta_attr(&meta, ino)),
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
            let tree = rtree!(self);
            let path = tree.get_path(ino);

            if path != "/" {
                let needed_chunks = size.div_ceil(CHUNK_SIZE_BYTES);
                let mut hashes = {
                    match tree.get(&ino) {
                        Some(Meta::File { hashes, .. }) => hashes.clone(),
                        _ => {
                            reply.error(Errno::ENOENT);
                            return;
                        }
                    }
                };
                hashes.truncate(needed_chunks as usize);

                let (_, ops) = wtree!(self).set_file(&path, size, hashes);
                if !ops.is_empty() {
                    let _ = self
                        .swarm
                        .blocking_send(SwarmCommand::BroadcastOperation { operation: ops });
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
            if pino != ROOT_ID && tree.get(&pino).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if pino == ROOT_ID {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(pino), name)
            }
        };

        let (ino, ops) = wtree!(self).mkdir(&path);
        if !ops.is_empty() {
            let _ = self
                .swarm
                .blocking_send(SwarmCommand::BroadcastOperation { operation: ops });
        }

        {
            if let Some(meta) = rtree!(self).get(&ino) {
                reply.entry(&DURATION, &meta_attr(&meta, ino), GENERATION);
            } else {
                reply.error(Errno::ENOENT);
            }
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
        let meta = tree.get(&ino);

        match meta {
            Some(Meta::Directory { .. }) => {}
            _ => {
                reply.error(Errno::ENOENT);
                return;
            }
        };

        let parent = tree.get_parent(&ino).unwrap_or(ino);
        let children = tree.get_children(&ino);

        let mut entries = vec![
            (ino, fuser::FileType::Directory, ".".to_string()),
            (parent, fuser::FileType::Directory, "..".to_string()),
        ];

        for (name, cino) in children {
            let kind = match tree.get(&cino) {
                Some(Meta::Directory { .. }) => fuser::FileType::Directory,
                Some(Meta::File { .. }) => fuser::FileType::RegularFile,
                _ => continue,
            };
            entries.push((cino, kind, name));
        }

        for (i, (child_ino, kind, name)) in entries.into_iter().enumerate().skip(offset as usize) {
            if reply.add(child_ino, (i + 1) as u64, kind, name) {
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
            if parent != ROOT_ID && tree.get(&parent).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if parent == ROOT_ID {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(parent), name)
            }
        };

        let ops = wtree!(self).delete(&path);
        if !ops.is_empty() {
            let _ = self
                .swarm
                .blocking_send(SwarmCommand::BroadcastOperation { operation: ops });
        }
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
            if parent != ROOT_ID && tree.get(&parent).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if parent == ROOT_ID {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(parent), name)
            }
        };

        let (ino, ops) = wtree!(self).set_file(&path, 0, Vec::new());
        if !ops.is_empty() {
            let _ = self
                .swarm
                .blocking_send(SwarmCommand::BroadcastOperation { operation: ops });
        }

        {
            let tree = rtree!(self);
            if let Some(meta) = tree.get(&ino) {
                let fh = self
                    .last_fh
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                wsess!(self).insert(
                    FileHandle(fh),
                    FileSession {
                        meta: meta.clone(),
                        dirty: false,
                    },
                );
                reply.created(
                    &DURATION,
                    &meta_attr(&meta, ino),
                    GENERATION,
                    FileHandle(fh),
                    fuser::FopenFlags::empty(),
                );
            } else {
                reply.error(Errno::ENOENT);
            }
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
        if let Some(meta) = tree.get(&ino) {
            match meta {
                Meta::File { .. } => {
                    let fh = self
                        .last_fh
                        .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    wsess!(self).insert(
                        fuser::FileHandle(fh),
                        FileSession {
                            meta: meta.clone(),
                            dirty: false,
                        },
                    );
                    reply.opened(fuser::FileHandle(fh), fuser::FopenFlags::empty());
                }
                Meta::Directory { .. } => reply.error(Errno::EISDIR),
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
                if let Meta::File { size, hashes, .. } = &session.meta {
                    (*size, hashes.clone())
                } else {
                    (0, Vec::new())
                }
            } else {
                let tree = rtree!(self);
                if let Some(Meta::File { size, hashes, .. }) = tree.get(&ino) {
                    (size, hashes.clone())
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
        _ino: fuser::INodeNo,
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
                if let Meta::File {
                    size: s, hashes: h, ..
                } = &session.meta
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

        let needed_chunks = new_size.div_ceil(chunk_size);
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
            if let Some(session) = sessions.get_mut(&fh)
                && let Meta::File {
                    size: ref mut s,
                    hashes: ref mut h,
                    ..
                } = session.meta
            {
                *s = new_size;
                *h = hashes;
                session.dirty = true;
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
            if let Some(session) = sessions.get(&fh)
                && session.dirty
                && let Meta::File {
                    size: s, hashes: h, ..
                } = &session.meta
            {
                size = *s;
                hashes = h.clone();
                dirty = true;
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

            let (_, ops) = wtree!(self).set_file(&path, size, hashes);
            if !ops.is_empty() {
                let _ = self
                    .swarm
                    .blocking_send(SwarmCommand::BroadcastOperation { operation: ops });
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
        _ino: fuser::INodeNo,
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
            if parent != ROOT_ID && tree.get(&parent).is_none() {
                reply.error(Errno::ENOENT);
                return;
            }
            if parent == ROOT_ID {
                format!("/{}", name)
            } else {
                format!("{}/{}", tree.get_path(parent), name)
            }
        };

        let ops = wtree!(self).delete(&path);
        if !ops.is_empty() {
            let _ = self
                .swarm
                .blocking_send(SwarmCommand::BroadcastOperation { operation: ops });
        }

        reply.ok();
    }
}
