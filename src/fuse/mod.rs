mod attr;
mod error;
mod gateway;
mod inode;
mod mount;
mod open_file;

use std::{
    ffi::OsStr,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, SystemTime},
};

use async_fuser::{
    AccessFlags, AsyncFilesystem, BsdFileFlags, Errno, FileHandle, FileType, FopenFlags,
    Generation, INodeNo, LockOwner, OpenFlags, RenameFlags, Request, TimeOrNow, WriteFlags,
    reply_async::{
        AttrResponse, CreateResponse, DataResponse, DirectoryResponse, EntryResponse,
        GetAttrResponse, OpenResponse, ReadResponse, StatfsResponse, WriteResponse,
    },
};
use async_trait::async_trait;
use parking_lot::RwLock;

use crate::{
    config::{FUSE_BLOCK_SIZE_BYTES, FUSE_MAX_NAME_LENGTH},
    domain::{NodeId, NodeMeta},
    filesystem::FileSystem,
    port::FileSystemGateway,
};

use attr::to_attr;
use inode::INodeMap;
use open_file::OpenFileManager;

const TTL: Duration = Duration::from_secs(0);
const GENERATION: Generation = Generation(0);

pub struct FuseFileSystem<T> {
    filesystem: Arc<RwLock<FileSystem>>,
    inode_map: RwLock<INodeMap>,
    gateway: T,
    open_files: OpenFileManager,
    next_fh: AtomicU64,
}

impl<T> FuseFileSystem<T> {
    pub fn new(filesystem: Arc<RwLock<FileSystem>>, gateway: T) -> Self {
        Self {
            filesystem,
            inode_map: RwLock::new(INodeMap::new()),
            gateway,
            open_files: OpenFileManager::new(),
            next_fh: AtomicU64::new(1),
        }
    }

    fn next_fh(&self) -> FileHandle {
        FileHandle(self.next_fh.fetch_add(1, Ordering::SeqCst))
    }

    fn ino_to_node_id(&self, ino: INodeNo) -> Result<NodeId, Errno> {
        self.inode_map.read().get_node_id(ino).ok_or(Errno::ENOENT)
    }

    fn node_id_to_ino(&self, node: NodeId) -> INodeNo {
        self.inode_map.write().get_or_assign_ino(node)
    }
}

#[async_trait]
impl<N: FileSystemGateway + 'static> AsyncFilesystem for FuseFileSystem<N> {
    async fn lookup(
        &self,
        _req: &Request,
        parent_ino: INodeNo,
        name: &OsStr,
    ) -> Result<EntryResponse, Errno> {
        let name = name.to_string_lossy();
        let fs = self.filesystem.read();
        let parent_id = self.ino_to_node_id(parent_ino)?;
        let node_id = fs.child_id(parent_id, &name)?;
        let meta = fs.metadata(node_id)?;
        let node_ino = self.node_id_to_ino(node_id);
        Ok(EntryResponse::new(
            TTL,
            to_attr(&meta, node_ino),
            GENERATION,
        ))
    }

    async fn getattr(
        &self,
        _req: &Request,
        node_ino: INodeNo,
        fh: Option<FileHandle>,
    ) -> Result<GetAttrResponse, Errno> {
        if let Some(fh) = fh
            && let Some(meta) = self.open_files.metadata(fh)
        {
            return Ok(GetAttrResponse::new(TTL, to_attr(&meta, node_ino)));
        }
        let node_id = self.ino_to_node_id(node_ino)?;
        let meta = self.filesystem.read().metadata(node_id)?;
        Ok(GetAttrResponse::new(TTL, to_attr(&meta, node_ino)))
    }

    async fn setattr(
        &self,
        _req: &Request,
        node_ino: INodeNo,
        mode: Option<u32>,
        uid: Option<u32>,
        gid: Option<u32>,
        new_size: Option<u64>,
        atime: Option<TimeOrNow>,
        mtime: Option<TimeOrNow>,
        ctime: Option<SystemTime>,
        _fh: Option<FileHandle>,
        crtime: Option<SystemTime>,
        chgtime: Option<SystemTime>,
        bkuptime: Option<SystemTime>,
        flags: Option<BsdFileFlags>,
    ) -> Result<AttrResponse, Errno> {
        if mode.is_some()
            || uid.is_some()
            || gid.is_some()
            || atime.is_some()
            || mtime.is_some()
            || ctime.is_some()
            || crtime.is_some()
            || chgtime.is_some()
            || bkuptime.is_some()
            || flags.is_some()
        {
            return Err(Errno::ENOTSUP);
        }

        let node_id = self.ino_to_node_id(node_ino)?;
        if let Some(new_size) = new_size {
            self.broadcast(|fs| fs.truncate_file(node_id, new_size))
                .await?;
        }
        let meta = self.filesystem.read().metadata(node_id)?;
        Ok(AttrResponse::new(TTL, to_attr(&meta, node_ino)))
    }

    async fn mkdir(
        &self,
        _req: &Request,
        parent_ino: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
    ) -> Result<EntryResponse, Errno> {
        let name = name.to_string_lossy();
        let parent_id = self.ino_to_node_id(parent_ino)?;
        let entry = self
            .broadcast(|fs| fs.create_directory(parent_id, &name))
            .await?;
        let node_ino = self.node_id_to_ino(entry.node_id);
        Ok(EntryResponse::new(
            TTL,
            to_attr(&entry.meta, node_ino),
            GENERATION,
        ))
    }

    async fn rmdir(&self, _req: &Request, parent_ino: INodeNo, name: &OsStr) -> Result<(), Errno> {
        let parent_id = self.ino_to_node_id(parent_ino)?;
        self.broadcast(|fs| fs.delete_directory(parent_id, &name.to_string_lossy()))
            .await?;
        Ok(())
    }

    async fn unlink(&self, _req: &Request, parent_ino: INodeNo, name: &OsStr) -> Result<(), Errno> {
        let parent_id = self.ino_to_node_id(parent_ino)?;
        self.broadcast(|fs| fs.delete_file(parent_id, &name.to_string_lossy()))
            .await?;
        Ok(())
    }

    async fn rename(
        &self,
        _req: &Request,
        parent_ino: INodeNo,
        name: &OsStr,
        new_parent_ino: INodeNo,
        new_name: &OsStr,
        _flags: RenameFlags,
    ) -> Result<(), Errno> {
        let parent_id = self.ino_to_node_id(parent_ino)?;
        let new_parent_id = self.ino_to_node_id(new_parent_ino)?;
        self.broadcast(|fs| {
            fs.rename(
                parent_id,
                &name.to_string_lossy(),
                new_parent_id,
                &new_name.to_string_lossy(),
            )
        })
        .await?;
        Ok(())
    }

    async fn create(
        &self,
        _req: &Request,
        parent_ino: INodeNo,
        name: &OsStr,
        _mode: u32,
        _umask: u32,
        _flags: i32,
    ) -> Result<CreateResponse, Errno> {
        let name = name.to_string_lossy();
        let parent_id = self.ino_to_node_id(parent_ino)?;
        let entry = self
            .broadcast(|fs| fs.create_file(parent_id, &name))
            .await?;
        let node_ino = self.node_id_to_ino(entry.node_id);
        let fh = self.next_fh();
        self.open_files.create_file(fh, entry.meta.clone());
        Ok(CreateResponse::new(
            TTL,
            to_attr(&entry.meta, node_ino),
            GENERATION,
            fh,
            FopenFlags::empty(),
        ))
    }

    async fn open(
        &self,
        _req: &Request,
        node_ino: INodeNo,
        _flags: OpenFlags,
    ) -> Result<OpenResponse, Errno> {
        let node_id = self.ino_to_node_id(node_ino)?;
        let meta = self.filesystem.read().metadata(node_id)?;
        match meta {
            NodeMeta::RegularFile { .. } => {
                let fh = self.next_fh();
                self.open_files.open_file(fh, meta);
                Ok(OpenResponse::new(fh, FopenFlags::empty()))
            }
            NodeMeta::Directory { .. } => Err(Errno::EISDIR),
            NodeMeta::Symlink { .. } => Err(Errno::EINVAL),
        }
    }

    async fn opendir(
        &self,
        _req: &Request,
        ino: INodeNo,
        _flags: OpenFlags,
    ) -> Result<OpenResponse, Errno> {
        let node_id = self.ino_to_node_id(ino)?;
        match self.filesystem.read().metadata(node_id)? {
            NodeMeta::Directory { .. } => {
                Ok(OpenResponse::new(self.next_fh(), FopenFlags::empty()))
            }
            _ => Err(Errno::ENOTDIR),
        }
    }

    async fn readdir(
        &self,
        _req: &Request,
        node_ino: INodeNo,
        _fh: FileHandle,
        size: u32,
        offset: u64,
    ) -> Result<DirectoryResponse, Errno> {
        let node_id = self.ino_to_node_id(node_ino)?;
        let fs = self.filesystem.read();

        let children = fs.list_directory(node_id)?;
        let parent_id = fs.parent_id(node_id);
        let parent_ino = self.node_id_to_ino(parent_id);

        let mut all = vec![
            (node_ino, FileType::Directory, ".".to_string()),
            (parent_ino, FileType::Directory, "..".to_string()),
        ];
        for (child_id, name, meta) in children {
            let child_ino = self.node_id_to_ino(child_id);
            let kind = match &meta {
                NodeMeta::Directory { .. } => FileType::Directory,
                NodeMeta::RegularFile { .. } => FileType::RegularFile,
                NodeMeta::Symlink { .. } => FileType::Symlink,
            };
            all.push((child_ino, kind, name));
        }

        let mut reply = DirectoryResponse::new(size as usize);
        for (i, (child_ino, kind, name)) in all.into_iter().enumerate().skip(offset as usize) {
            if reply.add(child_ino, (i + 1) as u64, kind, name) {
                break;
            }
        }
        Ok(reply)
    }

    async fn read(
        &self,
        _req: &Request,
        node_ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        size: u32,
        _flags: OpenFlags,
        _lock: Option<LockOwner>,
    ) -> Result<ReadResponse, Errno> {
        if let Some(data) = self.open_files.read(fh, offset, size as u64) {
            return Ok(ReadResponse::new(data));
        }

        let node_id = self.ino_to_node_id(node_ino)?;
        if !self
            .fetch_missing_chunks(node_id, offset, size as u64)
            .await?
        {
            return Err(Errno::EIO);
        }
        let data = self
            .filesystem
            .read()
            .read_file(node_id, offset, size as u64)?;
        Ok(ReadResponse::new(data))
    }

    async fn write(
        &self,
        _req: &Request,
        node_ino: INodeNo,
        fh: FileHandle,
        offset: u64,
        data: &[u8],
        _write_flags: WriteFlags,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
    ) -> Result<WriteResponse, Errno> {
        if data.is_empty() {
            return Ok(WriteResponse::new(0));
        }

        if self.open_files.needs_buffer(fh)? {
            let node = self.ino_to_node_id(node_ino)?;
            if !self.fetch_missing_chunks(node, 0, u64::MAX).await? {
                return Err(Errno::EIO);
            }
            let buffer = self.filesystem.read().read_file_all(node)?;
            self.open_files.set_buffer(fh, buffer);
        }

        let written = self.open_files.write(fh, offset, data)?;
        Ok(WriteResponse::new(written as u32))
    }

    async fn release(
        &self,
        _req: &Request,
        node_ino: INodeNo,
        fh: FileHandle,
        _flags: OpenFlags,
        _lock_owner: Option<LockOwner>,
        _flush: bool,
    ) -> Result<(), Errno> {
        let Some(buffer) = self.open_files.release_file(fh) else {
            return Ok(());
        };
        let node = self.ino_to_node_id(node_ino)?;
        if self.filesystem.read().metadata(node).is_err() {
            return Ok(());
        }
        self.broadcast(|fs| fs.write_file(node, &buffer)).await?;
        Ok(())
    }

    async fn readlink(&self, _req: &Request, ino: INodeNo) -> Result<DataResponse, Errno> {
        let node_id = self.ino_to_node_id(ino)?;
        let meta = self.filesystem.read().metadata(node_id)?;
        match meta {
            NodeMeta::Symlink { target, .. } => Ok(DataResponse::new(target.into_bytes())),
            _ => Err(Errno::EINVAL),
        }
    }

    async fn symlink(
        &self,
        _req: &Request,
        parent: INodeNo,
        link_name: &OsStr,
        target: &Path,
    ) -> Result<EntryResponse, Errno> {
        let name = link_name.to_string_lossy();
        let target = target.to_string_lossy().to_string();
        let parent_id = self.ino_to_node_id(parent)?;
        let entry = self
            .broadcast(|fs| fs.create_symlink(parent_id, &name, target))
            .await?;
        let node_ino = self.node_id_to_ino(entry.node_id);
        Ok(EntryResponse::new(
            TTL,
            to_attr(&entry.meta, node_ino),
            GENERATION,
        ))
    }

    async fn statfs(&self, _req: &Request, _ino: INodeNo) -> Result<StatfsResponse, Errno> {
        let filesystem = self.filesystem.read();
        let block_size = u64::from(FUSE_BLOCK_SIZE_BYTES);
        let total = (filesystem.capacity_bytes() as u64).div_ceil(block_size);
        let free = filesystem.free_bytes() as u64 / block_size;
        Ok(StatfsResponse::new(
            total,
            free,
            free,
            u64::MAX,
            u64::MAX,
            FUSE_BLOCK_SIZE_BYTES,
            FUSE_MAX_NAME_LENGTH,
            FUSE_BLOCK_SIZE_BYTES,
        ))
    }

    async fn releasedir(
        &self,
        _req: &Request,
        _ino: INodeNo,
        _fh: FileHandle,
        _flags: OpenFlags,
    ) -> Result<(), Errno> {
        Ok(())
    }

    async fn access(&self, _req: &Request, _ino: INodeNo, _mask: AccessFlags) -> Result<(), Errno> {
        Ok(())
    }
}
