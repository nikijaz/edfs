use std::collections::HashMap;

use async_fuser::{Errno, FileHandle};
use parking_lot::RwLock;

use crate::domain::NodeMeta;

struct OpenFile {
    meta: NodeMeta,
    buffer: Option<Vec<u8>>,
    dirty: bool,
}

impl OpenFile {
    fn new(meta: NodeMeta, buffer: Option<Vec<u8>>) -> Self {
        Self {
            meta,
            buffer,
            dirty: false,
        }
    }
}

pub struct OpenFileManager {
    open_files: RwLock<HashMap<FileHandle, OpenFile>>,
}

impl OpenFileManager {
    pub fn new() -> Self {
        Self {
            open_files: RwLock::new(HashMap::new()),
        }
    }

    pub fn create_file(&self, fh: FileHandle, meta: NodeMeta) {
        self.open_files
            .write()
            .insert(fh, OpenFile::new(meta, Some(Vec::new())));
    }

    pub fn open_file(&self, fh: FileHandle, meta: NodeMeta) {
        self.open_files
            .write()
            .insert(fh, OpenFile::new(meta, None));
    }

    pub fn release_file(&self, fh: FileHandle) -> Option<Vec<u8>> {
        let session = self.open_files.write().remove(&fh)?;
        if !session.dirty {
            return None;
        }
        session.buffer
    }

    pub fn metadata(&self, fh: FileHandle) -> Option<NodeMeta> {
        self.open_files.read().get(&fh).map(|s| s.meta.clone())
    }

    pub fn read(&self, fh: FileHandle, offset: u64, length: u64) -> Option<Vec<u8>> {
        let files = self.open_files.read();
        let file = files.get(&fh)?;
        let buffer = file.buffer.as_ref()?;
        let buffer_size = buffer.len() as u64;

        if offset >= buffer_size {
            return Some(Vec::new());
        }

        let end = offset.saturating_add(length).min(buffer_size) as usize;
        Some(buffer[offset as usize..end].to_vec())
    }

    pub fn write(&self, fh: FileHandle, offset: u64, data: &[u8]) -> Result<u64, Errno> {
        let mut files = self.open_files.write();
        let file = files.get_mut(&fh).ok_or(Errno::EBADF)?;
        let buffer = file.buffer.as_mut().ok_or(Errno::EBADF)?;

        let write_start = offset as usize;
        let write_end = write_start.checked_add(data.len()).ok_or(Errno::EFBIG)?;

        if write_end > buffer.len() {
            buffer.resize(write_end, 0);
        }
        buffer[write_start..write_end].copy_from_slice(data);

        if let NodeMeta::RegularFile { size, .. } = &mut file.meta {
            *size = (*size).max(write_end as u64);
        }

        file.dirty = true;
        Ok(data.len() as u64)
    }

    pub fn needs_buffer(&self, fh: FileHandle) -> Result<bool, Errno> {
        let files = self.open_files.read();
        let file = files.get(&fh).ok_or(Errno::EBADF)?;
        Ok(file.buffer.is_none())
    }

    pub fn set_buffer(&self, fh: FileHandle, buffer: Vec<u8>) {
        if let Some(session) = self.open_files.write().get_mut(&fh) {
            session.buffer = Some(buffer);
        }
    }
}
