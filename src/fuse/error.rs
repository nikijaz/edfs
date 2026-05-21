use async_fuser::Errno;

use crate::filesystem::error::FileSystemError;

impl From<FileSystemError> for Errno {
    fn from(err: FileSystemError) -> Self {
        match err {
            FileSystemError::NotFound => Errno::ENOENT,
            FileSystemError::NotADirectory => Errno::ENOTDIR,
            FileSystemError::IsADirectory => Errno::EISDIR,
            FileSystemError::DirectoryNotEmpty => Errno::ENOTEMPTY,
            FileSystemError::AlreadyExists => Errno::EEXIST,
            FileSystemError::StorageFull => Errno::ENOSPC,
            FileSystemError::ReservedNode => Errno::EPERM,
        }
    }
}
