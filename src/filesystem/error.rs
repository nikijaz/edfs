use crate::filesystem::storage::StorageError;
use crate::filesystem::tree::error::NodeTreeError;

#[derive(Debug)]
pub enum FileSystemError {
    NotFound,
    ReservedNode,
    StorageFull,
    NotADirectory,
    IsADirectory,
    DirectoryNotEmpty,
    AlreadyExists,
}

impl From<NodeTreeError> for FileSystemError {
    fn from(err: NodeTreeError) -> Self {
        match err {
            NodeTreeError::NotFound => FileSystemError::NotFound,
            NodeTreeError::ReservedNode => FileSystemError::ReservedNode,
        }
    }
}

impl From<StorageError> for FileSystemError {
    fn from(err: StorageError) -> Self {
        match err {
            StorageError::StorageFull => FileSystemError::StorageFull,
        }
    }
}
