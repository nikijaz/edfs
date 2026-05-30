mod chunk;
pub mod error;
mod mutation;
pub mod storage;
pub mod tree;

pub use mutation::TreeMutation;

use std::collections::HashSet;
use std::collections::VecDeque;

use uuid::Uuid;

use crate::{
    domain::{Chunk, ChunkHash, ChunkRef, NodeId, NodeMeta},
    filesystem::{
        error::FileSystemError,
        storage::ChunkStorage,
        tree::{NodeOperation, NodeOperationId, NodeTree},
    },
};

pub struct FileSystemEntry {
    pub node_id: NodeId,
    pub meta: NodeMeta,
}

pub struct FileSystem {
    tree: NodeTree,
    storage: ChunkStorage,
}

impl FileSystem {
    pub fn new(tree: NodeTree, storage: ChunkStorage) -> Self {
        Self { tree, storage }
    }

    pub fn capacity_bytes(&self) -> usize {
        self.storage.capacity_bytes()
    }

    pub fn free_bytes(&self) -> usize {
        self.storage.free_bytes()
    }

    pub fn chunk(&self, hash: &ChunkHash) -> Option<Vec<u8>> {
        self.storage.get(hash)
    }

    pub fn metadata(&self, node_id: NodeId) -> Result<NodeMeta, FileSystemError> {
        if node_id == NodeId::ROOT {
            return Ok(NodeMeta::Directory {
                name: String::new(),
            });
        }
        if self.tree.is_trashed(node_id) {
            return Err(FileSystemError::NotFound);
        }
        self.tree
            .metadata(node_id)
            .cloned()
            .ok_or(FileSystemError::NotFound)
    }

    pub fn parent_id(&self, node_id: NodeId) -> NodeId {
        self.tree.parent_id(node_id).unwrap_or(NodeId::ROOT)
    }

    pub fn child_id(&self, parent_id: NodeId, name: &str) -> Result<NodeId, FileSystemError> {
        self.tree
            .children(parent_id)?
            .into_iter()
            .find(|&child_id| {
                self.tree
                    .metadata(child_id)
                    .is_some_and(|metadata| metadata.name() == name)
            })
            .ok_or(FileSystemError::NotFound)
    }

    pub fn create_directory(
        &mut self,
        parent_id: NodeId,
        name: &str,
    ) -> Result<TreeMutation<FileSystemEntry>, FileSystemError> {
        self.create_node(
            parent_id,
            name,
            NodeMeta::Directory {
                name: name.to_string(),
            },
        )
    }

    pub fn list_directory(
        &self,
        node_id: NodeId,
    ) -> Result<Vec<(NodeId, String, NodeMeta)>, FileSystemError> {
        match self.metadata(node_id)? {
            NodeMeta::Directory { .. } => {}
            _ => return Err(FileSystemError::NotADirectory),
        }
        let mut children = Vec::new();
        for child_id in self.tree.children(node_id)? {
            if let Ok(meta) = self.metadata(child_id) {
                children.push((child_id, meta.name().to_string(), meta));
            }
        }
        Ok(children)
    }

    pub fn delete_directory(
        &mut self,
        parent_id: NodeId,
        name: &str,
    ) -> Result<TreeMutation<()>, FileSystemError> {
        let node_id = self.child_id(parent_id, name)?;
        match self.metadata(node_id)? {
            NodeMeta::Directory { .. } => {}
            _ => return Err(FileSystemError::NotADirectory),
        }
        if !self.tree.children(node_id)?.is_empty() {
            return Err(FileSystemError::DirectoryNotEmpty);
        }
        Ok(TreeMutation::new((), vec![self.tree.delete_node(node_id)?]))
    }

    pub fn create_file(
        &mut self,
        parent_id: NodeId,
        name: &str,
    ) -> Result<TreeMutation<FileSystemEntry>, FileSystemError> {
        self.create_node(
            parent_id,
            name,
            NodeMeta::RegularFile {
                name: name.to_string(),
                size: 0,
                chunk_refs: Vec::new(),
            },
        )
    }

    pub fn delete_file(
        &mut self,
        parent_id: NodeId,
        name: &str,
    ) -> Result<TreeMutation<()>, FileSystemError> {
        let node_id = self.child_id(parent_id, name)?;
        match self.metadata(node_id)? {
            NodeMeta::Directory { .. } => Err(FileSystemError::IsADirectory),
            _ => Ok(TreeMutation::new((), vec![self.tree.delete_node(node_id)?])),
        }
    }

    pub fn write_file(
        &mut self,
        node_id: NodeId,
        data: &[u8],
    ) -> Result<TreeMutation<()>, FileSystemError> {
        let NodeMeta::RegularFile { name, .. } = self.metadata(node_id)? else {
            return Err(FileSystemError::IsADirectory);
        };
        let chunks = chunk::split(data);
        let chunk_refs: Vec<ChunkRef> = chunks.iter().map(Chunk::reference).collect();
        self.storage.try_pin(chunks)?;

        let new_meta = NodeMeta::RegularFile {
            name,
            size: data.len() as u64,
            chunk_refs,
        };
        let parent_id = self
            .tree
            .parent_id(node_id)
            .ok_or(FileSystemError::NotFound)?;
        let op = self.tree.move_node(node_id, parent_id, new_meta)?;
        Ok(TreeMutation::new((), vec![op]))
    }

    pub fn truncate_file(
        &mut self,
        node_id: NodeId,
        size: u64,
    ) -> Result<TreeMutation<()>, FileSystemError> {
        let NodeMeta::RegularFile {
            name, chunk_refs, ..
        } = self.metadata(node_id)?
        else {
            return Err(FileSystemError::IsADirectory);
        };
        let new_meta = NodeMeta::RegularFile {
            name,
            size,
            chunk_refs: chunk::truncate(&chunk_refs, size),
        };
        let parent_id = self
            .tree
            .parent_id(node_id)
            .ok_or(FileSystemError::NotFound)?;
        let op = self.tree.move_node(node_id, parent_id, new_meta)?;
        Ok(TreeMutation::new((), vec![op]))
    }

    pub fn read_file(
        &self,
        node_id: NodeId,
        offset: u64,
        length: u64,
    ) -> Result<Vec<u8>, FileSystemError> {
        let NodeMeta::RegularFile {
            size, chunk_refs, ..
        } = self.metadata(node_id)?
        else {
            return Err(FileSystemError::IsADirectory);
        };

        let read_end = offset.saturating_add(length).min(size);
        if offset >= read_end {
            return Ok(Vec::new());
        }

        let mut output = Vec::with_capacity((read_end - offset) as usize);
        let mut chunk_start = 0;

        for chunk_ref in &chunk_refs {
            let chunk_end = chunk_start + chunk_ref.size;

            if chunk_end <= offset {
                chunk_start = chunk_end;
                continue;
            }
            if chunk_start >= read_end {
                break;
            }

            let data = self
                .storage
                .get(&chunk_ref.hash)
                .unwrap_or_else(|| vec![0; chunk_ref.size as usize]);

            let start_in_chunk = offset.saturating_sub(chunk_start) as usize;
            let end_in_chunk = (read_end - chunk_start).min(chunk_ref.size) as usize;
            let available_end = end_in_chunk.min(data.len());

            if start_in_chunk < available_end {
                output.extend_from_slice(&data[start_in_chunk..available_end]);
            }

            chunk_start = chunk_end;
        }

        Ok(output)
    }

    pub fn read_file_all(&self, node_id: NodeId) -> Result<Vec<u8>, FileSystemError> {
        let NodeMeta::RegularFile { size, .. } = self.metadata(node_id)? else {
            return Err(FileSystemError::IsADirectory);
        };

        self.read_file(node_id, 0, size)
    }

    pub fn create_symlink(
        &mut self,
        parent_id: NodeId,
        name: &str,
        target: String,
    ) -> Result<TreeMutation<FileSystemEntry>, FileSystemError> {
        self.create_node(
            parent_id,
            name,
            NodeMeta::Symlink {
                name: name.to_string(),
                target,
            },
        )
    }

    pub fn rename(
        &mut self,
        parent_id: NodeId,
        name: &str,
        new_parent_id: NodeId,
        new_name: &str,
    ) -> Result<TreeMutation<()>, FileSystemError> {
        let node_id = self.child_id(parent_id, name)?;

        if let Ok(existing) = self.child_id(new_parent_id, new_name)
            && existing != node_id
        {
            return Err(FileSystemError::AlreadyExists);
        }
        if new_parent_id == parent_id && new_name == name {
            return Ok(TreeMutation::new((), Vec::new()));
        }

        let new_meta = match self.metadata(node_id)?.clone() {
            NodeMeta::Directory { .. } => NodeMeta::Directory {
                name: new_name.to_string(),
            },
            NodeMeta::RegularFile {
                size, chunk_refs, ..
            } => NodeMeta::RegularFile {
                name: new_name.to_string(),
                size,
                chunk_refs,
            },
            NodeMeta::Symlink { target, .. } => NodeMeta::Symlink {
                name: new_name.to_string(),
                target,
            },
        };
        let operation = self.tree.move_node(node_id, new_parent_id, new_meta)?;
        Ok(TreeMutation::new((), vec![operation]))
    }

    pub fn pin_chunk(&mut self, data: Vec<u8>) -> Result<(), FileSystemError> {
        self.storage.try_pin(std::iter::once(Chunk::new(data)))?;
        Ok(())
    }

    pub fn cache_chunk(&mut self, data: Vec<u8>) {
        self.storage.cache(Chunk::new(data));
    }

    pub fn is_pinned(&self, hash: &ChunkHash) -> bool {
        self.storage.is_pinned(hash)
    }

    pub fn is_cached(&self, hash: &ChunkHash) -> bool {
        self.storage.is_cached(hash)
    }

    pub fn pinned_hashes(&self) -> Vec<ChunkHash> {
        self.storage.pinned_hashes()
    }

    pub fn promote_to_pinned(&mut self, hash: &ChunkHash) -> bool {
        self.storage.promote_to_pinned(hash)
    }

    pub fn evict_chunk(&mut self, hash: &ChunkHash) -> bool {
        self.storage.evict(hash)
    }

    pub fn reachable_chunk_hashes(&self) -> HashSet<ChunkHash> {
        let mut reachable = HashSet::new();
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        visited.insert(NodeId::ROOT);
        queue.push_back(NodeId::ROOT);

        while let Some(node_id) = queue.pop_front() {
            let children = match self.tree.children(node_id) {
                Ok(children) => children,
                Err(_) => continue,
            };
            for child in children {
                if child == NodeId::TRASH || !visited.insert(child) {
                    continue;
                }
                match self.tree.metadata(child) {
                    Some(NodeMeta::RegularFile { chunk_refs, .. }) => {
                        reachable.extend(chunk_refs.iter().map(|chunk_ref| chunk_ref.hash.clone()));
                    }
                    Some(NodeMeta::Directory { .. }) => queue.push_back(child),
                    _ => {}
                }
            }
        }
        reachable
    }

    pub fn missing_chunk_hashes_for(
        &self,
        node_id: NodeId,
        offset: u64,
        length: u64,
    ) -> Result<Vec<ChunkHash>, FileSystemError> {
        let NodeMeta::RegularFile {
            size, chunk_refs, ..
        } = self.metadata(node_id)?
        else {
            return Err(FileSystemError::IsADirectory);
        };

        let read_end = offset.saturating_add(length).min(size);
        let mut missing = Vec::new();
        let mut chunk_start = 0;

        for chunk_ref in &chunk_refs {
            let chunk_end = chunk_start + chunk_ref.size;

            if chunk_start < read_end
                && chunk_end > offset
                && !self.storage.contains(&chunk_ref.hash)
            {
                missing.push(chunk_ref.hash.clone());
            }

            chunk_start = chunk_end;
            if chunk_start >= read_end {
                break;
            }
        }

        Ok(missing)
    }

    pub fn apply_operations(&mut self, opeartions: Vec<NodeOperation>) {
        self.tree.apply_operations(opeartions);
    }

    pub fn operation_ids(&self) -> Vec<NodeOperationId> {
        self.tree.operation_ids()
    }

    pub fn operations_missing_from(&self, ids: &HashSet<NodeOperationId>) -> Vec<NodeOperation> {
        self.tree.operations_missing_from(ids)
    }

    fn create_node(
        &mut self,
        parent: NodeId,
        name: &str,
        meta: NodeMeta,
    ) -> Result<TreeMutation<FileSystemEntry>, FileSystemError> {
        if self.child_id(parent, name).is_ok() {
            return Err(FileSystemError::AlreadyExists);
        }
        let node_id = NodeId::from_uuid(Uuid::new_v5(&parent.as_uuid(), name.as_bytes()));
        let operation = self.tree.create_node(parent, node_id, meta.clone());
        Ok(TreeMutation::new(
            FileSystemEntry { node_id, meta },
            vec![operation],
        ))
    }
}
