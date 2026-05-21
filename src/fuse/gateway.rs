use async_fuser::Errno;

use crate::{
    domain::NodeId,
    filesystem::{FileSystem, TreeMutation, error::FileSystemError},
    port::FileSystemGateway,
};

use super::FuseFileSystem;

impl<N: FileSystemGateway> FuseFileSystem<N> {
    pub async fn broadcast<T>(
        &self,
        mutation: impl FnOnce(&mut FileSystem) -> Result<TreeMutation<T>, FileSystemError>,
    ) -> Result<T, Errno> {
        let changes = mutation(&mut self.filesystem.write())?;
        Ok(changes.broadcast(&self.gateway).await)
    }

    pub async fn fetch_missing_chunks(
        &self,
        node_id: NodeId,
        offset: u64,
        length: u64,
    ) -> Result<bool, Errno> {
        let missing_hashes = self
            .filesystem
            .read()
            .missing_chunk_hashes_for(node_id, offset, length)?;

        let fetched =
            futures::future::join_all(missing_hashes.iter().map(|hash| self.gateway.fetch(hash)))
                .await;

        let mut all_fetched = true;
        for (hash, data) in missing_hashes.into_iter().zip(fetched) {
            match data {
                Some(data) => self.filesystem.write().cache_chunk(hash, data),
                None => all_fetched = false,
            }
        }

        Ok(all_fetched)
    }
}
