use std::collections::{HashMap, HashSet, VecDeque};

use parking_lot::Mutex;

use crate::{
    config::FASTCDC_MAX_CHUNK_SIZE,
    domain::{Chunk, ChunkHash},
};

const MAX_CACHED_BYTES: usize = FASTCDC_MAX_CHUNK_SIZE * 16;

#[derive(Debug)]
pub enum StorageError {
    StorageFull,
}

struct ChunkEntry {
    data: Vec<u8>,
    pinned: bool,
}

pub struct ChunkStorage {
    chunks: HashMap<ChunkHash, ChunkEntry>,
    cache_lru: Mutex<VecDeque<ChunkHash>>,
    pinned_bytes: usize,
    cached_bytes: usize,
    max_pinned_bytes: usize,
    max_cached_bytes: usize,
}

impl ChunkStorage {
    pub fn new(max_pinned_bytes: usize) -> Self {
        Self {
            chunks: HashMap::new(),
            cache_lru: Mutex::new(VecDeque::new()),
            pinned_bytes: 0,
            cached_bytes: 0,
            max_pinned_bytes,
            max_cached_bytes: MAX_CACHED_BYTES,
        }
    }

    pub fn capacity_bytes(&self) -> usize {
        self.max_pinned_bytes
    }

    pub fn free_bytes(&self) -> usize {
        self.max_pinned_bytes - self.pinned_bytes
    }

    pub fn try_pin(&mut self, chunks: impl IntoIterator<Item = Chunk>) -> Result<(), StorageError> {
        let mut required_bytes = 0;
        let mut seen_hashes = HashSet::new();
        let mut new_chunks = Vec::new();
        let mut cached_hashes = Vec::new();

        for chunk in chunks {
            let hash = chunk.hash().clone();
            if !seen_hashes.insert(hash.clone()) {
                continue;
            }

            match self.chunks.get(&hash) {
                Some(entry) if !entry.pinned => {
                    required_bytes += entry.data.len();
                    cached_hashes.push(hash);
                }
                Some(_) => {}
                None => {
                    required_bytes += chunk.data().len();
                    new_chunks.push(chunk);
                }
            }
        }

        if self.pinned_bytes + required_bytes > self.max_pinned_bytes {
            return Err(StorageError::StorageFull);
        }

        for chunk in new_chunks {
            let (hash, data) = chunk.into_parts();
            self.pinned_bytes += data.len();
            self.chunks.insert(hash, ChunkEntry { data, pinned: true });
        }
        for hash in cached_hashes {
            let entry = self.chunks.get_mut(&hash).unwrap();
            entry.pinned = true;
            self.pinned_bytes += entry.data.len();
            self.cached_bytes -= entry.data.len();
            self.cache_lru.lock().retain(|cached| cached != &hash);
        }

        Ok(())
    }

    pub fn cache(&mut self, hash: ChunkHash, data: Vec<u8>) {
        let chunk_size = data.len();

        if self.chunks.contains_key(&hash) || chunk_size > self.max_cached_bytes {
            return;
        }

        let mut lru = self.cache_lru.lock();
        while self.cached_bytes + chunk_size > self.max_cached_bytes {
            let Some(hash) = lru.pop_front() else {
                return;
            };
            let Some(entry) = self.chunks.remove(&hash) else {
                continue;
            };
            self.cached_bytes -= entry.data.len();
        }
        lru.push_back(hash.clone());

        self.chunks.insert(
            hash,
            ChunkEntry {
                data,
                pinned: false,
            },
        );
        self.cached_bytes += chunk_size;
    }

    pub fn get(&self, hash: &ChunkHash) -> Option<Vec<u8>> {
        let entry = self.chunks.get(hash)?;
        let pinned = entry.pinned;
        let data = entry.data.clone();
        if !pinned {
            let mut lru = self.cache_lru.lock();
            lru.retain(|h| h != hash);
            lru.push_back(hash.clone());
        }
        Some(data)
    }

    pub fn contains(&self, hash: &ChunkHash) -> bool {
        self.chunks.contains_key(hash)
    }
}
