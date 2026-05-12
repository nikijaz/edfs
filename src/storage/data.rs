use std::collections::HashMap;

use crate::storage::{Data, Hash};

pub struct StorageData {
    data: HashMap<Hash, Data>,
    used_memory: usize,
    max_memory: usize,
}

impl StorageData {
    pub fn new(max_memory: usize) -> Self {
        Self {
            data: HashMap::new(),
            used_memory: 0,
            max_memory,
        }
    }

    pub fn get(&self, hash: &Hash) -> Option<&Data> {
        self.data.get(hash)
    }

    pub fn insert(&mut self, hash: Hash, data: Data) -> bool {
        if self.used_memory + data.len() > self.max_memory {
            return false;
        }
        if !self.data.contains_key(&hash) {
            self.used_memory += data.len();
            self.data.insert(hash, data);
        }
        true
    }
}
