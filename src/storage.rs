use std::collections::HashMap;

use fuser::INodeNo;

pub type Hash = Vec<u8>;
pub type Data = Vec<u8>;

enum NodeKind {
    File { size: u64, hashes: Vec<Hash> },
    Directory { children: Vec<INodeNo> },
}

struct Node {
    pub inode: INodeNo,
    pub parent: INodeNo,
    pub name: String,
    pub kind: NodeKind,
}

pub struct Storage {
    data: HashMap<Hash, Data>,
    nodes: HashMap<INodeNo, Node>,

    last_inode: INodeNo,

    used_memory: usize,
    max_memory: usize,
}

impl Storage {
    pub fn new(max_memory: usize) -> Self {
        let mut storage = Self {
            data: HashMap::new(),
            nodes: HashMap::new(),
            last_inode: INodeNo::ROOT,
            used_memory: 0,
            max_memory: max_memory,
        };

        storage.nodes.insert(
            INodeNo::ROOT,
            Node {
                inode: INodeNo::ROOT,
                parent: INodeNo::ROOT,
                name: "".to_string(),
                kind: NodeKind::Directory {
                    children: Vec::new(),
                },
            },
        );

        storage
    }

    pub fn push_chunk(&mut self, hash: Hash, data: Data) {
        self.data.insert(hash, data);
    }

    pub fn pull_chunk(&self, hash: Hash) -> Option<&Data> {
        self.data.get(&hash)
    }
}

pub trait SwarmStorage {
    fn update_file(&mut self, path: String, size: u64, hashes: Vec<Hash>) -> ();
    fn create_directory(&mut self, path: String) -> ();
    fn delete(&mut self, path: String) -> ();
}

pub trait FuseStorage {}
