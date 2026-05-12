use std::collections::HashMap;

use crdt_tree::TreeReplica;
use fuser::INodeNo;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::protocol::{Meta, NodeOpMove};
use crate::storage::Hash;

pub const ROOT_ID: INodeNo = fuser::INodeNo(1);
pub const TRASH_ID: INodeNo = fuser::INodeNo(2);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTree {
    pub replica: TreeReplica<INodeNo, Meta, PeerId>,
}

impl StorageTree {
    pub fn new(peer_id: PeerId) -> Self {
        let replica = TreeReplica::new(peer_id);
        Self { replica }
    }

    pub fn mkdir(&mut self, path: &str) -> (INodeNo, Vec<NodeOpMove>) {
        let mut ops = Vec::new();
        let ino = self.ensure_path(path, true, &mut ops);
        (ino, ops)
    }

    pub fn set_file(
        &mut self,
        path: &str,
        size: u64,
        hashes: Vec<Hash>,
    ) -> (INodeNo, Vec<NodeOpMove>) {
        let mut ops = Vec::new();
        let ino = self.ensure_path(path, false, &mut ops);

        if let Some(node) = self.replica.tree().find(&ino) {
            let meta = node.metadata().clone();
            if let Meta::File {
                size: ref m_size,
                hashes: ref m_hashes,
                ..
            } = meta
                && (*m_size != size || *m_hashes != hashes)
            {
                let new_meta = Meta::File {
                    name: meta.name().to_string(),
                    size,
                    hashes: hashes.clone(),
                };
                let pino = *node.parent_id();
                let op = self.replica.opmove(pino, new_meta, ino);
                self.replica.apply_op(op.clone());
                ops.push(op);
            }
        }

        (ino, ops)
    }

    pub fn delete(&mut self, path: &str) -> Vec<NodeOpMove> {
        let mut ops = Vec::new();
        if let Some(ino) = self.find_by_path(path)
            && ino != ROOT_ID
        {
            let meta = self.replica.tree().find(&ino).unwrap().metadata().clone();
            let op = self.replica.opmove(TRASH_ID, meta, ino);
            self.replica.apply_op(op.clone());
            ops.push(op);
        }
        ops
    }

    pub fn get(&self, ino: &INodeNo) -> Option<Meta> {
        if *ino == ROOT_ID {
            return Some(Meta::Directory {
                name: "".to_string(),
            });
        }

        let node = self.replica.tree().find(ino)?;
        let meta = node.metadata();
        let pino = *node.parent_id();

        if pino == TRASH_ID {
            return None;
        }

        Some(meta.clone())
    }

    pub fn get_parent(&self, ino: &INodeNo) -> Option<INodeNo> {
        if *ino == ROOT_ID {
            return Some(ROOT_ID);
        }
        let node = self.replica.tree().find(ino)?;
        let pino = *node.parent_id();
        if pino == TRASH_ID {
            return None;
        }
        Some(pino)
    }

    pub fn get_children(&self, pino: &INodeNo) -> HashMap<String, INodeNo> {
        self.replica
            .tree()
            .children(pino)
            .into_iter()
            .filter_map(|c| {
                let node = self.replica.tree().find(&c)?;
                if *node.parent_id() == TRASH_ID {
                    return None;
                }
                Some((node.metadata().name().to_string(), c))
            })
            .collect()
    }

    pub fn get_child(&self, pino: &INodeNo, name: &str) -> Option<INodeNo> {
        self.replica.tree().children(pino).into_iter().find(|c| {
            let node = self.replica.tree().find(c);
            match node {
                Some(n) => n.metadata().name() == name && *n.parent_id() != TRASH_ID,
                None => false,
            }
        })
    }

    pub fn get_path(&self, mut ino: INodeNo) -> String {
        let mut parts = Vec::new();
        while ino != ROOT_ID {
            if let Some(node) = self.replica.tree().find(&ino) {
                if *node.parent_id() == TRASH_ID {
                    return format!("/.trash/{}", ino);
                }
                parts.push(node.metadata().name().to_string());
                ino = *node.parent_id();
            } else {
                break;
            }
        }
        parts.reverse();
        if parts.is_empty() {
            "/".to_string()
        } else {
            format!("/{}", parts.join("/"))
        }
    }

    fn find_by_path(&self, path: &str) -> Option<INodeNo> {
        if path == "/" || path.is_empty() {
            return Some(ROOT_ID);
        }

        let mut current = ROOT_ID;
        for component in path.split('/').filter(|s| !s.is_empty()) {
            current = self.get_child(&current, component)?;
        }
        Some(current)
    }

    fn ensure_path(&mut self, path: &str, is_dir: bool, ops: &mut Vec<NodeOpMove>) -> INodeNo {
        if path == "/" || path.is_empty() {
            return ROOT_ID;
        }

        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let mut ino = ROOT_ID;

        for (i, component) in components.iter().enumerate() {
            let is_last = i == components.len() - 1;
            let target_is_dir = if is_last { is_dir } else { true };

            match self.get_child(&ino, component) {
                Some(child_no) => {
                    ino = child_no;
                }
                None => {
                    let new_ino = self.generate_ino();
                    let meta = if target_is_dir {
                        Meta::Directory {
                            name: component.to_string(),
                        }
                    } else {
                        Meta::File {
                            name: component.to_string(),
                            size: 0,
                            hashes: Vec::new(),
                        }
                    };
                    let op = self.replica.opmove(ino, meta, new_ino);
                    self.replica.apply_op(op.clone());
                    ops.push(op);
                    ino = new_ino;
                }
            }
        }
        ino
    }

    fn generate_ino(&self) -> INodeNo {
        loop {
            let ino = fuser::INodeNo(rand::random::<u64>());
            if ino != ROOT_ID && ino != TRASH_ID {
                return ino;
            }
        }
    }
}
