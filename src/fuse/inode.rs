use std::collections::HashMap;

use async_fuser::INodeNo;

use crate::domain::NodeId;

pub trait INodeNoExt {
    fn from_raw(value: u64) -> Self;
    fn as_raw(&self) -> u64;
}

impl INodeNoExt for INodeNo {
    fn from_raw(value: u64) -> Self {
        Self(value)
    }

    fn as_raw(&self) -> u64 {
        self.0
    }
}

pub struct INodeMap {
    ino_to_id: HashMap<INodeNo, NodeId>,
    id_to_ino: HashMap<NodeId, INodeNo>,
    next_ino: INodeNo,
}

impl INodeMap {
    pub fn new() -> Self {
        let mut ino_to_id = HashMap::new();
        let mut id_to_ino = HashMap::new();
        ino_to_id.insert(INodeNo::ROOT, NodeId::ROOT);
        id_to_ino.insert(NodeId::ROOT, INodeNo::ROOT);
        Self {
            ino_to_id,
            id_to_ino,
            next_ino: INodeNo::from_raw(INodeNo::ROOT.as_raw() + 1),
        }
    }

    pub fn get_or_assign_ino(&mut self, node_id: NodeId) -> INodeNo {
        if let Some(&ino) = self.id_to_ino.get(&node_id) {
            return ino;
        }
        let ino = self.next_ino;
        self.next_ino = INodeNo::from_raw(self.next_ino.as_raw() + 1);
        self.ino_to_id.insert(ino, node_id);
        self.id_to_ino.insert(node_id, ino);
        ino
    }

    pub fn get_node_id(&self, ino: INodeNo) -> Option<NodeId> {
        self.ino_to_id.get(&ino).copied()
    }
}
