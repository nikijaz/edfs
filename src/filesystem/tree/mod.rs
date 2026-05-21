use std::collections::HashSet;

use crdt_tree::{Clock, OpMove, TreeReplica};
use libp2p::PeerId;
use uuid::Uuid;

use crate::{
    domain::{NodeId, NodeMeta},
    filesystem::tree::{error::NodeTreeError, hlc::HybridLogicalClock},
};
pub mod error;
mod hlc;

impl NodeId {
    pub const ROOT: Self = Self::from_uuid(Uuid::nil());
    pub const TRASH: Self = Self::from_uuid(Uuid::max());

    fn is_reserved(self) -> bool {
        self == Self::ROOT || self == Self::TRASH
    }
}

pub type NodeOperation = OpMove<NodeId, NodeMeta, PeerId>;
pub type NodeOperationId = (PeerId, u64);

pub struct NodeTree {
    peer_id: PeerId,
    tree: TreeReplica<NodeId, NodeMeta, PeerId>,
    clock: HybridLogicalClock,
}

impl NodeTree {
    pub fn new(peer_id: PeerId) -> Self {
        Self {
            peer_id,
            tree: TreeReplica::new(peer_id),
            clock: HybridLogicalClock::new(),
        }
    }

    fn move_operation(
        &mut self,
        node_id: NodeId,
        parent_id: NodeId,
        metadata: NodeMeta,
    ) -> NodeOperation {
        OpMove::new(
            Clock::new(self.peer_id, Some(self.clock.next())),
            parent_id,
            metadata,
            node_id,
        )
    }

    pub fn create_node(
        &mut self,
        parent_id: NodeId,
        node_id: NodeId,
        metadata: NodeMeta,
    ) -> NodeOperation {
        let operation = self.move_operation(node_id, parent_id, metadata);
        self.tree.apply_op(operation.clone());
        operation
    }

    pub fn move_node(
        &mut self,
        node_id: NodeId,
        parent_id: NodeId,
        metadata: NodeMeta,
    ) -> Result<NodeOperation, NodeTreeError> {
        if self.metadata(node_id).is_none() {
            return Err(NodeTreeError::NotFound);
        }
        let operation = self.move_operation(node_id, parent_id, metadata);
        self.tree.apply_op(operation.clone());
        Ok(operation)
    }

    pub fn delete_node(&mut self, node_id: NodeId) -> Result<NodeOperation, NodeTreeError> {
        if node_id.is_reserved() {
            return Err(NodeTreeError::ReservedNode);
        }
        let metadata = self
            .metadata(node_id)
            .ok_or(NodeTreeError::NotFound)?
            .clone();
        let operation = self.move_operation(node_id, NodeId::TRASH, metadata);
        self.tree.apply_op(operation.clone());
        Ok(operation)
    }

    pub fn metadata(&self, node_id: NodeId) -> Option<&NodeMeta> {
        self.tree.tree().find(&node_id).map(|node| node.metadata())
    }

    pub fn parent_id(&self, node_id: NodeId) -> Option<NodeId> {
        self.tree
            .tree()
            .find(&node_id)
            .map(|node| *node.parent_id())
    }

    pub fn children(&self, parent_id: NodeId) -> Result<Vec<NodeId>, NodeTreeError> {
        if !parent_id.is_reserved() {
            self.tree
                .tree()
                .find(&parent_id)
                .ok_or(NodeTreeError::NotFound)?;
        }
        Ok(self.tree.tree().children(&parent_id))
    }

    pub fn is_trashed(&self, node_id: NodeId) -> bool {
        self.parent_id(node_id) == Some(NodeId::TRASH)
    }

    pub fn apply_operations(&mut self, operations: Vec<NodeOperation>) {
        for operation in &operations {
            self.clock.observe(operation.timestamp().counter());
        }
        self.tree.apply_ops(operations);
    }

    pub fn operation_ids(&self) -> Vec<NodeOperationId> {
        self.tree
            .state()
            .log()
            .iter()
            .map(|log_entry| {
                (
                    *log_entry.timestamp().actor_id(),
                    log_entry.timestamp().counter(),
                )
            })
            .collect()
    }

    pub fn operations_missing_from(&self, ids: &HashSet<NodeOperationId>) -> Vec<NodeOperation> {
        self.tree
            .state()
            .log()
            .iter()
            .filter(|log_entry| {
                !ids.contains(&(
                    *log_entry.timestamp().actor_id(),
                    log_entry.timestamp().counter(),
                ))
            })
            .map(|log_entry| log_entry.clone().op_into())
            .collect()
    }
}
