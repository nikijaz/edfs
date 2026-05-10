use std::collections::HashMap;

use fuser::INodeNo;

use crate::storage::{FileName, Hash, INode};

pub struct StorageTree {
    nodes: HashMap<INodeNo, INode>,
    last_inode: INodeNo,
}

impl StorageTree {
    pub fn new() -> Self {
        let mut tree = Self {
            nodes: HashMap::new(),
            last_inode: INodeNo::ROOT,
        };

        tree.nodes.insert(
            INodeNo::ROOT,
            INode::Directory {
                inode: INodeNo::ROOT,
                parent: INodeNo::ROOT,
                name: "".to_string(),
                children: HashMap::new(),
            },
        );

        tree
    }

    pub fn get(&self, inode: INodeNo) -> Option<&INode> {
        self.nodes.get(&inode)
    }

    pub fn get_child(&self, parent: INodeNo, name: &str) -> Option<INodeNo> {
        self.nodes.get(&parent).and_then(|node| match node {
            INode::Directory { children, .. } => children.get(name).copied(),
            INode::File { .. } => None,
        })
    }

    pub fn get_children(&self, parent: INodeNo) -> Option<&HashMap<FileName, INodeNo>> {
        self.nodes.get(&parent).and_then(|node| match node {
            INode::Directory { children, .. } => Some(children),
            INode::File { .. } => None,
        })
    }

    pub fn add(&mut self, parent: INodeNo, name: FileName, is_dir: bool) -> INodeNo {
        self.last_inode.0 += 1;
        let inode = self.last_inode;

        let node = if is_dir {
            INode::Directory {
                inode,
                parent,
                name: name.clone(),
                children: HashMap::new(),
            }
        } else {
            INode::File {
                inode,
                parent,
                name: name.clone(),
                size: 0,
                hashes: Vec::new(),
            }
        };

        self.nodes.insert(inode, node);
        if let Some(INode::Directory { children, .. }) = self.nodes.get_mut(&parent) {
            children.insert(name, inode);
        }

        inode
    }

    pub fn remove(&mut self, parent: INodeNo, name: &str) -> Option<INodeNo> {
        let inode = self.get_child(parent, name)?;

        if let Some(INode::Directory { children, .. }) = self.nodes.get_mut(&parent) {
            children.remove(name);
        }

        self.nodes.remove(&inode);
        Some(inode)
    }

    pub fn get_path(&self, mut inode: INodeNo) -> String {
        let mut path = Vec::new();
        while inode != INodeNo::ROOT {
            if let Some(node) = self.get(inode) {
                match node {
                    INode::File { name, parent, .. } | INode::Directory { name, parent, .. } => {
                        path.push(name.clone());
                        inode = *parent;
                    }
                }
            } else {
                break;
            }
        }
        path.reverse();
        format!("/{}", path.join("/"))
    }

    pub fn update(&mut self, inode: INodeNo, size: u64, hashes: Vec<Hash>) {
        if let Some(INode::File {
            size: s, hashes: h, ..
        }) = self.nodes.get_mut(&inode)
        {
            *s = size;
            *h = hashes;
        }
    }
}
