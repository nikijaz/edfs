use std::collections::HashMap;

use fuser::INodeNo;
use libp2p::PeerId;
use serde::{Deserialize, Serialize};

use crate::{protocol::FsEvent, storage::INode};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageTree {
    nodes: HashMap<INodeNo, INode>,
    last_ino: INodeNo,
    tombstones: HashMap<String, INodeNo>,
}

impl StorageTree {
    pub fn new() -> Self {
        let mut tree = Self {
            nodes: HashMap::new(),
            last_ino: INodeNo::ROOT,
            tombstones: HashMap::new(),
        };

        tree.nodes.insert(
            INodeNo::ROOT,
            INode::Directory {
                ino: INodeNo::ROOT,
                pino: INodeNo::ROOT,
                name: "".to_string(),
                author: PeerId::random(),
                mtime: 0,
                children: HashMap::new(),
            },
        );

        tree
    }

    pub fn apply(&mut self, event: FsEvent, author: PeerId, mtime: u128) -> Option<INodeNo> {
        match event {
            FsEvent::CreateDirectory { path } => self.ensure_path(&path, true, author, mtime),
            FsEvent::SetFile { path, size, hashes } => {
                let ino = self.ensure_path(&path, false, author, mtime);
                if let Some(ino) = ino {
                    if let Some(INode::File {
                        size: size_old,
                        hashes: hashes_old,
                        mtime: mtime_old,
                        author: author_old,
                        ..
                    }) = self.nodes.get_mut(&ino)
                    {
                        if mtime > *mtime_old || (mtime == *mtime_old && author > *author_old) {
                            *size_old = size;
                            *hashes_old = hashes;
                            *mtime_old = mtime;
                            *author_old = author;
                        }
                    }
                }
                ino
            }
            FsEvent::Delete { path } => {
                if let Some(ino) = self.find_by_path(&path) {
                    if ino != INodeNo::ROOT {
                        if let Some(node) = self.nodes.get(&ino) {
                            let (parent, name, mtime_old, author_old) = match node {
                                INode::File {
                                    pino: parent,
                                    name,
                                    mtime,
                                    author,
                                    ..
                                }
                                | INode::Directory {
                                    pino: parent,
                                    name,
                                    mtime,
                                    author,
                                    ..
                                } => (*parent, name.clone(), *mtime, *author),
                                INode::Tombstone { .. } => return None,
                            };

                            if mtime > mtime_old || (mtime == mtime_old && author > author_old) {
                                if let Some(INode::Directory { children, .. }) =
                                    self.nodes.get_mut(&parent)
                                {
                                    children.remove(&name);
                                }
                                self.nodes
                                    .insert(ino, INode::Tombstone { ino, mtime, author });
                                self.tombstones.insert(path, ino);
                            }
                        }
                    }
                } else if let Some(&ino) = self.tombstones.get(&path) {
                    if let Some(INode::Tombstone {
                        mtime: m_old,
                        author: a_old,
                        ..
                    }) = self.nodes.get_mut(&ino)
                    {
                        if mtime > *m_old || (mtime == *m_old && author > *a_old) {
                            *m_old = mtime;
                            *a_old = author;
                        }
                    }
                } else {
                    self.last_ino.0 += 1;
                    let ino = self.last_ino;
                    self.nodes
                        .insert(ino, INode::Tombstone { ino, mtime, author });
                    self.tombstones.insert(path, ino);
                }
                None
            }
        }
    }

    pub fn get(&self, ino: &INodeNo) -> Option<&INode> {
        match self.nodes.get(ino) {
            Some(INode::Tombstone { .. }) => None,
            node => node,
        }
    }

    pub fn get_child(&self, pino: &INodeNo, name: &str) -> Option<INodeNo> {
        self.nodes.get(pino).and_then(|node| match node {
            INode::Directory { children, .. } => children.get(name).copied(),
            _ => None,
        })
    }

    pub fn get_path(&self, mut ino: INodeNo) -> String {
        let mut parts = Vec::new();
        while ino != INodeNo::ROOT {
            if let Some(node) = self.nodes.get(&ino) {
                match node {
                    INode::File {
                        pino: parent, name, ..
                    }
                    | INode::Directory {
                        pino: parent, name, ..
                    } => {
                        parts.push(name.clone());
                        ino = *parent;
                    }
                    INode::Tombstone { .. } => panic!("get_path() faced Tombstone"),
                }
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

    fn insert_child(
        &mut self,
        pino: INodeNo,
        name: String,
        is_dir: bool,
        author: PeerId,
        mtime: u128,
    ) -> INodeNo {
        self.last_ino.0 += 1;
        let ino = self.last_ino;

        let node = if !is_dir {
            INode::File {
                ino,
                pino,
                name: name.clone(),
                author,
                mtime,
                size: 0,
                hashes: Vec::new(),
            }
        } else {
            INode::Directory {
                ino,
                pino,
                name: name.clone(),
                author,
                mtime,
                children: HashMap::new(),
            }
        };

        self.nodes.insert(ino, node);
        if let Some(INode::Directory { children, .. }) = self.nodes.get_mut(&pino) {
            children.insert(name, ino);
        }

        ino
    }

    fn find_by_path(&self, path: &str) -> Option<INodeNo> {
        if path == "/" || path.is_empty() {
            return Some(INodeNo::ROOT);
        }

        let mut current = INodeNo::ROOT;
        for component in path.split("/").filter(|s| !s.is_empty()) {
            current = self.get_child(&current, component)?;
        }
        Some(current)
    }

    fn ensure_path(
        &mut self,
        path: &str,
        is_dir: bool,
        author: PeerId,
        mtime: u128,
    ) -> Option<INodeNo> {
        if path == "/" || path.is_empty() {
            return Some(INodeNo::ROOT);
        }

        let components: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();
        let mut ino = INodeNo::ROOT;
        let mut path = String::new();

        for (i, component) in components.iter().enumerate() {
            let is_last = i == components.len() - 1;
            let target_is_dir = if is_last { is_dir } else { true };

            path.push('/');
            path.push_str(component);

            if let Some(&tomb_ino) = self.tombstones.get(&path) {
                if let Some(INode::Tombstone {
                    mtime: mtime_old,
                    author: author_old,
                    ..
                }) = self.nodes.get(&tomb_ino)
                {
                    if mtime < *mtime_old || (mtime == *mtime_old && author < *author_old) {
                        return None;
                    } else {
                        self.tombstones.remove(&path.clone());
                    }
                }
            }

            match self.get_child(&ino, component) {
                Some(child_no) => {
                    ino = child_no;
                }
                None => {
                    ino =
                        self.insert_child(ino, component.to_string(), target_is_dir, author, mtime);
                }
            }
        }
        Some(ino)
    }
}
