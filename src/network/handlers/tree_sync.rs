use std::{
    collections::HashSet,
    sync::Arc,
    time::{Duration, Instant},
};

use libp2p::{PeerId, Swarm, swarm::SwarmEvent};
use parking_lot::RwLock;
use rand::RngExt;

use crate::{
    filesystem::{
        FileSystem,
        tree::{NodeOperation, NodeOperationId},
    },
    network::{
        behaviour::Behaviour,
        event::{Event, EventContext, EventHandler},
        protocol::{Request, Response},
    },
};

const SYNC_INTERVAL: Duration = Duration::from_secs(15);

pub struct SyncHandler {
    last_sync: Instant,
}

impl Default for SyncHandler {
    fn default() -> Self {
        Self {
            last_sync: Instant::now(),
        }
    }
}

impl SyncHandler {
    fn synchronize_with(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        filesystem: &Arc<RwLock<FileSystem>>,
        peer_id: PeerId,
    ) {
        let known_ids = filesystem.read().operation_ids();
        swarm
            .behaviour_mut()
            .request_response
            .send_request(&peer_id, Request::PullMissingOperations { known_ids });
    }

    fn apply_operations(&self, fs: &Arc<RwLock<FileSystem>>, operations: Vec<NodeOperation>) {
        if !operations.is_empty() {
            fs.write().apply_operations(operations);
        }
    }
}

impl EventHandler for SyncHandler {
    fn tick(&mut self, ctx: &mut EventContext) {
        if self.last_sync.elapsed() >= SYNC_INTERVAL {
            self.last_sync = Instant::now();
            let peers: Vec<PeerId> = ctx.swarm.connected_peers().copied().collect();
            if !peers.is_empty() {
                let index = rand::rng().random_range(0..peers.len());
                self.synchronize_with(ctx.swarm, ctx.filesystem, peers[index]);
            }
        }
    }

    fn handle(&mut self, ctx: &mut EventContext, event: &mut Event) {
        match event {
            Event::Swarm(SwarmEvent::ConnectionEstablished {
                peer_id,
                num_established,
                ..
            }) if num_established.get() == 1 => {
                self.synchronize_with(ctx.swarm, ctx.filesystem, *peer_id);
            }
            Event::Request {
                request: Request::PullMissingOperations { known_ids },
                reply,
                ..
            } => {
                let Some(reply) = reply.take() else {
                    return;
                };
                let known_ids: HashSet<NodeOperationId> = known_ids.iter().cloned().collect();
                let operations = ctx.filesystem.read().operations_missing_from(&known_ids);
                let _ = ctx
                    .swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(reply, Response::MissingOperations { operations });
            }
            Event::Response {
                response: Response::MissingOperations { operations },
                ..
            } => {
                self.apply_operations(ctx.filesystem, operations.clone());
            }
            _ => {}
        }
    }
}
