use std::sync::Arc;

use libp2p::{Swarm, gossipsub};
use parking_lot::RwLock;
use tracing::error;

use crate::{
    config::NETWORK_GOSSIP_TOPIC,
    filesystem::{FileSystem, tree::NodeOperation},
    network::{
        behaviour::Behaviour,
        event::{Event, EventContext, EventHandler, SwarmCommand},
        protocol::Gossip,
    },
};

use bincode_next::{
    config,
    serde::{decode_from_slice, encode_to_vec},
};

#[derive(Default)]
pub struct GossipHandler;

impl GossipHandler {
    fn broadcast(&self, swarm: &mut Swarm<Behaviour>, operations: &[NodeOperation]) {
        let Ok(bytes) = encode_to_vec(
            &Gossip {
                operations: operations.to_vec(),
            },
            config::standard(),
        ) else {
            error!("failed to serialize gossip message");
            return;
        };
        let topic = gossipsub::IdentTopic::new(NETWORK_GOSSIP_TOPIC);
        if let Err(error) = swarm.behaviour_mut().gossipsub.publish(topic, bytes) {
            error!("failed to publish gossip message: {error}");
        }
    }

    fn apply_gossip(&self, filesystem: &Arc<RwLock<FileSystem>>, data: &[u8]) {
        let Ok((gossip, _)) = decode_from_slice::<Gossip, _>(data, config::standard()) else {
            error!("failed to deserialize gossip message");
            return;
        };
        filesystem.write().apply_operations(gossip.operations);
    }
}

impl EventHandler for GossipHandler {
    fn handle(&mut self, ctx: &mut EventContext, event: &mut Event) {
        match event {
            Event::Command(SwarmCommand::BroadcastOperations { operations }) => {
                self.broadcast(ctx.swarm, operations);
            }
            Event::Gossip { data, .. } => self.apply_gossip(ctx.filesystem, data),
            _ => {}
        }
    }
}
