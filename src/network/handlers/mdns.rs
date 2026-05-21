use libp2p::{mdns, swarm::SwarmEvent};

use crate::network::{
    behaviour::BehaviourEvent,
    event::{Event, EventContext, EventHandler},
};

#[derive(Default)]
pub struct MdnsHandler;

impl EventHandler for MdnsHandler {
    fn handle(&mut self, ctx: &mut EventContext, event: &mut Event) {
        if let Event::Swarm(SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(
            list,
        )))) = event
        {
            for (peer_id, addr) in list {
                if peer_id == ctx.swarm.local_peer_id() {
                    continue;
                }
                let already_connected = ctx.swarm.connected_peers().any(|peer| peer == peer_id);
                if !already_connected {
                    let _ = ctx.swarm.dial(addr.clone());
                }
            }
        }
    }
}
