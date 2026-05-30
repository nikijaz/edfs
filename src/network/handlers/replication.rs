use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use libp2p::{
    PeerId, Swarm, request_response, request_response::OutboundRequestId, swarm::SwarmEvent,
};
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::{
    config::{
        CHUNK_FETCH_BACKOFF, MAX_FETCHES_PER_REBALANCE, MAX_HASHES_PER_PROBE,
        REPLICATION_EVICTION_INTERVAL, REPLICATION_REBALANCE_INTERVAL,
    },
    domain::ChunkHash,
    filesystem::FileSystem,
    network::{
        behaviour::{Behaviour, BehaviourEvent},
        event::{Event, EventContext, EventHandler, SwarmCommand},
        hrw,
        protocol::{Request, Response},
    },
};

fn current_members(swarm: &Swarm<Behaviour>) -> Vec<PeerId> {
    let mut members: Vec<PeerId> = swarm.connected_peers().copied().collect();
    members.push(*swarm.local_peer_id());
    members
}

fn designated_holders(hash: &ChunkHash, members: &[PeerId]) -> Vec<PeerId> {
    hrw::top_peers(hash, members, 2)
}

pub struct ReplicationHandler {
    swarm_tx: mpsc::Sender<SwarmCommand>,
    next_rebalance: Instant,
    next_eviction: Instant,
    blocked_until: HashMap<ChunkHash, Instant>,
    confirmed_holders: HashMap<ChunkHash, HashSet<PeerId>>,
    pending_probes: HashMap<OutboundRequestId, PeerId>,
}

impl ReplicationHandler {
    pub fn new(swarm_tx: mpsc::Sender<SwarmCommand>) -> Self {
        Self {
            swarm_tx,
            next_rebalance: Instant::now(),
            next_eviction: Instant::now(),
            blocked_until: HashMap::new(),
            confirmed_holders: HashMap::new(),
            pending_probes: HashMap::new(),
        }
    }

    fn rebalance(&mut self, swarm: &Swarm<Behaviour>, filesystem: &Arc<RwLock<FileSystem>>) {
        let members = current_members(swarm);
        if members.len() == 1 {
            return;
        }

        let local = *swarm.local_peer_id();
        let now = Instant::now();
        let live = filesystem.read().reachable_chunk_hashes();
        let mut issued = 0;

        for hash in live {
            if !designated_holders(&hash, &members).contains(&local) {
                continue;
            }
            if filesystem.read().is_pinned(&hash) {
                continue;
            }
            if self
                .blocked_until
                .get(&hash)
                .is_some_and(|until| *until > now)
            {
                continue;
            }
            if filesystem.read().is_cached(&hash) {
                if filesystem.write().promote_to_pinned(&hash) {
                    continue;
                }
                self.blocked_until.insert(hash, now + CHUNK_FETCH_BACKOFF);
                continue;
            }
            if issued >= MAX_FETCHES_PER_REBALANCE {
                break;
            }
            if self
                .swarm_tx
                .try_send(SwarmCommand::FetchChunk {
                    hash: hash.clone(),
                    reply: None,
                    pin: true,
                })
                .is_ok()
            {
                issued += 1;
            }
        }
    }

    fn probe_eviction_candidates(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        filesystem: &Arc<RwLock<FileSystem>>,
    ) {
        let members = current_members(swarm);
        let peer_id = *swarm.local_peer_id();
        let reachable = filesystem.read().reachable_chunk_hashes();
        let pinned = filesystem.read().pinned_hashes();

        let candidates: Vec<ChunkHash> = pinned
            .into_iter()
            .filter(|hash| {
                reachable.contains(hash) && !designated_holders(hash, &members).contains(&peer_id)
            })
            .collect();

        if candidates.is_empty() {
            self.confirmed_holders.clear();
            return;
        }
        self.confirmed_holders
            .retain(|hash, _| candidates.contains(hash));

        let mut by_holder: HashMap<PeerId, Vec<ChunkHash>> = HashMap::new();
        for hash in &candidates {
            for peer in designated_holders(hash, &members) {
                let confirmed = self
                    .confirmed_holders
                    .get(hash)
                    .is_some_and(|set| set.contains(&peer));
                if !confirmed {
                    by_holder.entry(peer).or_default().push(hash.clone());
                }
            }
        }

        for (peer, hashes) in by_holder {
            for batch in hashes.chunks(MAX_HASHES_PER_PROBE) {
                let request_id = swarm.behaviour_mut().request_response.send_request(
                    &peer,
                    Request::CheckChunks {
                        hashes: batch.to_vec(),
                    },
                );
                self.pending_probes.insert(request_id, peer);
            }
        }
    }

    fn evict_confirmed(&mut self, swarm: &Swarm<Behaviour>, filesystem: &Arc<RwLock<FileSystem>>) {
        let members = current_members(swarm);
        let live = filesystem.read().reachable_chunk_hashes();

        let mut completed = Vec::new();
        for (hash, confirmed) in &self.confirmed_holders {
            if live.contains(hash)
                && designated_holders(hash, &members)
                    .iter()
                    .all(|peer| confirmed.contains(peer))
            {
                completed.push(hash.clone());
            }
        }

        for hash in completed {
            if filesystem.read().is_pinned(&hash) {
                filesystem.write().evict_chunk(&hash);
            }
            self.confirmed_holders.remove(&hash);
        }
    }
}

impl EventHandler for ReplicationHandler {
    fn handle(&mut self, ctx: &mut EventContext, event: &mut Event) {
        match event {
            Event::Request {
                request: Request::CheckChunks { hashes },
                reply,
                ..
            } => {
                let Some(reply) = reply.take() else {
                    return;
                };
                let held: Vec<ChunkHash> = {
                    let fs = ctx.filesystem.read();
                    hashes
                        .iter()
                        .filter(|hash| fs.is_pinned(hash))
                        .cloned()
                        .collect()
                };
                ctx.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(reply, Response::HeldChunks { hashes: held })
                    .ok();
            }
            Event::Response {
                request_id,
                response: Response::HeldChunks { hashes },
                ..
            } => {
                let Some(peer) = self.pending_probes.remove(request_id) else {
                    return;
                };
                for hash in hashes {
                    self.confirmed_holders
                        .entry(hash.clone())
                        .or_default()
                        .insert(peer);
                }
                self.evict_confirmed(ctx.swarm, ctx.filesystem);
            }
            Event::Swarm(SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure { request_id, .. },
            ))) => {
                self.pending_probes.remove(request_id);
            }
            Event::Swarm(SwarmEvent::ConnectionEstablished { .. }) => {
                self.rebalance(ctx.swarm, ctx.filesystem);
            }
            _ => {}
        }
    }

    fn tick(&mut self, ctx: &mut EventContext) {
        let now = Instant::now();
        if now >= self.next_rebalance {
            self.next_rebalance = now + REPLICATION_REBALANCE_INTERVAL;
            self.rebalance(ctx.swarm, ctx.filesystem);
        }
        if now >= self.next_eviction {
            self.next_eviction = now + REPLICATION_EVICTION_INTERVAL;
            self.probe_eviction_candidates(ctx.swarm, ctx.filesystem);
        }
    }
}
