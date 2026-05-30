use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
    time::Instant,
};

use libp2p::{
    PeerId, Swarm, request_response, request_response::OutboundRequestId, swarm::SwarmEvent,
};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use crate::{
    config::{CHUNK_FETCH_BACKOFF, MAX_FETCH_TARGETS},
    domain::ChunkHash,
    filesystem::FileSystem,
    network::{
        behaviour::{Behaviour, BehaviourEvent},
        event::{ChunkReply, Event, EventContext, EventHandler, SwarmCommand},
        hrw::top_peers,
        protocol::{ProtocolError, Request, Response},
    },
};

fn next_targets(
    hash: &ChunkHash,
    connected: &[PeerId],
    tried: &HashSet<PeerId>,
    limit: usize,
) -> Vec<PeerId> {
    top_peers(hash, connected, connected.len())
        .into_iter()
        .filter(|peer| !tried.contains(peer))
        .take(limit)
        .collect()
}

struct FetchSession {
    waiters: Vec<ChunkReply>,
    attempts: HashMap<OutboundRequestId, PeerId>,
    tried: HashSet<PeerId>,
    pin: bool,
}

#[derive(Default)]
struct ChunkFetcher {
    sessions: HashMap<ChunkHash, FetchSession>,
    requests: HashMap<OutboundRequestId, ChunkHash>,
    blocked_until: HashMap<ChunkHash, Instant>,
}

impl ChunkFetcher {
    fn fetch_chunk(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        hash: ChunkHash,
        reply: Option<ChunkReply>,
        pin: bool,
    ) {
        if let Some(session) = self.sessions.get_mut(&hash) {
            if let Some(reply) = reply {
                session.waiters.push(reply);
            }
            session.pin |= pin;
            return;
        }
        if !pin && self.is_blocked(&hash) {
            if let Some(reply) = reply {
                reply.send(None).ok();
            }
            return;
        }
        self.sessions.insert(
            hash.clone(),
            FetchSession {
                waiters: reply.into_iter().collect(),
                attempts: HashMap::new(),
                pin,
                tried: HashSet::new(),
            },
        );
        self.walk_down(swarm, hash);
    }

    fn is_blocked(&self, hash: &ChunkHash) -> bool {
        self.blocked_until
            .get(hash)
            .is_some_and(|until| *until > Instant::now())
    }

    fn walk_down(&mut self, swarm: &mut Swarm<Behaviour>, hash: ChunkHash) {
        let connected: Vec<PeerId> = swarm.connected_peers().copied().collect();
        let targets = match self.sessions.get(&hash) {
            Some(session) => next_targets(&hash, &connected, &session.tried, MAX_FETCH_TARGETS),
            None => return,
        };

        if targets.is_empty() {
            let session = self.sessions.remove(&hash).unwrap();
            for reply in session.waiters {
                let _ = reply.send(None);
            }
            if session.pin {
                self.blocked_until
                    .insert(hash, Instant::now() + CHUNK_FETCH_BACKOFF);
            }
            return;
        }

        for peer in targets {
            let request_id = swarm
                .behaviour_mut()
                .request_response
                .send_request(&peer, Request::PullChunk { hash: hash.clone() });
            if let Some(session) = self.sessions.get_mut(&hash) {
                session.attempts.insert(request_id, peer);
                session.tried.insert(peer);
            }
            self.requests.insert(request_id, hash.clone());
        }
    }

    fn handle_response(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        filesystem: &Arc<RwLock<FileSystem>>,
        request_id: OutboundRequestId,
        data: &[u8],
    ) {
        let Some(hash) = self.requests.remove(&request_id) else {
            return;
        };
        let actual_hash = ChunkHash::from_bytes(Sha256::digest(data).into());
        if actual_hash != hash {
            self.on_attempt_failed(swarm, &hash, &request_id);
            return;
        }

        let Some(session) = self.sessions.remove(&hash) else {
            return;
        };
        for attempt_id in session.attempts.keys() {
            self.requests.remove(attempt_id);
        }

        if session.pin {
            if filesystem.write().pin_chunk(data.to_vec()).is_err() {
                filesystem.write().cache_chunk(data.to_vec());
                self.blocked_until
                    .insert(hash, Instant::now() + CHUNK_FETCH_BACKOFF);
            }
        } else {
            filesystem.write().cache_chunk(data.to_vec());
        }

        for reply in session.waiters {
            let _ = reply.send(Some(data.to_vec()));
        }
    }

    fn handle_failure(&mut self, swarm: &mut Swarm<Behaviour>, request_id: OutboundRequestId) {
        let Some(hash) = self.requests.remove(&request_id) else {
            return;
        };
        self.on_attempt_failed(swarm, &hash, &request_id);
    }

    fn on_attempt_failed(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        hash: &ChunkHash,
        request_id: &OutboundRequestId,
    ) {
        let Some(session) = self.sessions.get_mut(hash) else {
            return;
        };
        session.attempts.remove(request_id);
        if !session.attempts.is_empty() {
            return;
        }
        self.walk_down(swarm, hash.clone());
    }
}

#[derive(Default)]
pub struct ChunkHandler {
    fetcher: ChunkFetcher,
}

impl EventHandler for ChunkHandler {
    fn handle(&mut self, ctx: &mut EventContext, event: &mut Event) {
        match event {
            Event::Command(SwarmCommand::FetchChunk { hash, reply, pin }) => {
                self.fetcher
                    .fetch_chunk(ctx.swarm, hash.clone(), reply.take(), *pin);
            }
            Event::Request {
                request: Request::PullChunk { hash },
                reply,
                ..
            } => {
                let Some(reply) = reply.take() else {
                    return;
                };
                let response = match ctx.filesystem.read().chunk(hash) {
                    Some(data) => Response::ChunkData(data),
                    None => Response::Error(ProtocolError::ChunkNotFound),
                };
                ctx.swarm
                    .behaviour_mut()
                    .request_response
                    .send_response(reply, response)
                    .ok();
            }
            Event::Response {
                request_id,
                response: Response::ChunkData(data),
                ..
            } => self
                .fetcher
                .handle_response(ctx.swarm, ctx.filesystem, *request_id, data),
            Event::Response {
                request_id,
                response: Response::Error(_),
                ..
            }
            | Event::Swarm(SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure { request_id, .. },
            ))) => self.fetcher.handle_failure(ctx.swarm, *request_id),
            _ => {}
        }
    }
}
