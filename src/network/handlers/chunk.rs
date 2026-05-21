use std::{collections::HashMap, sync::Arc};

use libp2p::{
    PeerId, Swarm, request_response, request_response::OutboundRequestId, swarm::SwarmEvent,
};
use parking_lot::RwLock;
use sha2::Digest;

use crate::{
    domain::ChunkHash,
    filesystem::FileSystem,
    network::{
        behaviour::{Behaviour, BehaviourEvent},
        event::{ChunkReply, Event, EventContext, EventHandler, SwarmCommand},
        protocol::{ProtocolError, Request, Response},
    },
};

const MAX_PEERS_PER_FETCH: usize = 3;

#[derive(Default)]
struct FetchSession {
    waiters: Vec<ChunkReply>,
    attempts: HashMap<OutboundRequestId, PeerId>,
}

struct TrackedRequest {
    hash: ChunkHash,
    peer: PeerId,
}

#[derive(Default)]
struct ChunkTracker {
    sessions: HashMap<ChunkHash, FetchSession>,
    requests: HashMap<OutboundRequestId, TrackedRequest>,
}

impl ChunkTracker {
    fn is_fetching(&self, hash: &ChunkHash) -> bool {
        self.sessions.contains_key(hash)
    }

    fn has_requested_from(&self, hash: &ChunkHash, peer: &PeerId) -> bool {
        self.requests
            .values()
            .any(|request| &request.hash == hash && &request.peer == peer)
    }

    fn add_waiter(&mut self, hash: ChunkHash, reply: ChunkReply) {
        self.sessions.entry(hash).or_default().waiters.push(reply);
    }

    fn track_request(&mut self, id: OutboundRequestId, hash: ChunkHash, peer: PeerId) {
        self.requests.insert(
            id,
            TrackedRequest {
                hash: hash.clone(),
                peer,
            },
        );
        self.sessions
            .entry(hash)
            .or_default()
            .attempts
            .insert(id, peer);
    }

    fn has_active_attempts(&self, hash: &ChunkHash) -> bool {
        self.sessions
            .get(hash)
            .is_some_and(|session| !session.attempts.is_empty())
    }

    fn expected_hash(&self, id: &OutboundRequestId) -> Option<&ChunkHash> {
        self.requests.get(id).map(|request| &request.hash)
    }

    fn complete_success(&mut self, id: OutboundRequestId) -> Option<(ChunkHash, Vec<ChunkReply>)> {
        let request = self.requests.remove(&id)?;
        let session = self.sessions.remove(&request.hash)?;
        for attempt_id in session.attempts.keys() {
            self.requests.remove(attempt_id);
        }
        Some((request.hash, session.waiters))
    }

    fn record_failure(&mut self, id: OutboundRequestId) -> Option<Vec<ChunkReply>> {
        let request = self.requests.remove(&id)?;
        let session = self.sessions.get_mut(&request.hash)?;
        session.attempts.remove(&id);
        if !session.attempts.is_empty() {
            return None;
        }
        Some(self.sessions.remove(&request.hash)?.waiters)
    }

    fn fail_session(&mut self, hash: &ChunkHash) -> Vec<ChunkReply> {
        self.sessions.remove(hash).map_or_else(Vec::new, |session| {
            for attempt_id in session.attempts.keys() {
                self.requests.remove(attempt_id);
            }
            session.waiters
        })
    }
}

#[derive(Default)]
struct ChunkFetcher {
    tracker: ChunkTracker,
}

impl ChunkFetcher {
    fn fetch_chunk(&mut self, swarm: &mut Swarm<Behaviour>, hash: ChunkHash, reply: ChunkReply) {
        if self.tracker.is_fetching(&hash) {
            self.tracker.add_waiter(hash, reply);
            return;
        }

        self.tracker.add_waiter(hash.clone(), reply);
        let peers: Vec<PeerId> = swarm.connected_peers().copied().collect();
        for peer in peers.into_iter().take(MAX_PEERS_PER_FETCH) {
            self.send_request(swarm, &hash, peer);
        }
        if !self.tracker.has_active_attempts(&hash) {
            for reply in self.tracker.fail_session(&hash) {
                let _ = reply.send(None);
            }
        }
    }

    fn handle_response(
        &mut self,
        filesystem: &Arc<RwLock<FileSystem>>,
        request_id: OutboundRequestId,
        data: &[u8],
    ) {
        let Some(expected_hash) = self.tracker.expected_hash(&request_id).cloned() else {
            return;
        };
        let actual_hash = ChunkHash::from_bytes(sha2::Sha256::digest(data).into());
        if actual_hash != expected_hash {
            self.handle_failure(request_id);
            return;
        }
        if let Some((hash, waiters)) = self.tracker.complete_success(request_id) {
            filesystem.write().cache_chunk(hash, data.to_vec());
            for reply in waiters {
                let _ = reply.send(Some(data.to_vec()));
            }
        }
    }

    fn handle_failure(&mut self, request_id: OutboundRequestId) {
        if let Some(waiters) = self.tracker.record_failure(request_id) {
            for reply in waiters {
                let _ = reply.send(None);
            }
        }
    }

    fn send_request(&mut self, swarm: &mut Swarm<Behaviour>, hash: &ChunkHash, peer: PeerId) {
        if self.tracker.has_requested_from(hash, &peer) {
            return;
        }
        let request_id = swarm
            .behaviour_mut()
            .request_response
            .send_request(&peer, Request::PullChunk { hash: hash.clone() });
        self.tracker.track_request(request_id, hash.clone(), peer);
    }
}

#[derive(Default)]
pub struct ChunkHandler {
    fetcher: ChunkFetcher,
}

impl EventHandler for ChunkHandler {
    fn handle(&mut self, ctx: &mut EventContext, event: &mut Event) {
        match event {
            Event::Command(SwarmCommand::FetchChunk { hash, reply }) => {
                if let Some(reply) = reply.take() {
                    self.fetcher.fetch_chunk(ctx.swarm, hash.clone(), reply);
                }
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
                .handle_response(ctx.filesystem, *request_id, data),
            Event::Response {
                request_id,
                response: Response::Error(_),
                ..
            }
            | Event::Swarm(SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::OutboundFailure { request_id, .. },
            ))) => self.fetcher.handle_failure(*request_id),
            _ => {}
        }
    }
}
