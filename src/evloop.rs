use std::{
    collections::{HashMap, VecDeque},
    error::Error,
    sync::Arc,
};

use libp2p::{Swarm, futures::StreamExt, gossipsub, mdns, request_response, swarm::SwarmEvent};
use rand::seq::SliceRandom;
use tokio::sync::{mpsc, oneshot};

use crate::{
    protocol::{Gossip, Request, Response, SwarmCommand},
    storage::{Data, Hash, Storage},
    swarm::{Behaviour, BehaviourEvent},
};

pub struct LoopState {
    pub gossips: VecDeque<Gossip>,
    pub is_syncing: bool,
    pub request_id_to_chunk_hash: HashMap<request_response::OutboundRequestId, Hash>,
    pub chunk_hash_to_fuse: HashMap<Hash, Vec<oneshot::Sender<Option<Data>>>>,
}

impl LoopState {
    pub fn new() -> Self {
        Self {
            gossips: VecDeque::new(),
            is_syncing: true,
            request_id_to_chunk_hash: HashMap::new(),
            chunk_hash_to_fuse: HashMap::new(),
        }
    }
}

pub async fn tick(
    swarm: &mut Swarm<Behaviour>,
    rx: &mut mpsc::Receiver<SwarmCommand>,
    storage: &Arc<Storage>,
    state: &mut LoopState,
) -> Result<(), Box<dyn Error>> {
    tokio::select! {
        event = swarm.select_next_some() => tick_swarm(swarm, event, storage, state).await?,
        Some(cmd) = rx.recv() => tick_command(swarm, cmd, state).await?,
    }
    Ok(())
}

async fn tick_command(
    swarm: &mut Swarm<Behaviour>,
    cmd: SwarmCommand,
    state: &mut LoopState,
) -> Result<(), Box<dyn Error>> {
    match cmd {
        SwarmCommand::BroadcastOperation { operation } => {
            log::info!("Broadcasting CRDT operation");
            if !operation.is_empty() {
                let gossip = Gossip { operation };
                if let Ok(encoded) = bincode::serialize(&gossip) {
                    let topic = gossipsub::IdentTopic::new("edfs-metadata");
                    if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, encoded) {
                        log::error!("Failed to publish gossip: {:?}", e);
                    }
                }
            }
        }
        SwarmCommand::FetchChunk { hash, reply } => {
            let mut peers: Vec<_> = swarm.connected_peers().cloned().collect();

            if peers.is_empty() {
                log::warn!("No connected peers to fetch chunk");
                let _ = reply.send(None);
                return Ok(());
            }

            let mut rng = rand::rng();
            peers.shuffle(&mut rng);
            let peers = if peers.len() > 3 {
                peers[..3].to_vec()
            } else {
                peers
            };

            state
                .chunk_hash_to_fuse
                .entry(hash.clone())
                .or_default()
                .push(reply);

            for peer in peers {
                let request_id = swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer, Request::PullChunk { hash: hash.clone() });
                state
                    .request_id_to_chunk_hash
                    .insert(request_id, hash.clone());
            }
        }
    }
    Ok(())
}

async fn tick_swarm(
    swarm: &mut Swarm<Behaviour>,
    event: SwarmEvent<BehaviourEvent>,
    storage: &Arc<Storage>,
    state: &mut LoopState,
) -> Result<(), Box<dyn Error>> {
    match event {
        SwarmEvent::NewListenAddr { address, .. } => {
            log::info!("Listening on {:?}", address);
        }

        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            log::info!("Connection established with {:?}", peer_id);
            if swarm.listeners().count() == 0 {
                swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
            }
            if state.is_syncing {
                log::info!("Requesting tree state from {:?}", peer_id);
                swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer_id, Request::PullTree);
            }
        }

        SwarmEvent::ConnectionClosed { peer_id, cause, .. } => {
            log::info!("Connection with {:?} closed: {:?}", peer_id, cause);
        }

        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            for (peer_id, addr) in list {
                log::info!("mDNS discovered peer {:?} at {:?}", peer_id, addr);
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            }
        }

        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
            message,
            ..
        })) => {
            if let Ok(gossip) = bincode::deserialize::<Gossip>(&message.data) {
                if state.is_syncing {
                    log::info!("Queuing gossip from {:?} while syncing", message.source);
                    state.gossips.push_back(gossip);
                } else {
                    log::info!("Applying gossip from {:?}", message.source);
                    let mut tree = storage.tree.write().unwrap();
                    for op in gossip.operation {
                        tree.replica.apply_op(op);
                    }
                }
            }
        }

        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
            request_response::Event::Message { peer, message, .. },
        )) => match message {
            request_response::Message::Request {
                request, channel, ..
            } => match request {
                Request::PullTree => {
                    log::info!("Pushing tree...");
                    let tree = storage.tree.read().unwrap().clone();
                    let _ = swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, Response::TreeState(tree));
                }
                Request::PullChunk { hash } => {
                    let data = storage.content.read().unwrap().get(&hash).cloned();
                    if let Some(data) = data {
                        let _ = swarm
                            .behaviour_mut()
                            .request_response
                            .send_response(channel, Response::Data(data));
                    } else {
                        log::warn!("Chunk {:?} requested by {:?} not found", hash, peer);
                        let _ = swarm.behaviour_mut().request_response.send_response(
                            channel,
                            Response::Error(crate::protocol::Error::ChunkNotFound),
                        );
                    }
                }
                Request::PushChunk { hash, data } => {
                    storage.content.write().unwrap().insert(hash, data);
                    let _ = swarm
                        .behaviour_mut()
                        .request_response
                        .send_response(channel, Response::Ack);
                }
            },

            request_response::Message::Response {
                response,
                request_id,
                ..
            } => match response {
                Response::TreeState(tree) if state.is_syncing => {
                    log::info!(
                        "Received tree state from {:?}. Applying queued gossips: {}",
                        peer,
                        state.gossips.len()
                    );
                    *storage.tree.write().unwrap() = tree;
                    state.is_syncing = false;
                    while let Some(gossip) = state.gossips.pop_front() {
                        let mut tree = storage.tree.write().unwrap();
                        for op in gossip.operation {
                            tree.replica.apply_op(op);
                        }
                    }
                }
                Response::Data(data) => {
                    if let Some(hash) = state.request_id_to_chunk_hash.remove(&request_id)
                        && let Some(replies) = state.chunk_hash_to_fuse.remove(&hash)
                    {
                        for reply in replies {
                            let _ = reply.send(Some(data.clone()));
                        }
                        state.request_id_to_chunk_hash.retain(|_, h| h != &hash);
                    }
                }
                Response::Error(e) => {
                    if let Some(hash) = state.request_id_to_chunk_hash.remove(&request_id) {
                        log::warn!(
                            "Peer {:?} returned error for hash {:?}: {:?}",
                            peer,
                            hash,
                            e
                        );
                        if !state.request_id_to_chunk_hash.values().any(|h| h == &hash)
                            && let Some(replies) = state.chunk_hash_to_fuse.remove(&hash)
                        {
                            for reply in replies {
                                let _ = reply.send(None);
                            }
                        }
                    }
                }
                _ => {}
            },
        },

        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
            request_response::Event::OutboundFailure {
                request_id, error, ..
            },
        )) => {
            log::error!(
                "Request-Response outbound failure for id {:?}: {:?}",
                request_id,
                error
            );
            if let Some(hash) = state.request_id_to_chunk_hash.remove(&request_id)
                && !state.request_id_to_chunk_hash.values().any(|h| h == &hash)
                && let Some(replies) = state.chunk_hash_to_fuse.remove(&hash)
            {
                for reply in replies {
                    let _ = reply.send(None);
                }
            }
        }

        _ => {}
    }
    Ok(())
}
