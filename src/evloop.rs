use std::{collections::VecDeque, error::Error, sync::Arc};

use libp2p::{
    Swarm,
    futures::StreamExt,
    gossipsub,
    mdns,
    request_response,
    swarm::SwarmEvent,
};
use tokio::{
    io::{BufReader, Lines, Stdin},
    sync::{mpsc, oneshot},
};

use crate::{
    protocol::{Gossip, Request, Response, SwarmCommand},
    storage::{Data, Storage},
    swarm::{Behaviour, BehaviourEvent},
};

pub struct LoopState {
    pub gossip_queue: VecDeque<Gossip>,
    pub syncing: bool,
    pub pending_fetches: std::collections::HashMap<request_response::OutboundRequestId, oneshot::Sender<Option<Data>>>,
}

impl LoopState {
    pub fn new() -> Self {
        Self {
            gossip_queue: VecDeque::new(),
            syncing: true,
            pending_fetches: std::collections::HashMap::new(),
        }
    }
}

pub async fn tick(
    swarm: &mut Swarm<Behaviour>,
    stdin: &mut Lines<BufReader<Stdin>>,
    command_rx: &mut mpsc::Receiver<SwarmCommand>,
    storage: &Arc<Storage>,
    state: &mut LoopState,
) -> Result<(), Box<dyn Error>> {
    tokio::select! {
        event = swarm.select_next_some() => tick_swarm(swarm, event, storage, state).await?,
        Ok(Some(line)) = stdin.next_line() => tick_stdin(stdin, line)?,
        Some(cmd) = command_rx.recv() => tick_command(swarm, cmd, storage, state).await?,
    }
    Ok(())
}

async fn tick_command(
    swarm: &mut Swarm<Behaviour>,
    cmd: SwarmCommand,
    storage: &Arc<Storage>,
    state: &mut LoopState,
) -> Result<(), Box<dyn Error>> {
    match cmd {
        SwarmCommand::ApplyFsEvent { event, reply } => {
            log::debug!("Applying local FS event: {:?}", event);
            let mtime = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let author = swarm.local_peer_id().clone();

            let ino = storage.tree.write().unwrap().apply(event.clone(), author, mtime);

            // Broadcast gossip
            let gossip = Gossip { mtime, event };
            if let Ok(encoded) = bincode::serialize(&gossip) {
                let topic = gossipsub::IdentTopic::new("edfs-metadata");
                if let Err(e) = swarm.behaviour_mut().gossipsub.publish(topic, encoded) {
                    log::error!("Failed to publish gossip: {:?}", e);
                } else {
                    log::debug!("Published gossip for event");
                }
            }

            let _ = reply.send(ino);
        }
        SwarmCommand::FetchChunk { hash, reply } => {
            log::debug!("Fetching chunk: {:?}", hash);
            let peers: Vec<_> = swarm.connected_peers().cloned().collect();
            
            if peers.is_empty() {
                log::warn!("No connected peers to fetch chunk");
                let _ = reply.send(None);
                return Ok(());
            }

            // Simple strategy: ask the first connected peer
            let peer = peers[0];
            log::debug!("Requesting chunk from peer: {:?}", peer);
            let request_id = swarm.behaviour_mut().request_response.send_request(&peer, Request::PullChunk { hash: hash.clone() });
            state.pending_fetches.insert(request_id, reply);
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

        SwarmEvent::IncomingConnection { .. } => {
            log::debug!("Incoming connection attempt");
        }

        SwarmEvent::Dialing { peer_id, .. } => {
            log::debug!("Dialing peer {:?}", peer_id);
        }

        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            log::info!("Connection established with {:?}", peer_id);
            if swarm.listeners().count() == 0 {
                swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
            }
            if state.syncing {
                log::info!("Requesting tree state from {:?}", peer_id);
                swarm.behaviour_mut().request_response.send_request(&peer_id, Request::PullTree);
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

        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message { message, .. })) => {
            if let Ok(gossip) = bincode::deserialize::<Gossip>(&message.data) {
                if state.syncing {
                    log::info!("Queuing gossip from {:?} while syncing", message.source);
                    state.gossip_queue.push_back(gossip);
                } else {
                    log::info!("Applying gossip from {:?}", message.source);
                    storage.tree.write().unwrap().apply(gossip.event, message.source.unwrap_or_else(|| swarm.local_peer_id().clone()), gossip.mtime);
                }
            }
        }

        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(request_response::Event::Message { peer, message, .. })) => {
            match message {
                request_response::Message::Request { request, channel, .. } => {
                    log::debug!("Received request from {:?}: {:?}", peer, request);
                    match request {
                        Request::PullTree => {
                            let tree = storage.tree.read().unwrap().clone();
                            let _ = swarm.behaviour_mut().request_response.send_response(channel, Response::TreeState(tree));
                        }
                        Request::PullChunk { hash } => {
                            let data = storage.content.read().unwrap().get(&hash).cloned();
                            if let Some(data) = data {
                                log::debug!("Serving chunk {:?} to {:?}", hash, peer);
                                let _ = swarm.behaviour_mut().request_response.send_response(channel, Response::Data(data));
                            } else {
                                log::warn!("Chunk {:?} requested by {:?} not found", hash, peer);
                                let _ = swarm.behaviour_mut().request_response.send_response(channel, Response::Error(crate::protocol::Error::ChunkNotFound));
                            }
                        }
                        Request::PushChunk { hash, data } => {
                            storage.content.write().unwrap().insert(hash, data);
                            let _ = swarm.behaviour_mut().request_response.send_response(channel, Response::Ack);
                        }
                    }
                }
                request_response::Message::Response { response, request_id, .. } => {
                    log::debug!("Received response from {:?}: {:?}", peer, response);
                    match response {
                        Response::TreeState(tree) => {
                            if state.syncing {
                                log::info!("Received tree state from {:?}. Applying queued gossips: {}", peer, state.gossip_queue.len());
                                *storage.tree.write().unwrap() = tree;
                                state.syncing = false;
                                while let Some(gossip) = state.gossip_queue.pop_front() {
                                    storage.tree.write().unwrap().apply(gossip.event, peer.clone(), gossip.mtime);
                                }
                            }
                        }
                        Response::Data(data) => {
                            if let Some(reply) = state.pending_fetches.remove(&request_id) {
                                let _ = reply.send(Some(data));
                            }
                        }
                        Response::Error(e) => {
                            log::warn!("Peer {:?} returned error: {:?}", peer, e);
                            if let Some(reply) = state.pending_fetches.remove(&request_id) {
                                let _ = reply.send(None);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(request_response::Event::OutboundFailure { request_id, error, .. })) => {
            log::error!("Request-Response outbound failure: {:?}", error);
            if let Some(reply) = state.pending_fetches.remove(&request_id) {
                let _ = reply.send(None);
            }
        }

        _ => {}
    }
    Ok(())
}

fn tick_stdin(_stdin: &mut Lines<BufReader<Stdin>>, _line: String) -> Result<(), Box<dyn Error>> {
    Ok(())
}
