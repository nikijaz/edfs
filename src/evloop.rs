use std::{collections::VecDeque, error::Error, sync::Arc};

use libp2p::{Swarm, futures::StreamExt, gossipsub, mdns, request_response, swarm::SwarmEvent};
use tokio::{
    io::{BufReader, Lines, Stdin},
    sync::mpsc,
};

use crate::{
    protocol::{Gossip, Request, Response, SwarmCommand},
    storage::Storage,
    swarm::{Behaviour, BehaviourEvent},
};

pub struct LoopState {
    pub gossip_queue: VecDeque<Gossip>,
    pub syncing: bool,
}

impl LoopState {
    pub fn new() -> Self {
        Self {
            gossip_queue: VecDeque::new(),
            syncing: true,
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
        Some(cmd) = command_rx.recv() => tick_command(swarm, cmd, storage).await?,
    }
    Ok(())
}

async fn tick_command(
    swarm: &mut Swarm<Behaviour>,
    cmd: SwarmCommand,
    storage: &Arc<Storage>,
) -> Result<(), Box<dyn Error>> {
    match cmd {
        SwarmCommand::ApplyFsEvent { event, reply } => {
            let mtime = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos();
            let author = swarm.local_peer_id().clone();

            let ino = storage
                .tree
                .write()
                .unwrap()
                .apply(event.clone(), author, mtime);

            // Broadcast gossip
            let gossip = Gossip { mtime, event };
            if let Ok(encoded) = bincode::serialize(&gossip) {
                let topic = gossipsub::IdentTopic::new("edfs-metadata");
                let _ = swarm.behaviour_mut().gossipsub.publish(topic, encoded);
            }

            let _ = reply.send(ino);
        }
        SwarmCommand::FetchChunk { hash, reply } => {
            let mut peers = Vec::new();
            for bucket in swarm.behaviour_mut().kademlia.kbuckets() {
                for entry in bucket.iter() {
                    peers.push(entry.node.key.preimage().clone());
                }
            }

            for peer in peers {
                swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer, Request::PullChunk { hash: hash.clone() });
            }
            let _ = reply.send(None);
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
        SwarmEvent::ConnectionEstablished { peer_id, .. } => {
            if swarm.listeners().count() == 0 {
                swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
            }
            if state.syncing {
                swarm
                    .behaviour_mut()
                    .request_response
                    .send_request(&peer_id, Request::PullTree);
            }
        }

        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            for (peer_id, addr) in list {
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            }
        }

        SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
            message,
            ..
        })) => {
            if let Ok(gossip) = bincode::deserialize::<Gossip>(&message.data) {
                if state.syncing {
                    state.gossip_queue.push_back(gossip);
                } else {
                    storage.tree.write().unwrap().apply(
                        gossip.event,
                        message
                            .source
                            .unwrap_or_else(|| swarm.local_peer_id().clone()),
                        gossip.mtime,
                    );
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
            request_response::Message::Response { response, .. } => match response {
                Response::TreeState(tree) => {
                    if state.syncing {
                        *storage.tree.write().unwrap() = tree;
                        state.syncing = false;
                        while let Some(gossip) = state.gossip_queue.pop_front() {
                            storage.tree.write().unwrap().apply(
                                gossip.event,
                                peer.clone(),
                                gossip.mtime,
                            );
                        }
                    }
                }
                _ => {}
            },
        },

        _ => {}
    }
    Ok(())
}

fn tick_stdin(_stdin: &mut Lines<BufReader<Stdin>>, _line: String) -> Result<(), Box<dyn Error>> {
    Ok(())
}
