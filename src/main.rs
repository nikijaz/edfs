use std::{collections::HashSet, error::Error, time::Duration};

use libp2p::{
    Swarm, Transport,
    core::upgrade,
    futures::StreamExt,
    gossipsub, identity,
    kad::{self, store::MemoryStore},
    mdns, noise,
    request_response::{self, OutboundRequestId, ProtocolSupport, cbor},
    swarm::{Config, NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use rand::RngExt;
use serde::{Deserialize, Serialize};
use sha2::Digest;
use tokio::io::{AsyncBufReadExt, BufReader, stdin};

#[derive(Debug, Serialize, Deserialize)]
enum Request {
    Message(String),
}

#[derive(Debug, Serialize, Deserialize)]
enum Response {
    Ack,
}

#[derive(NetworkBehaviour)]
struct Behaviour {
    gossipsub: gossipsub::Behaviour,
    kademlia: kad::Behaviour<MemoryStore>,
    mdns: mdns::tokio::Behaviour,
    request_response: cbor::Behaviour<Request, Response>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let secret = std::env::args().nth(1).unwrap_or_else(|| {
        format!(
            "{}-{:0>4x}",
            petname::petname(2, "-").unwrap(),
            rand::rng().random_range(0..=0xffff)
        )
    });
    println!("{secret}");
    let secret_hash = sha2::Sha256::digest(secret).to_vec();

    let identity = identity::Keypair::generate_ed25519();

    let noise = noise::Config::new(&identity)?.with_prologue(secret_hash);
    let yamux = yamux::Config::default();
    let transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(upgrade::Version::V1)
        .authenticate(noise)
        .multiplex(yamux)
        .boxed();

    let mut swarm = libp2p::SwarmBuilder::with_existing_identity(identity)
        .with_tokio()
        .with_other_transport(|_| transport)?
        .with_behaviour(|identity| {
            Ok(Behaviour {
                gossipsub: gossipsub::Behaviour::new(
                    gossipsub::MessageAuthenticity::Signed(identity.clone()),
                    gossipsub::Config::default(),
                )?,
                kademlia: kad::Behaviour::new(
                    identity.public().to_peer_id(),
                    MemoryStore::new(identity.public().to_peer_id()),
                ),
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    identity.public().to_peer_id(),
                )?,
                request_response: cbor::Behaviour::<Request, Response>::new(
                    [(
                        libp2p::StreamProtocol::new("/edfs/dev"),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default(),
                ),
            })
        })?
        .with_swarm_config(|cfg: Config| {
            cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX))
        })
        .build();

    let topic = gossipsub::IdentTopic::new("edfs-metadata");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    if std::env::args().nth(1).is_none() {
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    }

    let mut stdin = BufReader::new(stdin()).lines();
    let mut pending_requests: HashSet<OutboundRequestId> = HashSet::new();

    loop {
        tokio::select! {
            Ok(Some(line)) = stdin.next_line() => {
                let peers: Vec<_> = swarm.connected_peers().copied().collect();
                for peer in peers {
                    let req_id = swarm.behaviour_mut().request_response.send_request(&peer, Request::Message(line.clone()));
                    pending_requests.insert(req_id);
                }
            }
            event = swarm.select_next_some() => handler(&mut swarm, event, &mut pending_requests)?,
        }
    }
}

fn handler(
    swarm: &mut Swarm<Behaviour>,
    event: SwarmEvent<BehaviourEvent>,
    pending_requests: &mut HashSet<OutboundRequestId>,
) -> Result<(), Box<dyn Error>> {
    match event {
        SwarmEvent::ConnectionEstablished { .. } if swarm.listeners().count() == 0 => {
            swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
        }

        SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
            for (peer_id, addr) in list {
                swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
            }
        }

        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
            request_response::Event::Message {
                message:
                    request_response::Message::Request {
                        request: Request::Message(msg),
                        channel,
                        ..
                    },
                ..
            },
        )) => {
            println!("{msg}");
            let _ = swarm
                .behaviour_mut()
                .request_response
                .send_response(channel, Response::Ack);
        }
        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
            request_response::Event::Message {
                message:
                    request_response::Message::Response {
                        request_id,
                        response: Response::Ack,
                    },
                ..
            },
        )) => {
            if pending_requests.remove(&request_id) {
                println!("Ack");
            }
        }
        SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
            request_response::Event::OutboundFailure { request_id, .. },
        )) => {
            pending_requests.remove(&request_id);
        }

        _ => {}
    }
    Ok(())
}
