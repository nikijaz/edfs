use std::error::Error;

use libp2p::{
    Swarm, Transport,
    core::upgrade,
    gossipsub, identity,
    kad::{self, store::MemoryStore},
    mdns, noise,
    request_response::{self, ProtocolSupport, cbor},
    swarm::NetworkBehaviour,
    tcp, yamux,
};
use sha2::Digest;

use crate::protocol::{Request, Response};

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub kademlia: kad::Behaviour<MemoryStore>,
    pub mdns: mdns::tokio::Behaviour,
    pub request_response: cbor::Behaviour<Request, Response>,
}

pub fn init(secret: &str) -> Result<Swarm<Behaviour>, Box<dyn Error>> {
    let secret_hash = sha2::Sha256::digest(secret.to_string()).to_vec();
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
                    gossipsub::ConfigBuilder::default()
                        .max_transmit_size(2 * 1024 * 1024)
                        .build()
                        .expect("Valid gossipsub config"),
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
                    request_response::Config::default()
                        .with_request_timeout(std::time::Duration::from_secs(60)),
                ),
            })
        })?
        .build();

    let topic = gossipsub::IdentTopic::new("edfs-metadata");
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    Ok(swarm)
}
