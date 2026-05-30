use std::error::Error;

use libp2p::{
    Swarm, Transport,
    core::upgrade,
    gossipsub, identity, mdns, noise,
    request_response::{self, ProtocolSupport, cbor},
    swarm::NetworkBehaviour,
    tcp, yamux,
};
use sha2::{Digest, Sha256};

use crate::{
    config::{
        MAX_GOSSIP_MESSAGE_BYTES, NETWORK_GOSSIP_TOPIC, NETWORK_PROTOCOL_NAME,
        NETWORK_REQUEST_TIMEOUT,
    },
    network::protocol::{Request, Response},
};

#[derive(NetworkBehaviour)]
pub struct Behaviour {
    pub gossipsub: gossipsub::Behaviour,
    pub mdns: mdns::tokio::Behaviour,
    pub request_response: cbor::Behaviour<Request, Response>,
}

pub fn build_swarm(
    identity: identity::Keypair,
    secret: &str,
) -> Result<Swarm<Behaviour>, Box<dyn Error>> {
    let secret_hash = Sha256::digest(secret).to_vec();

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
                        .max_transmit_size(MAX_GOSSIP_MESSAGE_BYTES)
                        .build()?,
                )?,
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    identity.public().to_peer_id(),
                )?,
                request_response: cbor::Behaviour::<Request, Response>::new(
                    [(
                        libp2p::StreamProtocol::new(NETWORK_PROTOCOL_NAME),
                        ProtocolSupport::Full,
                    )],
                    request_response::Config::default()
                        .with_request_timeout(NETWORK_REQUEST_TIMEOUT),
                ),
            })
        })?
        .build();

    let topic = gossipsub::IdentTopic::new(NETWORK_GOSSIP_TOPIC);
    swarm.behaviour_mut().gossipsub.subscribe(&topic)?;

    Ok(swarm)
}
