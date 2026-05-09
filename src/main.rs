use std::{error::Error, time::Duration};

use libp2p::{
    Swarm, Transport,
    core::upgrade,
    futures::StreamExt,
    identity,
    kad::{self, store::MemoryStore},
    mdns, noise,
    swarm::{Config, NetworkBehaviour, SwarmEvent},
    tcp, yamux,
};
use rand::RngExt;
use sha2::Digest;
use tokio::time::interval;

#[derive(NetworkBehaviour)]
struct Behaviour {
    kademlia: kad::Behaviour<MemoryStore>,
    mdns: mdns::tokio::Behaviour,
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
                kademlia: kad::Behaviour::new(
                    identity.public().to_peer_id(),
                    MemoryStore::new(identity.public().to_peer_id()),
                ),
                mdns: mdns::tokio::Behaviour::new(
                    mdns::Config::default(),
                    identity.public().to_peer_id(),
                )?,
            })
        })?
        .with_swarm_config(|cfg: Config| {
            cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX))
        })
        .build();

    swarm
        .behaviour_mut()
        .kademlia
        .set_mode(Some(kad::Mode::Server));

    if std::env::args().nth(1).is_none() {
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    }

    loop {
        tokio::select! {
            event = swarm.select_next_some() => handler(&mut swarm, event)?,
        }
    }
}

fn handler(
    swarm: &mut Swarm<Behaviour>,
    event: SwarmEvent<BehaviourEvent>,
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
        _ => {}
    }
    Ok(())
}
