use std::{error::Error, time::Duration};

use libp2p::{
    Transport,
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
    let mut rng = rand::rng(); // TODO: Make It Cryptographic
    let key = std::env::args().nth(1).unwrap_or_else(|| {
        format!(
            "{}-{:0>4x}",
            petname::petname(2, "-").unwrap(),
            rng.random_range(0..=0xffff)
        )
    });
    println!("Mnemonic ID: {key}");

    let hash = sha2::Sha256::digest(key).to_vec();
    println!(
        "Hash: {}",
        hash.iter()
            .map(|b| format!("{:02x}", b))
            .collect::<String>()
    );

    let identity = identity::Keypair::generate_ed25519();
    let transport = tcp::tokio::Transport::new(tcp::Config::default())
        .upgrade(upgrade::Version::V1)
        .authenticate(noise::Config::new(&identity).unwrap().with_prologue(hash))
        .multiplex(yamux::Config::default())
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
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    let mut interval = interval(Duration::from_secs(5));
    loop {
        tokio::select! {
            _ = interval.tick() => {
                println!("Number of connected peers: {}", swarm.connected_peers().count());
            }
            event = swarm.select_next_some() => {
                #[allow(clippy::single_match)]
                match event {
                    SwarmEvent::Behaviour(BehaviourEvent::Mdns(mdns::Event::Discovered(list))) => {
                        for (peer_id, addr) in list {
                            swarm.behaviour_mut().kademlia.add_address(&peer_id, addr);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}
