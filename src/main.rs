use std::{error::Error, time::Duration};

use libp2p::{Multiaddr, futures::StreamExt, noise, ping, swarm::SwarmEvent, tcp, yamux};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let mut swarm = libp2p::SwarmBuilder::with_new_identity()
        .with_tokio()
        .with_tcp(
            tcp::Config::default(),
            noise::Config::new,
            yamux::Config::default,
        )?
        .with_behaviour(|_| ping::Behaviour::default())?
        .with_swarm_config(|cfg| cfg.with_idle_connection_timeout(Duration::from_secs(u64::MAX)))
        .build();

    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    if let Some(addr) = std::env::args().nth(1) {
        let remote: Multiaddr = addr.parse()?;
        swarm.dial(remote)?;
    }

    loop {
        match swarm.select_next_some().await {
            SwarmEvent::NewListenAddr { address, .. } => println!("Listening on {address:?}"),
            SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                println!("Connected to {peer_id:?}")
            }
            SwarmEvent::ConnectionClosed { peer_id, .. } => {
                println!("Disconnected from {peer_id:?}")
            }
            _ => {}
        }
    }
}
