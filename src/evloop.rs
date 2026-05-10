use std::error::Error;

use libp2p::{Swarm, futures::StreamExt, mdns, swarm::SwarmEvent};
use tokio::io::{BufReader, Lines, Stdin};

use crate::swarm::{Behaviour, BehaviourEvent};

pub async fn tick(
    swarm: &mut Swarm<Behaviour>,
    stdin: &mut Lines<BufReader<Stdin>>,
) -> Result<(), Box<dyn Error>> {
    tokio::select! {
        event = swarm.select_next_some() => tick_swarm(swarm, event),
        Ok(Some(event)) = stdin.next_line() => tick_stdin(stdin, event),
    }
}

fn tick_swarm(
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

#[allow(unused)]
fn tick_stdin(stdin: &mut Lines<BufReader<Stdin>>, event: String) -> Result<(), Box<dyn Error>> {
    Ok(())
}
