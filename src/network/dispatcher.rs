use std::{error::Error, sync::Arc};

use libp2p::{Swarm, futures::StreamExt};
use parking_lot::RwLock;
use tokio::sync::mpsc;

use crate::{
    config::DISPATCH_TICK_INTERVAL,
    filesystem::FileSystem,
    network::{
        behaviour::Behaviour,
        event::{Event, EventContext, EventHandler, SwarmCommand},
    },
};

pub struct Dispatcher {
    handlers: Vec<Box<dyn EventHandler>>,
}

impl Dispatcher {
    pub fn builder() -> DispatcherBuilder {
        DispatcherBuilder {
            handlers: Vec::new(),
        }
    }

    pub async fn tick(
        &mut self,
        swarm: &mut Swarm<Behaviour>,
        rx: &mut mpsc::Receiver<SwarmCommand>,
        filesystem: &Arc<RwLock<FileSystem>>,
    ) -> Result<(), Box<dyn Error>> {
        let mut event = tokio::select! {
            cmd = rx.recv() => match cmd {
                Some(cmd) => cmd.into(),
                None => return Err("command channel unavailable".into()),
            },
            event = swarm.select_next_some() => event.into(),
            _ = tokio::time::sleep(DISPATCH_TICK_INTERVAL) => Event::Tick,
        };

        {
            let mut ctx = EventContext { swarm, filesystem };
            for handler in &mut self.handlers {
                handler.handle(&mut ctx, &mut event);
            }
            for handler in &mut self.handlers {
                handler.tick(&mut ctx);
            }
        }

        Ok(())
    }
}

pub struct DispatcherBuilder {
    handlers: Vec<Box<dyn EventHandler>>,
}

impl DispatcherBuilder {
    pub fn handler(mut self, handler: impl EventHandler + 'static) -> Self {
        self.handlers.push(Box::new(handler));
        self
    }

    pub fn build(self) -> Dispatcher {
        Dispatcher {
            handlers: self.handlers,
        }
    }
}
