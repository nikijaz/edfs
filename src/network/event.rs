use std::sync::Arc;

use libp2p::{Swarm, gossipsub, request_response, swarm::SwarmEvent};
use parking_lot::RwLock;
use tokio::sync::oneshot;

use crate::{
    domain::ChunkHash,
    filesystem::{FileSystem, tree::NodeOperation},
    network::{
        behaviour::{Behaviour, BehaviourEvent},
        protocol::{Request, Response},
    },
};

pub type ChunkReply = oneshot::Sender<Option<Vec<u8>>>;

pub enum SwarmCommand {
    BroadcastOperations {
        operations: Vec<NodeOperation>,
    },
    FetchChunk {
        hash: ChunkHash,
        reply: Option<ChunkReply>,
        pin: bool,
    },
}

pub enum Event {
    Tick,
    Swarm(SwarmEvent<BehaviourEvent>),
    Request {
        request: Request,
        reply: Option<request_response::ResponseChannel<Response>>,
    },
    Response {
        request_id: request_response::OutboundRequestId,
        response: Response,
    },
    Gossip {
        data: Vec<u8>,
    },
    Command(SwarmCommand),
}

impl From<SwarmCommand> for Event {
    fn from(cmd: SwarmCommand) -> Self {
        Self::Command(cmd)
    }
}

impl From<SwarmEvent<BehaviourEvent>> for Event {
    fn from(event: SwarmEvent<BehaviourEvent>) -> Self {
        match event {
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::Message {
                    message:
                        request_response::Message::Request {
                            request, channel, ..
                        },
                    ..
                },
            )) => Self::Request {
                request,
                reply: Some(channel),
            },
            SwarmEvent::Behaviour(BehaviourEvent::RequestResponse(
                request_response::Event::Message {
                    message:
                        request_response::Message::Response {
                            request_id,
                            response,
                        },
                    ..
                },
            )) => Self::Response {
                request_id,
                response,
            },
            SwarmEvent::Behaviour(BehaviourEvent::Gossipsub(gossipsub::Event::Message {
                message,
                ..
            })) => Self::Gossip { data: message.data },
            other => Self::Swarm(other),
        }
    }
}

pub struct EventContext<'a> {
    pub swarm: &'a mut Swarm<Behaviour>,
    pub filesystem: &'a Arc<RwLock<FileSystem>>,
}

pub trait EventHandler {
    fn handle(&mut self, _ctx: &mut EventContext, _event: &mut Event) {}
    fn tick(&mut self, _ctx: &mut EventContext) {}
}
