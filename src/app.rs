use std::{error::Error, sync::Arc};

use crate::{
    cli::Args,
    config::{MOUNT_STATUS_POLL_INTERVAL, SWARM_CHANNEL_CAPACITY},
    filesystem::{FileSystem, storage::ChunkStorage, tree::NodeTree},
    fuse::FuseFileSystem,
    network::{
        NetworkBridge, behaviour,
        dispatcher::Dispatcher,
        handlers::{
            chunk::ChunkHandler, gossip::GossipHandler, mdns::MdnsHandler, tree_sync::SyncHandler,
        },
    },
};
use libp2p::identity;
use parking_lot::RwLock;
use tokio::sync::mpsc;
use tracing::{error, info};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

pub async fn run(args: Args) -> Result<(), Box<dyn Error>> {
    let filter = tracing_subscriber::EnvFilter::new(format!("edfs={}", args.log_level));
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer())
        .init();

    let identity = identity::Keypair::generate_ed25519();
    let peer_id = identity.public().to_peer_id();
    let storage_bytes = args.max_storage as usize * 1024 * 1024;
    let filesystem = Arc::new(RwLock::new(FileSystem::new(
        NodeTree::new(peer_id),
        ChunkStorage::new(storage_bytes),
    )));

    let (swarm_tx, mut swarm_rx) = mpsc::channel(SWARM_CHANNEL_CAPACITY);
    let network_bridge = NetworkBridge::new(swarm_tx);
    let fuse = FuseFileSystem::new(filesystem.clone(), network_bridge);
    let mount_session = fuse.mount(&args.mountpoint).await?;

    let mut dispatcher = Dispatcher::builder()
        .handler(MdnsHandler::default())
        .handler(GossipHandler::default())
        .handler(SyncHandler::default())
        .handler(ChunkHandler::default())
        .build();

    let mut swarm = behaviour::build_swarm(identity, &args.secret)?;
    swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;

    info!("started successfully");

    let mut shutdown_error: Option<Box<dyn Error>> = None;
    {
        let ctrl_c = tokio::signal::ctrl_c();
        tokio::pin!(ctrl_c);

        let mut mount_watch = tokio::time::interval(MOUNT_STATUS_POLL_INTERVAL);
        let mount_wait = mount_session.wait();
        tokio::pin!(mount_wait);

        loop {
            tokio::select! {
                _ = &mut ctrl_c => break,
                result = &mut mount_wait => {
                    match result {
                        Ok(()) => info!("filesystem unmounted"),
                        Err(error) => {
                            error!("FUSE session failed: {error}");
                            shutdown_error = Some(error.into());
                        }
                    }
                    break;
                }
                _ = mount_watch.tick() => {
                    if !mount_session.mounted() {
                        info!("filesystem unmounted externally");
                        break;
                    }
                }
                result = dispatcher.tick(&mut swarm, &mut swarm_rx, &filesystem) => {
                    if let Err(error) = result {
                        error!("network dispatcher failed: {error}");
                        shutdown_error = Some(error);
                        break;
                    }
                }
            }
        }
    }

    mount_session.unmount().await;

    if let Some(error) = shutdown_error {
        Err(error)
    } else {
        info!("stopped successfully");
        Ok(())
    }
}
