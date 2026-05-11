use std::{error::Error, sync::Arc};

use rand::RngExt;
use tokio::sync::mpsc;

use crate::{evloop::LoopState, fuse::Fuse};

mod evloop;
mod fuse;
mod protocol;
mod storage;
mod swarm;

pub const CHUNK_SIZE_BYTES: u64 = 512 * 1024; // 512 KB
pub const STORAGE_SIZE_BYTES: usize = 128 * 1024 * 1024; // 128 MB

pub const FUSE_TO_SWARM_CMD_BUFFER: usize = 128;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let secret = std::env::args()
        .nth(1)
        .filter(|secret| secret != "-")
        .unwrap_or_else(|| {
            format!(
                "{}-{:0>4x}",
                petname::petname(2, "-").unwrap(),
                rand::rng().random_range(0..=0xffff)
            )
        });
    log::info!("Secret: {}", secret);

    let mut swarm = swarm::init(&secret)?;
    let storage = Arc::new(storage::Storage::new(STORAGE_SIZE_BYTES));

    let mountpoint = std::env::args().nth(2).unwrap_or_else(|| "mnt".to_string());
    std::fs::create_dir_all(&mountpoint)?;

    let (tx, rx) = mpsc::channel(FUSE_TO_SWARM_CMD_BUFFER);
    let mut rx = rx;

    let fuse_thread = match fuser::spawn_mount2(
        Fuse::new(storage.clone(), tx),
        &mountpoint,
        &fuser::Config::default(),
    ) {
        Ok(session) => session,
        Err(e) => {
            log::error!("FUSE mount failed");
            return Err(e.into());
        }
    };

    if std::env::args().nth(1).is_none() {
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    }

    log::info!("EDFS Started!");

    let mut loop_state = LoopState::new();
    loop {
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {
                log::info!("Ctrl+C received, shutting down...");
                break;
            }
            _ = evloop::tick(&mut swarm, &mut rx, &storage, &mut loop_state) => {}
        }
    }

    log::info!("Shutting down event loop...");
    drop(rx);

    log::info!("Cleaning up mountpoint...");
    drop(fuse_thread);

    if let Err(e) = std::fs::remove_dir(&mountpoint) {
        log::warn!("Failed to remove mountpoint directory: {:?}", e);
    }

    Ok(())
}
