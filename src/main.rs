use std::{error::Error, sync::Arc};

use fuser::MountOption;
use rand::RngExt;
use tokio::{
    io::{AsyncBufReadExt, BufReader, stdin},
    sync::mpsc,
};

mod evloop;
mod fuse;
mod protocol;
mod storage;
mod swarm;

pub const CHUNK_SIZE_BYTES: u64 = 1024;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init_from_env(env_logger::Env::default().default_filter_or("info"));

    let secret = std::env::args().nth(1).unwrap_or_else(|| {
        format!(
            "{}-{:0>4x}",
            petname::petname(2, "-").unwrap(),
            rand::rng().random_range(0..=0xffff)
        )
    });
    log::info!("Secret: {secret}");

    let mountpoint = std::env::args().nth(2).unwrap_or_else(|| "mnt".to_string());
    std::fs::create_dir_all(&mountpoint)?;
    log::info!("Mountpoint: {mountpoint}");

    let mut swarm = swarm::init(&secret)?;
    let storage = Arc::new(storage::Storage::new(1024 * 1024 * 100)); // 100MB cache

    let (command_tx, command_rx) = mpsc::channel(100);

    let fs = fuse::Fuse::new(storage.clone(), command_tx);

    let mut config = fuser::Config::default();
    config.mount_options = vec![
        MountOption::FSName("edfs".to_string()),
    ];

    let m_point = mountpoint.clone();
    tokio::task::spawn_blocking(move || {
        if let Err(e) = fuser::mount2(fs, m_point, &config) {
            log::error!("FUSE mount failed: {:?}", e);
        }
    });

    if std::env::args().nth(1).is_none() {
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    }

    let mut stdin = BufReader::new(stdin()).lines();
    let mut command_rx = command_rx;
    let mut state = evloop::LoopState::new();

    log::info!("EDFS started. Press Ctrl+C to exit.");

    tokio::select! {
        _ = tokio::signal::ctrl_c() => {
            log::info!("Ctrl+C received, shutting down...");
        }
        res = async {
            loop {
                evloop::tick(&mut swarm, &mut stdin, &mut command_rx, &storage, &mut state).await?;
            }
            #[allow(unreachable_code)]
            Ok::<(), Box<dyn Error>>(())
        } => {
            if let Err(e) = res {
                log::error!("Event loop error: {:?}", e);
            }
        }
    }

    // Cleanup
    log::info!("Shutting down event loop...");
    drop(command_rx);
    
    log::info!("Cleaning up mountpoint: {}", mountpoint);
    let _ = std::process::Command::new("fusermount")
        .arg("-uz")
        .arg(&mountpoint)
        .status();
    let _ = std::fs::remove_dir(mountpoint);

    Ok(())
}
