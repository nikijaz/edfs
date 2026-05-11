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
    let secret = std::env::args().nth(1).unwrap_or_else(|| {
        format!(
            "{}-{:0>4x}",
            petname::petname(2, "-").unwrap(),
            rand::rng().random_range(0..=0xffff)
        )
    });
    println!("{secret}");

    let mountpoint = std::env::args().nth(2).unwrap_or_else(|| "mnt".to_string());
    std::fs::create_dir_all(&mountpoint)?;

    let mut swarm = swarm::init(&secret)?;
    let storage = Arc::new(storage::Storage::new(1024 * 1024 * 100)); // 100MB cache

    let (command_tx, command_rx) = mpsc::channel(100);

    let fs = fuse::Fuse::new(storage.clone(), command_tx);

    let mut config = fuser::Config::default();
    config.mount_options = vec![
        MountOption::FSName("edfs".to_string()),
        MountOption::AutoUnmount,
        MountOption::CUSTOM("allow_other".to_string()),
    ];

    let mount_handle = tokio::task::spawn_blocking(move || fuser::mount2(fs, mountpoint, &config));

    if std::env::args().nth(1).is_none() {
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    }

    let mut stdin = BufReader::new(stdin()).lines();
    let mut command_rx = command_rx;
    let mut state = evloop::LoopState::new();

    loop {
        evloop::tick(
            &mut swarm,
            &mut stdin,
            &mut command_rx,
            &storage,
            &mut state,
        )
        .await?;
    }
}
