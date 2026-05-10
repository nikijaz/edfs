use std::error::Error;

use rand::RngExt;
use tokio::io::{AsyncBufReadExt, BufReader, stdin};

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

    let mut swarm = swarm::init(&secret)?;

    if std::env::args().nth(1).is_none() {
        swarm.listen_on("/ip4/0.0.0.0/tcp/0".parse()?)?;
    }

    let mut stdin = BufReader::new(stdin()).lines();

    loop {
        evloop::tick(&mut swarm, &mut stdin).await?;
    }
}
