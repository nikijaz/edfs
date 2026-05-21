use std::error::Error;

use clap::Parser;

mod app;
mod cli;
mod config;
mod domain;
mod filesystem;
mod fuse;
mod network;
mod port;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    app::run(cli::Args::parse()).await
}
