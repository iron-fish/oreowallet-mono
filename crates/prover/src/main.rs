use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use prover::run_prover;
use utils::{handle_signals, initialize_logger};

#[derive(Parser, Debug, Clone)]
pub struct Command {
    /// The ip:port server will listen on
    #[clap(short, long, default_value = "0.0.0.0:10002")]
    pub listen: SocketAddr,
    /// Set your logger level
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Command::parse();
    let Command { listen, verbosity } = args;
    initialize_logger(verbosity);
    handle_signals().await?;
    run_prover(listen).await?;
    Ok(())
}
