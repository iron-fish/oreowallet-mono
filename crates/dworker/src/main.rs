use anyhow::Result;
use clap::Parser;
use dworker::start_worker;
use rand::Rng;
use std::net::SocketAddr;
use tracing::info;
use utils::{handle_signals, initialize_logger};

#[derive(Parser, Debug)]
#[command(version, about)]
struct Cli {
    /// scheduler to connect to
    #[clap(short, long)]
    address: SocketAddr,
    /// worker name
    #[arg(short, long)]
    name: Option<String>,
    /// Set your logger level
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
}

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Cli::parse();
    initialize_logger(args.verbosity);
    handle_signals().await?;
    if args.name.is_none() {
        args.name = Some(format!(
            "dworker-{:?}-{}",
            gethostname::gethostname(),
            rand::thread_rng().gen::<u8>()
        ));
    }
    info!(
        "Start connecting to scheduler: {:?} with name {:?}",
        args.address, args.name
    );
    start_worker(args.address, args.name.unwrap()).await?;
    Ok(())
}
