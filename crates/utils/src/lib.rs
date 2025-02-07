use std::{cmp, ops::Range, time::Duration};

mod cli;
mod signer;
pub use clap::Parser;
pub use cli::*;
pub use signer::*;

use tokio::sync::oneshot;
use tracing::{info, warn};
pub use tracing_subscriber::EnvFilter;

pub async fn handle_signals() -> anyhow::Result<()> {
    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Shutdowning...");
                tokio::time::sleep(Duration::from_millis(5000)).await;
                info!("Goodbye");
                std::process::exit(0);
            }
            Err(error) => warn!("tokio::signal::ctrl_c encountered an error: {}", error),
        }
    });
    let _ = handler.await;
    info!("Signal handler installed");
    Ok(())
}

pub fn initialize_logger(verbosity: u8) {
    match verbosity {
        0 => std::env::set_var("RUST_LOG", "info"),
        1 => std::env::set_var("RUST_LOG", "debug"),
        2 | 3 | 4 => std::env::set_var("RUST_LOG", "trace"),
        _ => std::env::set_var("RUST_LOG", "info"),
    };
}

pub fn initialize_logger_filter(filter: EnvFilter) {
    tracing_subscriber::fmt().with_env_filter(filter).init();
}

pub fn blocks_range(blocks: Range<u64>, batch: u64) -> Vec<Range<u64>> {
    let end = blocks.end;
    let mut result = vec![];
    for block in blocks.step_by(batch as usize) {
        let start = block;
        let end = cmp::min(start + batch - 1, end);
        result.push(start..end)
    }
    result
}
