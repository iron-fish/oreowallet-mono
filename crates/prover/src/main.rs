use anyhow::Result;
use prover::run_prover;
use tracing::info;
use utils::{initialize_logger, initialize_logger_filter, EnvFilter, Parser, Prover};

#[tokio::main]
async fn main() -> Result<()> {
    let prover = Prover::parse();
    initialize_logger(prover.verbosity);
    let filter = EnvFilter::from_default_env().add_directive("bellperson=off".parse().unwrap());
    initialize_logger_filter(filter);
    info!("Prover starts {:?}", prover);
    run_prover(prover.listen).await?;
    Ok(())
}
