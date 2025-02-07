use anyhow::Result;
use dworker::start_worker;
use rand::Rng;
use tracing::info;
use utils::{
    handle_signals, initialize_logger, initialize_logger_filter, EnvFilter, Parser, Worker,
};

#[tokio::main]
async fn main() -> Result<()> {
    let mut args = Worker::parse();
    initialize_logger(args.verbosity);
    initialize_logger_filter(EnvFilter::from_default_env());
    handle_signals().await?;
    if args.name.is_none() {
        args.name = Some(
            format!(
                "dworker-{:?}-{}",
                gethostname::gethostname(),
                rand::thread_rng().gen::<u8>()
            )
            .into(),
        );
    }
    info!(
        "Start connecting to scheduler: {:?} with name {:?}",
        args.address, args.name
    );
    start_worker(args.address, args.name.unwrap()).await?;
    Ok(())
}
