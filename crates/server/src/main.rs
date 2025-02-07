use anyhow::Result;
use db_handler::load_db;
use params::{mainnet::Mainnet, network::Network, testnet::Testnet};
use server::run_server;
use utils::{
    handle_signals, initialize_logger, initialize_logger_filter, EnvFilter, Parser, Server,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Server::parse();
    let Server {
        listen,
        dbconfig,
        node,
        scanner,
        network,
        operator,
        verbosity,
    } = args;
    initialize_logger(verbosity);
    let filter = EnvFilter::from_default_env()
        .add_directive("ureq=off".parse().unwrap())
        .add_directive("rustls=off".parse().unwrap());
    initialize_logger_filter(filter);
    handle_signals().await?;
    let db_handler = load_db(dbconfig).unwrap();
    match network {
        Mainnet::ID => {
            run_server::<Mainnet>(listen.into(), node, db_handler, scanner, operator).await?;
        }
        Testnet::ID => {
            run_server::<Testnet>(listen.into(), node, db_handler, scanner, operator).await?;
        }
        _ => panic!("Invalid network used"),
    }
    Ok(())
}
