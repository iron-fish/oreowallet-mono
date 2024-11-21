use std::net::SocketAddr;

use anyhow::Result;
use clap::Parser;
use db_handler::{DBHandler, DbConfig, PgHandler};
use dotenv::dotenv;
use server::run_server;
use utils::{handle_signals, initialize_logger};

#[derive(Parser, Debug, Clone)]
pub struct Command {
    /// The ip:port server will listen on for restful api
    #[clap(long, default_value = "0.0.0.0:9093")]
    pub listen: SocketAddr,
    /// The path to db config file
    #[clap(long)]
    pub dbconfig: String,
    /// Set your logger level
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
    /// The Ironfish rpc node to connect to
    #[clap(short, long, default_value = "127.0.0.1:9092")]
    pub node: String,
    /// The scan server to connect to
    #[clap(long, default_value = "127.0.0.1:10001")]
    pub scan: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    let sk = std::env::var("SECRET_KEY").expect("SECRET_KEY not provided in env");
    let pk = std::env::var("PUBLIC_KEY").expect("PUBLIC_KEY not provided in env");
    let mut sk_u8 = [0u8; 32];
    let mut pk_u8 = [0u8; 33];
    hex::decode_to_slice(sk, &mut sk_u8).unwrap();
    hex::decode_to_slice(pk, &mut pk_u8).unwrap();
    let args = Command::parse();
    let Command {
        listen,
        dbconfig,
        verbosity,
        node,
        scan,
    } = args;
    initialize_logger(verbosity);
    handle_signals().await?;
    let db_config = DbConfig::load(dbconfig).unwrap();
    let db_handler = PgHandler::from_config(&db_config);
    run_server(listen, node, db_handler, scan, sk_u8, pk_u8).await?;
    Ok(())
}
