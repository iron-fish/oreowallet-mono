use std::net::SocketAddr;

use clap::Parser;

#[derive(Parser, Debug, Clone)]
pub struct Server {
    /// The ip:port server will listen for incoming restful requests.
    #[clap(long, default_value = "0.0.0.0:10001")]
    pub listen: SocketAddr,
    /// Specify the path to the db config file.
    #[clap(long)]
    pub dbconfig: String,
    /// The Ironfish rpc node to connect to.
    #[clap(short, long, default_value = "127.0.0.1:9092")]
    pub node: String,
    /// The scanner service to connect to.
    #[clap(long, default_value = "127.0.0.1:9093")]
    pub scanner: String,
    /// The network to work on, 0 for testnet, 1 for mainnet.
    #[clap(long, default_value = "0")]
    pub network: u8,
    /// The operator secret key for signing messages.
    #[clap(long)]
    pub operator: String,
    /// Specify the verbosity of the server [options: 0, 1, 2].
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
}

#[derive(Parser, Debug, Clone)]
pub struct Prover {
    /// The ip:port prover will listen for incoming proof requests.
    #[clap(short, long, default_value = "0.0.0.0:10002")]
    pub listen: SocketAddr,
    /// Specify the verbosity of the prover [options: 0, 1, 2].
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
}

#[derive(Parser, Debug, Clone)]
pub struct Scanner {
    /// The ip:port scanner will listen on for worker to connect.
    #[clap(long, default_value = "0.0.0.0:10001")]
    pub dlisten: SocketAddr,
    /// The ip:port scanner will listen on for incoming scanning requests.
    #[clap(long, default_value = "0.0.0.0:20001")]
    pub restful: SocketAddr,
    /// Specify the path to the db config file.
    #[clap(long)]
    pub dbconfig: String,
    /// The Ironfish rpc node to connect to.
    #[clap(short, long, default_value = "127.0.0.1:9092")]
    pub node: String,
    /// The oreowallet server to contribute to.
    #[clap(short, long, default_value = "127.0.0.1:9093")]
    pub server: String,
    /// The network to work on, 0 for testnet, 1 for mainnet.
    #[clap(long, default_value = "0")]
    pub network: u8,
    /// The operator secret key for signing messages.
    #[clap(long)]
    pub operator: String,
    /// Specify the verbosity of the server [options: 0, 1, 2].
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
}

#[derive(Parser, Debug)]
pub struct Worker {
    /// Specify the scanner to contribute to.
    #[clap(long)]
    pub address: SocketAddr,
    /// Specify worker name to identify this worker.
    #[clap(long)]
    pub name: Option<String>,
    /// Specify the verbosity of the server [options: 0, 1, 2].
    #[clap(short, long, default_value = "0")]
    pub verbosity: u8,
}
