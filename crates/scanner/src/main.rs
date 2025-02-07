use std::time::Duration;

use anyhow::Result;
use db_handler::{load_db, DBHandler, DBTransaction, InnerBlock, Json};
use networking::{rpc_abi::RpcBlock, rpc_handler::RpcHandler};
use params::{mainnet::Mainnet, network::Network, testnet::Testnet};
use scanner::run_dserver;
use tokio::time::sleep;
use tracing::{error, info};
use utils::{
    blocks_range, handle_signals, initialize_logger, initialize_logger_filter, EnvFilter, Parser,
    Scanner,
};

#[tokio::main]
async fn main() -> Result<()> {
    let args = Scanner::parse();
    let Scanner {
        dlisten,
        restful,
        dbconfig,
        node,
        server,
        network,
        operator,
        verbosity,
    } = args;
    initialize_logger(verbosity);
    initialize_logger_filter(EnvFilter::from_default_env());
    handle_signals().await?;
    let db_handler = load_db(dbconfig.clone()).unwrap();
    match network {
        Mainnet::ID => {
            let _ = load_blocks::<Mainnet>(node.clone(), &db_handler).await;
            run_dserver::<Mainnet>(
                dlisten.into(),
                restful.into(),
                node,
                db_handler,
                server,
                operator,
            )
            .await?;
        }
        Testnet::ID => {
            let _ = load_blocks::<Testnet>(node.clone(), &db_handler).await;
            run_dserver::<Testnet>(
                dlisten.into(),
                restful.into(),
                node,
                db_handler,
                server,
                operator,
            )
            .await?;
        }
        _ => panic!("Invalid network used"),
    }
    Ok(())
}

async fn load_blocks<N: Network>(
    node: String,
    db_handler: &Box<dyn DBHandler + Send + Sync>,
) -> Result<()> {
    info!("Loading database blocks...");
    let rpc_handler = RpcHandler::new(node);
    let genesis_block = rpc_handler
        .get_latest_block()
        .unwrap()
        .data
        .genesis_block_identifier;
    if genesis_block.hash.to_lowercase() != N::GENESIS_BLOCK_HASH.to_lowercase() {
        panic!("Genesis block doesn't match");
    }

    for group in blocks_range(1..N::LOCAL_BLOCKS_CHECKPOINT + 1, N::PRIMARY_BATCH) {
        if db_handler
            .get_blocks(group.start as i64, group.end as i64)
            .await
            .is_ok()
        {
            continue;
        }
        let results = {
            loop {
                match rpc_handler.get_blocks(group.start, group.end) {
                    Ok(res) => break res,
                    Err(e) => {
                        error!("Failed to get rpc blocks {}", e);
                    }
                }
                sleep(Duration::from_secs(1)).await;
            }
        };
        let blocks: Vec<RpcBlock> = results
            .data
            .blocks
            .into_iter()
            .map(|item| item.block)
            .collect();
        let inner_blocks = blocks
            .into_iter()
            .map(|rpc| InnerBlock {
                hash: rpc.hash.clone(),
                sequence: rpc.sequence as i64,
                transactions: Json(
                    rpc.transactions
                        .into_iter()
                        .map(|tx| DBTransaction {
                            hash: tx.hash,
                            serialized_notes: tx.notes.into_iter().map(|n| n.serialized).collect(),
                        })
                        .collect(),
                ),
            })
            .collect();
        if group.end % 1000 == 0 {
            info!(
                "save blocks from {} to {} in local db",
                group.start, group.end
            );
        }
        let _ = db_handler.save_blocks(inner_blocks).await;
    }
    Ok(())
}
