use std::{
    cmp::{self, Reverse},
    net::SocketAddr,
    ops::Deref,
    sync::Arc,
    time::{Duration, Instant},
};

use axum::{
    error_handling::HandleErrorLayer,
    extract::{self, State},
    http::StatusCode,
    response::IntoResponse,
    routing::post,
    BoxError, Json, Router,
};
use db_handler::{DBHandler, InnerBlock};
use manager::{AccountInfo, Manager, ServerMessage, SharedState, TaskInfo};
use networking::{
    decryption_message::{DecryptionMessage, ScanRequest, SuccessResponse},
    rpc_abi::BlockInfo,
    socket_message::codec::DRequest,
};
use params::network::Network;
use tokio::{net::TcpListener, sync::oneshot, time::sleep};
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, error, info, warn};
use utils::blocks_range;

pub mod manager;
pub mod router;

pub async fn scheduling_tasks(
    scheduler: Arc<Manager>,
    accounts: &Vec<ScanRequest>,
    blocks: Vec<InnerBlock>,
) -> anyhow::Result<()> {
    for account in accounts {
        let account_info_maybe = scheduler
            .account_mappling
            .read()
            .await
            .get(&account.address)
            .cloned();
        if let Some(account_info) = account_info_maybe {
            debug!(
                "start scanning {} blocks for account {:?}",
                blocks.len(),
                account.address.clone()
            );
            for block in blocks.iter() {
                if (block.sequence as u64) < account_info.start_block.sequence
                    || (block.sequence as u64) > account_info.end_block.sequence
                {
                    debug!("skip height {:?}", block.sequence);
                    continue;
                }
                let task = DRequest::from_transactions(account, block.transactions.deref().clone());
                let task_id = task.id.clone();
                let _ = scheduler.task_mapping.write().await.insert(
                    task_id,
                    TaskInfo {
                        since: Instant::now(),
                        sequence: block.sequence,
                        hash: block.hash.clone(),
                        address: account.address.clone(),
                    },
                );
                let mut task_sent = false;
                for (k, worker) in scheduler.workers.read().await.iter() {
                    if worker.status == 1 {
                        match worker
                            .router
                            .send(ServerMessage {
                                name: Some(k.to_string()),
                                request: task.clone(),
                            })
                            .await
                        {
                            Ok(_) => {
                                task_sent = true;
                                break;
                            }
                            Err(e) => {
                                error!("failed to send message to worker, {:?}", e);
                            }
                        }
                    }
                }
                if task_sent {
                    continue;
                }
                let _ = scheduler
                    .task_queue
                    .write()
                    .await
                    .push(task, Reverse(block.sequence));
            }
        }
    }
    Ok(())
}

pub async fn run_dserver<N: Network>(
    dlisten: SocketAddr,
    restful: SocketAddr,
    rpc_server: String,
    db_handler: Box<dyn Send + Sync + DBHandler>,
    server: String,
    operator: String,
) -> anyhow::Result<()> {
    let shared_resource = Arc::new(SharedState::new(db_handler, &rpc_server, &server, operator));
    let manager = Manager::new(shared_resource, N::ID);

    if let Err(e) = Manager::initialize_networking(manager.clone(), dlisten).await {
        error!("Init networking server failed {}", e);
    }

    if let Err(e) = Manager::initialize_status_updater(manager.clone()).await {
        error!("Init status updater failed {}", e);
    }

    {
        info!("Warmup, waiting for workers to join");
        sleep(Duration::from_secs(60)).await;
    }

    // primary task scheduling
    let schduler = manager.clone();
    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        loop {
            sleep(N::RESCHEDULING_DURATION).await;
            if !schduler.accounts_to_scan.read().await.is_empty() {
                if !schduler.account_mappling.read().await.is_empty() {
                    continue;
                }
                let mut accounts_should_scan = vec![];
                let mut scan_start = u64::MAX;
                let latest = schduler
                    .shared
                    .rpc_handler
                    .get_latest_block()
                    .unwrap()
                    .data
                    .current_block_identifier;
                let scan_end = schduler
                    .shared
                    .rpc_handler
                    .get_block(latest.index.parse::<i64>().unwrap() - N::REORG_DEPTH)
                    .unwrap()
                    .data
                    .block;
                let scan_end = BlockInfo {
                    sequence: scan_end.sequence as u64,
                    hash: scan_end.hash,
                };
                while let Some(account) = schduler.accounts_to_scan.write().await.pop() {
                    if schduler
                        .account_mappling
                        .read()
                        .await
                        .get(&account.address)
                        .is_some()
                    {
                        // should never happen
                        warn!("Unexpected duplicated account to scan {}", account.address);
                        continue;
                    }
                    let head = {
                        let head = account.head.clone().unwrap();
                        match head.sequence >= scan_end.sequence {
                            true => scan_end.clone(),
                            false => head,
                        }
                    };
                    let _ = schduler.account_mappling.write().await.insert(
                        account.address.clone(),
                        AccountInfo::new(
                            head.clone(),
                            scan_end.clone(),
                            account.in_vk.clone(),
                            account.out_vk.clone(),
                        ),
                    );
                    scan_start = cmp::min(scan_start, head.sequence);
                    accounts_should_scan.push(account);
                }
                if accounts_should_scan.is_empty() {
                    continue;
                }
                info!("accounts to scan, {:?}", accounts_should_scan);
                let blocks_to_scan =
                    blocks_range(scan_start..scan_end.sequence + 1, N::PRIMARY_BATCH);
                for group in blocks_to_scan {
                    let blocks = match group.end <= N::LOCAL_BLOCKS_CHECKPOINT {
                        true => schduler
                            .shared
                            .db_handler
                            .get_blocks(group.start as i64, group.end as i64)
                            .await
                            .unwrap(),
                        false => {
                            let items = schduler
                                .shared
                                .rpc_handler
                                .get_blocks(group.start, group.end)
                                .unwrap()
                                .data
                                .blocks;
                            items
                                .into_iter()
                                .map(|item| item.block.to_inner())
                                .collect()
                        }
                    };

                    let _ = scheduling_tasks(schduler.clone(), &accounts_should_scan, blocks)
                        .await
                        .unwrap();
                    // avoid too much memory usage
                    if group.end % 30000 == 0 {
                        sleep(Duration::from_secs(1)).await;
                    }
                }
            }
        }
    });
    let _ = handler.await;

    // secondary task scheduling
    let secondary = manager.clone();
    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        loop {
            let mut keys_to_reschedule = vec![];
            for key in secondary
                .task_mapping
                .read()
                .await
                .iter()
                .filter(|(_k, v)| v.since.elapsed().as_secs() >= 600)
                .map(|(k, _)| k.to_string())
            {
                keys_to_reschedule.push(key);
            }
            let keys_count = keys_to_reschedule.len();
            if keys_count > 0 {
                info!("Rescheduling {} tasks", keys_count);
            }
            let mut tasks_to_resechedule = vec![];
            for key in keys_to_reschedule {
                let key_maybe = secondary.task_mapping.write().await.remove(&key).clone();
                if let Some(task_info) = key_maybe {
                    let address = task_info.address;
                    let sequence = task_info.sequence;
                    let address_maybe = secondary
                        .account_mappling
                        .read()
                        .await
                        .get(&address)
                        .cloned();
                    if let Some(account) = address_maybe {
                        if let Ok(block) = secondary.shared.rpc_handler.get_block(sequence) {
                            let block = block.data.block.to_inner();
                            tasks_to_resechedule.push((
                                vec![ScanRequest {
                                    address: address.clone(),
                                    in_vk: account.in_vk.clone(),
                                    out_vk: account.out_vk.clone(),
                                    head: Some(account.start_block.clone()),
                                }],
                                vec![block],
                            ));
                            if tasks_to_resechedule.len() % 500 == 0 {
                                info!(
                                    "Tasks to reschedule len now {:?}",
                                    tasks_to_resechedule.len()
                                );
                            }
                            // We dont want to reschedule so many tasks at once
                            if tasks_to_resechedule.len() >= 20000 {
                                break;
                            }
                        }
                    } else {
                        error!("Account info missed for exist task, account {}", address);
                    }
                }
            }
            if !tasks_to_resechedule.is_empty() {
                info!(
                    "Done prepping tasks to be rescheduled.. now rescheduling {:?}",
                    tasks_to_resechedule.len()
                );
            }
            let mut count = 0;
            for (scan_request, blocks) in tasks_to_resechedule {
                scheduling_tasks(secondary.clone(), &scan_request, blocks)
                    .await
                    .unwrap();
                if count % 1000 == 0 {
                    info!("Rescheduled tasks so far {:?}", count);
                }
                count += 1;
            }
            if count > 0 {
                info!("Done rescheduling {:?} tasks", count);
            }
            sleep(N::RESCHEDULING_DURATION).await;
        }
    });
    let _ = handler.await;

    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        let _ = start_rest(manager.clone(), restful).await;
    });
    let _ = handler.await;

    std::future::pending::<()>().await;
    Ok(())
}

pub async fn start_rest(server: Arc<Manager>, restful: SocketAddr) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/scanAccount", post(account_scanner_handler))
        .with_state(server)
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_: BoxError| async {
                    StatusCode::REQUEST_TIMEOUT
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(60))),
        )
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_origin(Any)
                .allow_headers(Any),
        );
    let listener = TcpListener::bind(&restful).await?;
    info!("rest server listening on {}", &restful);
    axum::serve(listener, router).await?;
    Ok(())
}

pub async fn account_scanner_handler(
    State(manager): State<Arc<Manager>>,
    extract::Json(request): extract::Json<DecryptionMessage<ScanRequest>>,
) -> impl IntoResponse {
    info!("new scan request coming: {:?}", request);
    let DecryptionMessage { message, signature } = request;
    if let Ok(true) = manager.shared.operator.verify(&message, signature) {
        if !manager.should_skip_request(message.address.clone()).await {
            let _ = manager.accounts_to_scan.write().await.push(message);
        }
        return Json(SuccessResponse { success: true });
    }
    Json(SuccessResponse { success: false })
}

#[cfg(test)]
mod tests {
    use crate::blocks_range;

    #[test]
    fn block_range_test() {
        let range = blocks_range(1..100, 30);
        println!("ranges, {:?}", range);
    }
}
