use std::{
    cmp::{self, Reverse},
    net::SocketAddr,
    ops::Deref,
    str::FromStr,
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
use constants::{LOCAL_BLOCKS_CHECKPOINT, PRIMARY_BATCH, REORG_DEPTH, RESCHEDULING_DURATION};
use db_handler::{DBHandler, InnerBlock, PgHandler};
use manager::{AccountInfo, Manager, SecpKey, ServerMessage, SharedState, TaskInfo};
use networking::{
    decryption_message::{DecryptionMessage, ScanRequest, SuccessResponse},
    rpc_abi::BlockInfo,
    socket_message::codec::DRequest,
};
use tokio::{net::TcpListener, sync::oneshot, time::sleep};
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::{debug, error, info};
use utils::{blocks_range, default_secp, verify, Signature};

pub mod manager;
pub mod router;

const RESCHEDULING_TIME_LIMIT: u64 = 60; // originally was 600

pub async fn scheduling_tasks(
    scheduler: Arc<Manager>,
    accounts: &Vec<ScanRequest>,
    blocks: Vec<InnerBlock>,
) -> anyhow::Result<()> {
    for account in accounts.iter() {
        let account_info_maybe = scheduler
            .account_mappling
            .read()
            .await
            .get(&account.address)
            .cloned();
        // if let Some(account_info) = scheduler
        //     .account_mappling
        //     .read()
        //     .await
        //     .get(&account.address)
        //     .cloned()
        if let Some(account_info) = account_info_maybe {
            // info!("account_mappling HOLD");
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
            // info!("account_mappling FREE");
        }
    }
    Ok(())
}

pub async fn run_dserver(
    dlisten: SocketAddr,
    restful: SocketAddr,
    rpc_server: String,
    db_handler: PgHandler,
    server: String,
    sk_u8: [u8; 32],
    pk_u8: [u8; 33],
) -> anyhow::Result<()> {
    let secp_key = SecpKey {
        sk: sk_u8,
        pk: pk_u8,
    };
    let shared_resource = Arc::new(SharedState::new(db_handler, &rpc_server, &server, secp_key));
    let manager = Manager::new(shared_resource);

    if let Err(e) = Manager::initialize_networking(manager.clone(), dlisten).await {
        error!("init networking server {}", e);
    }

    // manager status updater
    let status_manager = manager.clone();
    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        loop {
            {
                info!(
                    "online workers: {}",
                    status_manager.workers.read().await.len()
                );
                info!(
                    "pending taskes in queue: {}",
                    status_manager.task_queue.read().await.len()
                );
                info!(
                    "pending account to scan: {:?}",
                    status_manager.accounts_to_scan.read().await
                );
            }
            sleep(Duration::from_secs(60)).await;
        }
    });
    let _ = handler.await;

    {
        info!("warmup, wait for worker to join");
        sleep(Duration::from_secs(60)).await;
    }

    // primary task scheduling
    let schduler = manager.clone();
    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        loop {
            sleep(RESCHEDULING_DURATION).await;
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
                    .get_block(latest.index.parse::<i64>().unwrap() - REORG_DEPTH)
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
                        continue;
                    }
                    let head = account.head.clone().unwrap();
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
                info!("accounts to scanning, {:?}", accounts_should_scan);
                let blocks_to_scan = blocks_range(scan_start..scan_end.sequence + 1, PRIMARY_BATCH);
                for group in blocks_to_scan {
                    let blocks = match group.end <= LOCAL_BLOCKS_CHECKPOINT {
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
                    if group.end % 20000 == 0 {
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
                .filter(|(_k, v)| v.since.elapsed().as_secs() >= RESCHEDULING_TIME_LIMIT)
                .map(|(k, _)| k.to_string())
            {
                // info!("rescheduling task: {:?}", key);
                // info!("sequence: {:?}", value.sequence);
                keys_to_reschedule.push(key.clone());
                // match secondary.task_mapping.write().await.remove(&key) {
                //     Some(task_info) => {
                //         info!("task rescheduling: Some(task_info)");
                //         let address = task_info.address.to_string();
                //         let sequence = task_info.sequence;
                //         if let Some(account) = secondary.account_mappling.read().await.get(&address)
                //         {
                //             if let Ok(block) = secondary.shared.rpc_handler.get_block(sequence) {
                //                 let block = block.data.block.to_inner();
                //                 let _ = scheduling_tasks(
                //                     secondary.clone(),
                //                     &vec![ScanRequest {
                //                         address: address.clone(),
                //                         in_vk: account.in_vk.clone(),
                //                         out_vk: account.out_vk.clone(),
                //                         head: Some(account.start_block.clone()),
                //                     }],
                //                     vec![block],
                //                 )
                //                 .await
                //                 .unwrap();
                //             }
                //         }
                //     }
                //     None => {
                //         info!("task rescheduling: None");
                //     }
                // }
            }

            let keys_count = keys_to_reschedule.len();
            if keys_count > 0 {
                info!("!!\nrescheduling up to {:?} tasks\n!!", keys_count);
            }

            let mut tasks_to_resechedule = vec![];

            let mut has_tried = false;
            for key in keys_to_reschedule {
                if !has_tried {
                    info!("try openning task_mapping...");
                }
                let key_maybe = secondary.task_mapping.write().await.remove(&key).clone();
                if let Some(task_info) = key_maybe {
                    if !has_tried {
                        info!("opened task_mapping");
                    }
                    let address = task_info.address.to_string();
                    let sequence = task_info.sequence;

                    if !has_tried {
                        info!("try opening account_mappling");
                    }
                    let address_maybe = secondary
                        .account_mappling
                        .read()
                        .await
                        .get(&address)
                        .cloned();
                    // if let Some(account) = secondary.account_mappling.read().await.get(&address) {
                    if let Some(account) = address_maybe {
                        if !has_tried {
                            info!("opened account_mappling");
                        }
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
                                    "tasks to reschedule len now {:?}",
                                    tasks_to_resechedule.len()
                                );
                            }
                            if !has_tried {
                                info!("pushed...");
                            }
                            // We dont want to reschedule so many tasks at once
                            if tasks_to_resechedule.len() >= 20000 {
                                break;
                            }
                            // let _ = scheduling_tasks(
                            //     secondary.clone(),
                            //     &vec![ScanRequest {
                            //         address: address.clone(),
                            //         in_vk: account.in_vk.clone(),
                            //         out_vk: account.out_vk.clone(),
                            //         head: Some(account.start_block.clone()),
                            //     }],
                            //     vec![block],
                            // )
                            // .await
                            // .unwrap();
                        }
                    }
                }
                if !has_tried {
                    info!("done for iteration of key_to_reschedule")
                }
                has_tried = true;
            }

            if !tasks_to_resechedule.is_empty() {
                info!(
                    "done prepping tasks to be rescheduled.. now rescheduling {:?}",
                    tasks_to_resechedule.len()
                );
            }

            let mut count = 0;
            for (scan_request, blocks) in tasks_to_resechedule {
                scheduling_tasks(secondary.clone(), &scan_request, blocks)
                    .await
                    .unwrap();
                if count % 1000 == 0 {
                    info!("rescheduled tasks so far {:?}", count);
                }
                count += 1;
            }

            if keys_count > 0 {
                info!("done rescheduling {:?} tasks", keys_count);
            }
            sleep(RESCHEDULING_DURATION).await;
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
    let secp = default_secp();
    let msg = bincode::serialize(&message).unwrap();
    let signature = Signature::from_str(&signature).unwrap();
    if let Ok(x) = verify(
        &secp,
        &msg[..],
        signature.serialize_compact(),
        &manager.shared.secp_key.pk,
    ) {
        if x {
            let _ = manager.accounts_to_scan.write().await.push(message);
            return Json(SuccessResponse { success: true });
        }
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
