use core::task;
use std::{
    cmp::Reverse,
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::Result;
use constants::MAINNET_GENESIS_HASH;
use db_handler::{DBHandler, PgHandler};
use futures::{SinkExt, StreamExt};
use networking::{
    decryption_message::{DecryptionMessage, ScanRequest},
    rpc_abi::{BlockIdentifier, BlockInfo, BlockWithHash, RpcSetAccountHeadRequest, TransactionWithHash},
    rpc_handler::RpcHandler,
    server_handler::ServerHandler,
    socket_message::codec::{DMessage, DMessageCodec, DRequest, DResponse},
};
use priority_queue::PriorityQueue;
use serde::{Deserialize, Serialize};
use tokio::{
    io::split,
    net::TcpStream,
    sync::{
        mpsc::{self, Sender},
        oneshot, RwLock,
    },
    time::timeout,
};
use tokio_util::codec::{FramedRead, FramedWrite};
use tracing::{debug, error, info, warn};
use utils::{default_secp, sign};

#[derive(Clone, Serialize, Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ServerMessage {
    pub name: Option<String>,
    pub request: DRequest,
}

#[derive(Debug, Clone)]
pub struct ServerWorker {
    pub router: Sender<ServerMessage>,
    // 1: Idle; 2: Busy
    pub status: u8,
}

impl ServerWorker {
    pub fn new(router: Sender<ServerMessage>) -> Self {
        Self { router, status: 1 }
    }
}

#[derive(Debug, Clone)]
pub struct TaskInfo {
    pub since: Instant,
    pub sequence: i64,
    pub hash: String,
    pub address: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountInfo {
    pub start_block: BlockInfo,
    pub end_block: BlockInfo,
    pub remaining_task: u64,
    pub in_vk: String,
    pub out_vk: String,
    // mapping from block_hash to transaction list in this block
    pub blocks: HashMap<String, Vec<TransactionWithHash>>,
}

impl AccountInfo {
    pub fn new(
        start_block: BlockInfo,
        end_block: BlockInfo,
        in_vk: String,
        out_vk: String,
    ) -> Self {
        let remaining_task = end_block.sequence - start_block.sequence + 1;
        Self {
            start_block,
            end_block,
            remaining_task,
            in_vk,
            out_vk,
            blocks: HashMap::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct SecpKey {
    pub sk: [u8; 32],
    pub pk: [u8; 33],
}

#[derive(Debug, Clone)]
pub struct SharedState<T: DBHandler> {
    pub db_handler: T,
    pub rpc_handler: RpcHandler,
    pub server_handler: ServerHandler,
    pub secp_key: SecpKey,
}

impl<T> SharedState<T>
where
    T: DBHandler,
{
    pub fn new(db_handler: T, endpoint: &str, server: &str, secp_key: SecpKey) -> Self {
        Self {
            db_handler: db_handler,
            rpc_handler: RpcHandler::new(endpoint.into()),
            server_handler: ServerHandler::new(server.into()),
            secp_key,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Manager {
    pub workers: Arc<RwLock<HashMap<String, ServerWorker>>>,
    pub task_queue: Arc<RwLock<PriorityQueue<DRequest, Reverse<i64>>>>,
    pub task_mapping: Arc<RwLock<HashMap<String, TaskInfo>>>,
    pub account_mappling: Arc<RwLock<HashMap<String, AccountInfo>>>,
    pub shared: Arc<SharedState<PgHandler>>,
    pub accounts_to_scan: Arc<RwLock<Vec<ScanRequest>>>,
}

impl Manager {
    pub fn new(shared: Arc<SharedState<PgHandler>>) -> Arc<Self> {
        Arc::new(Self {
            workers: Arc::new(RwLock::new(HashMap::new())),
            task_queue: Arc::new(RwLock::new(PriorityQueue::new())),
            task_mapping: Arc::new(RwLock::new(HashMap::new())),
            account_mappling: Arc::new(RwLock::new(HashMap::new())),
            shared,
            accounts_to_scan: Arc::new(RwLock::new(vec![])),
        })
    }

    pub async fn handle_stream(stream: TcpStream, server: Arc<Self>) -> Result<()> {
        let (tx, mut rx) = mpsc::channel::<ServerMessage>(1024);
        let mut worker_name = stream.peer_addr().unwrap().clone().to_string();
        let (r, w) = split(stream);
        let mut outbound_w = FramedWrite::new(w, DMessageCodec::default());
        let mut outbound_r = FramedRead::new(r, DMessageCodec::default());
        let (router, handler) = oneshot::channel();
        let mut timer = tokio::time::interval(Duration::from_secs(300));
        let _ = timer.tick().await;

        let worker_server = server.clone();

        let worker_server_clone = worker_server.clone();
        let _out_message_handler = tokio::spawn(async move {
            while let Some(message) = rx.recv().await {
                let ServerMessage { name, request } = message;
                match name {
                    Some(name) => {
                        let _ = worker_server_clone
                            .workers
                            .write()
                            .await
                            .get_mut(&name)
                            .unwrap()
                            .status = 2;
                    }
                    None => {}
                }
                let send_future = outbound_w.send(DMessage::DRequest(request));
                if let Err(error) = timeout(Duration::from_secs(3), send_future).await {
                    error!("send message to worker timeout: {}", error);
                }
            }
        });

        let _in_message_handler = tokio::spawn(async move {
            let _ = router.send(());
            loop {
                tokio::select! {
                    _ = timer.tick() => {
                        debug!("no message from worker {} for 5 mins, exit", worker_name);
                        let _ = worker_server.workers.write().await.remove(&worker_name).unwrap();
                        break;
                    },
                    result = outbound_r.next() => {
                        debug!("new message from outboud_reader {:?} of worker {}", result, worker_name);
                        match result {
                            Some(Ok(message)) => {
                                timer.reset();
                                match message {
                                    DMessage::RegisterWorker(register) => {
                                        debug!("heart beat info {:?}", register);
                                        match worker_name == register.name {
                                            true => {},
                                            false => {
                                                let worker = ServerWorker::new(tx.clone());
                                                worker_name = register.name;
                                                info!("new worker: {}", worker_name.clone());
                                                let _ = worker_server.workers.write().await.insert(worker_name.clone(), worker);
                                                match worker_server.task_queue.write().await.pop() {
                                                    Some((task, _)) => {
                                                        let _ = tx.send(ServerMessage { name: Some(worker_name.clone()), request: task }).await.unwrap();
                                                    },
                                                    None => {},
                                                }
                                            }
                                        }
                                    },
                                    DMessage::DRequest(_) => error!("invalid message from worker, should never happen"),
                                    DMessage::DResponse(response) => {
                                        debug!("new response from worker {}", response.id);
                                        match worker_server.task_queue.write().await.pop() {
                                            Some((task, _)) => {
                                                let _ = tx.send(ServerMessage { name: None, request: task }).await.unwrap();
                                            },
                                            None => worker_server.workers.write().await.get_mut(&worker_name).unwrap().status = 1,
                                        }
                                        let _ = worker_server.update_account(response).await;
                                    },
                                }
                            },
                            _ => {
                                warn!("unknown message");
                                let _ = worker_server.workers.write().await.remove(&worker_name).unwrap();
                                break;
                            },
                        }
                    }
                }
            }
            error!("worker {} main loop exit", worker_name);
        });
        let _ = handler.await;
        Ok(())
    }

    pub async fn update_account(&self, response: DResponse) -> Result<()> {
        let address = response.address.clone();
        let task_id = response.id.clone();
        let mut update_scan_head = false;
        let mut latest_block = BlockInfo {
            hash: MAINNET_GENESIS_HASH.to_string(),
            sequence: 0,
        };
        match self.account_mappling.write().await.get_mut(&address) {
            Some(account) => {
                if let Some(task_info) = self.task_mapping.read().await.get(&task_id) {
                    let block_hash = task_info.hash.to_string();
                    if !response.data.is_empty() {
                        debug!("account info: {:?}", account);
                        info!("new available block {} for account {}", block_hash, address);
                        account.blocks.insert(
                            block_hash,
                            response
                                .data
                                .into_iter()
                                .map(|hash| TransactionWithHash { hash })
                                .collect(),
                        );
                    }
                    account.remaining_task -= 1;
                    if account.remaining_task % 5000 == 0 || account.remaining_task == 0 {
                        update_scan_head = true;
                    }
                    if task_info.sequence > latest_block.sequence as i64 {
                        latest_block = BlockInfo {
                            hash: task_info.hash.clone(),
                            sequence: task_info.sequence as u64,
                        };
                    }
                }
            }
            None => {
                error!("bad response whose request account doesn't exist, should never happen")
            }
        }
        if update_scan_head {
            let account_info = self
                .account_mappling
                .read()
                .await
                .get(&address)
                .unwrap()
                .clone();
            let set_account_head_request = RpcSetAccountHeadRequest {
                account: address.clone(),
                start: account_info.start_block.hash.to_string(),
                end: account_info.end_block.hash.to_string(),
                blocks: account_info
                    .blocks
                    .iter()
                    .map(|(k, v)| BlockWithHash {
                        hash: k.to_string(),
                        transactions: v.clone(),
                    })
                    .collect(),
                latest: BlockIdentifier {
                    hash: latest_block.hash,
                    index: latest_block.sequence.to_string(),
                }
            };
            let msg = bincode::serialize(&set_account_head_request).unwrap();
            let secp = default_secp();
            let signature = sign(&secp, &msg[..], &self.shared.secp_key.sk)
                .unwrap()
                .to_string();
            let request = DecryptionMessage {
                message: set_account_head_request,
                signature,
            };
            match self.shared.server_handler.submit_scan_response(request) {
                Ok(msg) => {
                    if msg.success {
                        let _ = self.account_mappling.write().await.remove(&address);
                    }
                }
                Err(e) => error!("failed to submit decryption response to server, {:?}", e),
            }
        }
        let _ = self.task_mapping.write().await.remove(&task_id);
        Ok(())
    }
}
