use std::sync::Arc;

use axum::{
    extract::{self, State},
    response::IntoResponse,
    Json,
};
use networking::{
    decryption_message::{DecryptionMessage, ScanRequest, ScanResponse, SuccessResponse},
    rpc_abi::{
        BlockInfo, CreatedAt, OutPut, RpcAddTxRequest, RpcCreateTxRequest,
        RpcGetAccountStatusRequest, RpcGetAccountTransactionRequest, RpcGetBalancesRequest,
        RpcGetBalancesResponse, RpcGetTransactionsRequest, RpcImportAccountRequest,
        RpcImportAccountResponse, RpcRemoveAccountRequest, RpcResetAccountRequest, RpcResponse,
        RpcSetScanningRequest,
    },
    web_abi::{GetTransactionDetailResponse, ImportAccountRequest, RescanAccountResponse},
};
use oreo_errors::OreoError;
use params::{mainnet::Mainnet, network::Network, testnet::Testnet};
use serde_json::json;

use crate::SharedState;

pub async fn import_account_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(import): extract::Json<ImportAccountRequest>,
) -> impl IntoResponse {
    let genesis = shared.genesis().clone();
    let account_name = shared
        .db_handler
        .save_account(import.clone().to_account(genesis.clone()), 0)
        .await;
    if let Err(e) = account_name {
        return e.into_response();
    }
    let account_name = account_name.unwrap();
    let ImportAccountRequest {
        view_key,
        incoming_view_key,
        outgoing_view_key,
        public_address,
        created_at,
    } = import;

    let created_at = created_at.map(|created_at| CreatedAt {
        hash: created_at.hash,
        sequence: created_at.sequence,
        network_id: shared.network(),
    });
    let rpc_data = RpcImportAccountRequest {
        view_key,
        incoming_view_key: incoming_view_key.clone(),
        outgoing_view_key: outgoing_view_key.clone(),
        public_address: public_address.clone(),
        spending_key: None,
        version: shared.account_version(),
        name: account_name.clone(),
        created_at,
    };
    match shared.rpc_handler.import_account(rpc_data) {
        Ok(_) => {
            let latest = shared.rpc_handler.get_latest_block().unwrap();
            let latest_height = latest
                .data
                .current_block_identifier
                .index
                .parse::<u64>()
                .unwrap();
            let result = shared
                .rpc_handler
                .get_account_status(RpcGetAccountStatusRequest {
                    account: account_name.clone(),
                })
                .map(|x| {
                    let head = x.data.account.head.unwrap_or(BlockInfo {
                        hash: genesis.hash.clone(),
                        sequence: genesis.sequence,
                    });
                    if latest_height - head.sequence > 1000 {
                        shared.rpc_handler.set_scanning(RpcSetScanningRequest {
                            account: account_name.clone(),
                            enabled: false,
                        })?;
                        shared.rpc_handler.reset_account(RpcResetAccountRequest {
                            account: account_name.clone(),
                            reset_scanning_enabled: Some(false),
                            reset_created_at: Some(false),
                        })?;
                        let scan_request = ScanRequest {
                            address: public_address.clone(),
                            in_vk: incoming_view_key.clone(),
                            out_vk: outgoing_view_key.clone(),
                            head: Some(head),
                        };
                        let signature = shared
                            .operator
                            .sign(&scan_request)
                            .unwrap_or("default_but_bad_signature, should never happen".into());
                        shared.scan_handler.submit_scan_request(DecryptionMessage {
                            message: scan_request,
                            signature,
                        })?;
                    }
                    Ok::<RpcImportAccountResponse, OreoError>(RpcImportAccountResponse {
                        name: account_name.clone(),
                    })
                });
            match result {
                Ok(response) => RpcResponse {
                    status: 200,
                    data: response,
                }
                .into_response(),

                Err(e) => e.into_response(),
            }
        }
        Err(e) => e.into_response(),
    }
}

pub async fn remove_account_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(remove_account): extract::Json<RpcRemoveAccountRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(remove_account.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let result = shared.rpc_handler.remove_account(RpcRemoveAccountRequest {
        account: db_account.unwrap().name,
        confirm: Some(true),
        wait: Some(true),
    });
    match result {
        Ok(response) => {
            if let Err(e) = shared
                .db_handler
                .remove_account(remove_account.account.clone())
                .await
            {
                return e.into_response();
            }
            response.into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn account_status_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(account): extract::Json<RpcGetAccountStatusRequest>,
) -> impl IntoResponse {
    let db_account = shared.db_handler.get_account(account.account.clone()).await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let result = shared
        .rpc_handler
        .get_account_status(RpcGetAccountStatusRequest {
            account: db_account.unwrap().name,
        });
    let genesis = shared.genesis().clone();
    match result {
        Ok(mut result) => {
            match result.data.account.head {
                Some(_) => {}
                None => {
                    result.data.account.head = Some(BlockInfo {
                        hash: genesis.hash.clone(),
                        sequence: genesis.sequence,
                    })
                }
            }
            Ok(result)
        }
        Err(e) => Err(e),
    }
    .into_response()
}

async fn rescan_account(
    shared: Arc<SharedState>,
    account: RpcGetAccountStatusRequest,
) -> Result<RescanAccountResponse, OreoError> {
    let account = shared
        .db_handler
        .get_account(account.account.clone())
        .await?;
    shared.rpc_handler.set_scanning(RpcSetScanningRequest {
        account: account.name.clone(),
        enabled: false,
    })?;
    shared.rpc_handler.reset_account(RpcResetAccountRequest {
        account: account.name.clone(),
        reset_scanning_enabled: Some(false),
        reset_created_at: Some(false),
    })?;
    let _ = shared
        .db_handler
        .update_scan_status(account.address.clone(), true)
        .await?;
    let status = shared
        .rpc_handler
        .get_account_status(RpcGetAccountStatusRequest {
            account: account.name.clone(),
        })?;
    let genesis = shared.genesis();
    let head = status.data.account.head.unwrap_or(BlockInfo {
        hash: genesis.hash.clone(),
        sequence: genesis.sequence,
    });
    let scan_request = ScanRequest {
        address: account.address.clone(),
        in_vk: account.in_vk.clone(),
        out_vk: account.out_vk.clone(),
        head: Some(head),
    };
    let signature = shared
        .operator
        .sign(&scan_request)
        .unwrap_or("default_but_bad_signature, should never happen".into());
    shared.scan_handler.submit_scan_request(DecryptionMessage {
        message: scan_request,
        signature,
    })?;
    Ok(RescanAccountResponse { success: true })
}

pub async fn rescan_account_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(account): extract::Json<RpcGetAccountStatusRequest>,
) -> impl IntoResponse {
    match rescan_account(shared, account).await {
        Ok(response) => RpcResponse {
            status: 200,
            data: response,
        }
        .into_response(),
        Err(err) => err.into_response(),
    }
}

async fn update_scan_status(
    shared: Arc<SharedState>,
    response: DecryptionMessage<ScanResponse>,
) -> Result<SuccessResponse, OreoError> {
    let DecryptionMessage {
        mut message,
        signature,
    } = response;
    if let Ok(true) = shared.operator.verify(&message, signature) {
        let account = shared
            .db_handler
            .get_account(message.account.clone())
            .await?;
        let batch_size = shared.set_account_limit();
        let scan_complete = message.scan_complete;
        let mut first_request = true;
        message.account = account.name.clone();
        let mut blocks = message.blocks.clone();
        blocks.sort_by(|a, b| b.sequence.cmp(&a.sequence));
        let mut start_hash = String::new();
        let mut end_hash;
        loop {
            let mut message = message.clone();
            let mut limited_blocks = Vec::with_capacity(batch_size);
            while let Some(block) = blocks.pop() {
                limited_blocks.push(block);
                if limited_blocks.len() >= batch_size {
                    break;
                }
            }
            if !first_request && limited_blocks.is_empty() {
                break;
            }
            if !first_request {
                message.start = start_hash;
            }
            if !limited_blocks.is_empty() && !blocks.is_empty() {
                end_hash = limited_blocks.last().unwrap().hash.clone();
            } else {
                end_hash = message.end.clone();
            }

            message.end = end_hash.clone();
            message.blocks = limited_blocks;
            shared.rpc_handler.set_account_head(message)?;
            {
                first_request = false;
                start_hash = end_hash;
            }
        }
        if scan_complete {
            let _ = shared.rpc_handler.set_scanning(RpcSetScanningRequest {
                account: account.name.clone(),
                enabled: true,
            })?;
            shared
                .db_handler
                .update_scan_status(account.address, false)
                .await?;
        }
        Ok(SuccessResponse { success: true })
    } else {
        Err(OreoError::BadSignature)
    }
}

pub async fn update_scan_status_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(response): extract::Json<DecryptionMessage<ScanResponse>>,
) -> impl IntoResponse {
    match update_scan_status(shared, response).await {
        Ok(response) => Json(response).into_response(),
        Err(err) => err.into_response(),
    }
}

pub async fn get_balances_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(get_balance): extract::Json<RpcGetBalancesRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(get_balance.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let resp = shared.rpc_handler.get_balances(RpcGetBalancesRequest {
        account: db_account.unwrap().name,
        confirmations: Some(get_balance.confirmations.unwrap_or(10)),
    });
    match resp {
        Ok(mut res) => {
            for item in res.data.balances.iter_mut() {
                if let Ok(asset) = shared.rpc_handler.get_asset(item.asset_id.clone()) {
                    item.decimals = asset.data.verification.decimals;
                }
            }
            let data = match shared.network() {
                Testnet::ID => RpcGetBalancesResponse::verified_asset::<Testnet>(res.data),
                _ => RpcGetBalancesResponse::verified_asset::<Mainnet>(res.data),
            };
            let response = RpcResponse { status: 200, data };
            response.into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_ores_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(get_balance): extract::Json<RpcGetBalancesRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(get_balance.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let resp = shared.rpc_handler.get_balances(RpcGetBalancesRequest {
        account: db_account.unwrap().name,
        confirmations: Some(get_balance.confirmations.unwrap_or(10)),
    });
    match resp {
        Ok(res) => {
            let data = match shared.network() {
                Testnet::ID => RpcGetBalancesResponse::ores::<Testnet>(res.data).await,
                _ => RpcGetBalancesResponse::ores::<Mainnet>(res.data).await,
            };
            let response = RpcResponse { status: 200, data };
            response.into_response()
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_transaction_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(account): extract::Json<RpcGetAccountTransactionRequest>,
) -> impl IntoResponse {
    let db_account = shared.db_handler.get_account(account.account.clone()).await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let rpc_transaction =
        shared
            .rpc_handler
            .get_account_transaction(RpcGetAccountTransactionRequest {
                account: db_account.unwrap().name,
                hash: account.hash,
                notes: Some(true),
            });
    match rpc_transaction {
        Ok(RpcResponse { data, status: _ }) => {
            let transaction_detail = GetTransactionDetailResponse::from_rpc_data(data);
            match transaction_detail {
                Ok(detail) => RpcResponse {
                    status: 200,
                    data: detail,
                }
                .into_response(),
                Err(e) => e.into_response(),
            }
        }
        Err(e) => e.into_response(),
    }
}

pub async fn get_transactions_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(get_transactions): extract::Json<RpcGetTransactionsRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(get_transactions.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    shared
        .rpc_handler
        .get_transactions(RpcGetTransactionsRequest {
            account: db_account.unwrap().name,
            limit: Some(get_transactions.limit.unwrap_or(6)),
            reverse: Some(true),
        })
        .into_response()
}

pub async fn create_transaction_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(create_transaction): extract::Json<RpcCreateTxRequest>,
) -> impl IntoResponse {
    let db_account = shared
        .db_handler
        .get_account(create_transaction.account.clone())
        .await;
    if let Err(e) = db_account {
        return e.into_response();
    }
    let outputs: Vec<OutPut> = create_transaction
        .outputs
        .unwrap_or(vec![])
        .iter()
        .map(|output| match shared.network() {
            Testnet::ID => OutPut::from::<Testnet>(output.clone()),
            _ => OutPut::from::<Mainnet>(output.clone()),
        })
        .collect();
    let mut mints = vec![];
    for item in create_transaction.mints.unwrap_or(vec![]).into_iter() {
        if item.asset_id.is_none() && item.name.is_none() {
            return OreoError::BadMintRequest.into_response();
        } else {
            mints.push(item);
        }
    }
    let burns = create_transaction.burns.unwrap_or(vec![]);
    shared
        .rpc_handler
        .create_transaction(RpcCreateTxRequest {
            account: db_account.unwrap().name,
            outputs: Some(outputs),
            fee: Some(create_transaction.fee.unwrap_or("1".into())),
            expiration_delta: Some(create_transaction.expiration_delta.unwrap_or(30)),
            mints: Some(mints),
            burns: Some(burns),
        })
        .into_response()
}

pub async fn add_transaction_handler(
    State(shared): State<Arc<SharedState>>,
    extract::Json(broadcast_transaction): extract::Json<RpcAddTxRequest>,
) -> impl IntoResponse {
    shared
        .rpc_handler
        .add_transaction(broadcast_transaction)
        .into_response()
}

pub async fn latest_block_handler(State(shared): State<Arc<SharedState>>) -> impl IntoResponse {
    shared.rpc_handler.get_latest_block().into_response()
}

pub async fn health_check_handler() -> impl IntoResponse {
    Json(json!({"code": 200, "data": "Hello prover!"})).into_response()
}
