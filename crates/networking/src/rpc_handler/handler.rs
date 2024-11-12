use std::{fmt::Debug, time::Duration};

use oreo_errors::OreoError;
use serde::Deserialize;
use serde_json::json;
use tracing::{debug, info};
use ureq::{Agent, AgentBuilder, Error, Response};

use crate::{
    rpc_abi::{
        RpcBroadcastTxRequest, RpcBroadcastTxResponse, RpcCreateTxRequest, RpcCreateTxResponse,
        RpcExportAccountResponse, RpcGetAccountStatusRequest, RpcGetAccountStatusResponse,
        RpcGetAccountTransactionRequest, RpcGetAccountTransactionResponse, RpcGetBalancesRequest,
        RpcGetBalancesResponse, RpcGetBlockRequest, RpcGetBlockResponse, RpcGetBlocksRequest,
        RpcGetBlocksResponse, RpcGetLatestBlockResponse, RpcGetTransactionsRequest,
        RpcGetTransactionsResponse, RpcImportAccountRequest, RpcImportAccountResponse,
        RpcRemoveAccountRequest, RpcRemoveAccountResponse, RpcResetAccountRequest, RpcResponse,
        RpcSetAccountHeadRequest, RpcSetScanningRequest, SendTransactionRequest,
        SendTransactionResponse, TransactionStatus,
    },
    rpc_handler::RpcError,
    stream::RequestExt,
};

#[derive(Debug, Clone)]
pub struct RpcHandler {
    pub endpoint: String,
    pub agent: Agent,
}

impl RpcHandler {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            agent: AgentBuilder::new()
                .timeout_read(Duration::from_secs(5))
                .timeout_write(Duration::from_secs(5))
                .build(),
        }
    }

    pub fn import_account(
        &self,
        request: RpcImportAccountRequest,
    ) -> Result<RpcResponse<RpcImportAccountResponse>, OreoError> {
        let path = format!("http://{}/wallet/importAccount", self.endpoint);
        let account_str = serde_json::to_string(&request)
            .map_err(|_| OreoError::InternalRpcError("JSON serialization failed".to_string()))?;
        let resp = self
            .agent
            .clone()
            .post(&path)
            .send_json(ureq::json!({"account": account_str}));
        handle_response(resp)
    }

    pub fn export_account(
        &self,
        account: String,
    ) -> Result<RpcResponse<RpcExportAccountResponse>, OreoError> {
        let path = format!("http://{}/wallet/exportAccount", self.endpoint);
        let resp = self
            .agent
            .clone()
            .post(&path)
            .send_json(json!({"account": account, "format": "JSON".to_string()}));
        handle_response(resp)
    }

    pub fn remove_account(
        &self,
        request: RpcRemoveAccountRequest,
    ) -> Result<RpcResponse<RpcRemoveAccountResponse>, OreoError> {
        debug!("req: {:?}", request);
        let path = format!("http://{}/wallet/removeAccount", self.endpoint);
        let resp = self
            .agent
            .clone()
            .post(&path)
            .send_json(&request)
            .map(|res| match res.status() {
                200 => Response::new(200, "OK", "{\"status\":200,\"data\":{\"removed\":true}}")
                    .unwrap(),
                _ => res,
            });
        handle_response(resp)
    }

    pub fn get_account_status(
        &self,
        request: RpcGetAccountStatusRequest,
    ) -> Result<RpcResponse<RpcGetAccountStatusResponse>, OreoError> {
        let path = format!("http://{}/wallet/getAccountStatus", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn set_scanning(
        &self,
        request: RpcSetScanningRequest,
    ) -> Result<RpcResponse<Option<()>>, OreoError> {
        let path = format!("http://{}/wallet/setScanning", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn set_account_head(
        &self,
        request: RpcSetAccountHeadRequest,
    ) -> Result<RpcResponse<Option<()>>, OreoError> {
        let path = format!("http://{}/wallet/setAccountHead", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn reset_account(
        &self,
        request: RpcResetAccountRequest,
    ) -> Result<RpcResponse<Option<()>>, OreoError> {
        let path = format!("http://{}/wallet/resetAccount", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn get_balances(
        &self,
        request: RpcGetBalancesRequest,
    ) -> Result<RpcResponse<RpcGetBalancesResponse>, OreoError> {
        let path = format!("http://{}/wallet/getBalances", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn get_account_transaction(
        &self,
        request: RpcGetAccountTransactionRequest,
    ) -> Result<RpcResponse<RpcGetAccountTransactionResponse>, OreoError> {
        let path = format!("http://{}/wallet/getAccountTransaction", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn get_transactions(
        &self,
        request: RpcGetTransactionsRequest,
    ) -> Result<RpcResponse<RpcGetTransactionsResponse>, OreoError> {
        let path = format!("http://{}/wallet/getAccountTransactions", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);

        match resp {
            Ok(response) => {
                let transactions: Result<Vec<_>, OreoError> =
                    response.into_stream::<TransactionStatus>().collect();

                Ok(RpcResponse {
                    status: 200,
                    data: RpcGetTransactionsResponse {
                        transactions: transactions?,
                    },
                })
            }
            Err(e) => Err(OreoError::InternalRpcError(e.to_string())),
        }
    }

    pub fn create_transaction(
        &self,
        request: RpcCreateTxRequest,
    ) -> Result<RpcResponse<RpcCreateTxResponse>, OreoError> {
        let path = format!("http://{}/wallet/createTransaction", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn broadcast_transaction(
        &self,
        request: RpcBroadcastTxRequest,
    ) -> Result<RpcResponse<RpcBroadcastTxResponse>, OreoError> {
        let path = format!("http://{}/wallet/addTransaction", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(&request);
        handle_response(resp)
    }

    pub fn get_latest_block(&self) -> Result<RpcResponse<RpcGetLatestBlockResponse>, OreoError> {
        let path = format!("http://{}/chain/getChainInfo", self.endpoint);
        let resp = self.agent.clone().get(&path).call();
        handle_response(resp)
    }

    pub fn get_block(&self, sequence: i64) -> Result<RpcResponse<RpcGetBlockResponse>, OreoError> {
        let path = format!("http://{}/chain/getBlock", self.endpoint);
        let resp = self
            .agent
            .clone()
            .post(&path)
            .send_json(RpcGetBlockRequest {
                sequence,
                serialized: Some(true),
            });
        handle_response(resp)
    }

    pub fn get_blocks(
        &self,
        start: u64,
        end: u64,
    ) -> Result<RpcResponse<RpcGetBlocksResponse>, OreoError> {
        let path = format!("http://{}/chain/getBlocks", self.endpoint);
        let resp = self
            .agent
            .clone()
            .post(&path)
            .send_json(RpcGetBlocksRequest {
                start,
                end,
                serialized: true,
            });
        handle_response(resp)
    }

    pub fn send_transaction(
        &self,
        request: SendTransactionRequest,
    ) -> Result<RpcResponse<SendTransactionResponse>, OreoError> {
        let path = format!("http://{}/wallet/sendTransaction", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(request);
        handle_response(resp)
    }
}

pub fn handle_response<S: Debug + for<'a> Deserialize<'a>>(
    resp: Result<Response, Error>,
) -> Result<RpcResponse<S>, OreoError> {
    info!("Handle response: {:?}", resp);
    let res = match resp {
        Ok(response) => match response.into_json::<RpcResponse<S>>() {
            Ok(data) => Ok(data),
            Err(e) => Err(RpcError {
                code: "Unknown".into(),
                status: 606,
                message: e.to_string(),
            }),
        },
        Err(ureq::Error::Status(_code, response)) => match response.into_json::<RpcError>() {
            Ok(data) => Err(data),
            Err(e) => Err(RpcError {
                code: "Unknown".into(),
                status: 606,
                message: e.to_string(),
            }),
        },
        Err(e) => Err(RpcError {
            code: "Unknown".into(),
            status: 606,
            message: e.to_string(),
        }),
    };
    debug!("Handle rpc response: {:?}", res);
    match res {
        Ok(data) => Ok(data),
        Err(e) => Err(OreoError::try_from(e).unwrap()),
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc_abi::{RpcResetAccountRequest, RpcSetScanningRequest};

    use super::RpcHandler;

    #[test]
    pub fn get_block_should_work() {
        let rpc_handler = RpcHandler::new("127.0.0.1:8021".into());
        let block = rpc_handler.get_latest_block().unwrap().data;
        println!("{:?}", block);
    }

    #[test]
    pub fn get_blocks_should_work() {
        let rpc_handler = RpcHandler::new("127.0.0.1:8021".into());
        let blocks = rpc_handler.get_blocks(3, 12).unwrap().data;
        println!("{:?}", blocks.blocks);
    }

    #[test]
    pub fn set_scanning_should_work() {
        let rpc_handler = RpcHandler::new("127.0.0.1:8021".into());
        let result = rpc_handler.set_scanning(RpcSetScanningRequest {
            account: "test".to_string(),
            enabled: false,
        });
        assert!(result.is_ok());
    }

    #[test]
    pub fn reset_account_should_work() {
        let rpc_handler = RpcHandler::new("127.0.0.1:8021".into());
        let result = rpc_handler.reset_account(RpcResetAccountRequest {
            account: "test".to_string(),
            reset_created_at: Some(false),
            reset_scanning_enabled: Some(false),
        });
        assert!(result.is_ok());
    }
}
