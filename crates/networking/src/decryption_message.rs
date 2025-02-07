use serde::{Deserialize, Serialize};

use crate::rpc_abi::BlockInfo;
pub use crate::rpc_abi::RpcSetAccountHeadRequest as ScanResponse;

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct DecryptionMessage<T> {
    pub message: T,
    pub signature: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ScanRequest {
    pub in_vk: String,
    pub out_vk: String,
    pub address: String,
    pub head: Option<BlockInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct SuccessResponse {
    pub success: bool,
}
