use constants::MAINNET_GENESIS_SEQUENCE;
use db_handler::{address_to_name, Account};
use oreo_errors::OreoError;
use serde::{Deserialize, Serialize};

use crate::rpc_abi::{
    AssetBalanceDelta, BlockInfo, RpcGetAccountTransactionResponse, RpcNote, TransactionWithNotes,
};

#[derive(Debug, Deserialize, Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ImportAccountRequest {
    pub view_key: String,
    pub incoming_view_key: String,
    pub outgoing_view_key: String,
    pub public_address: String,
    pub created_at: Option<BlockInfo>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ImportAccountResponse {
    pub name: String,
}

impl ImportAccountRequest {
    pub fn to_account(&self, genesis_hash: String) -> Account {
        let (create_head, create_hash) = match &self.created_at {
            Some(creat) => (Some(creat.sequence as i64), Some(creat.hash.clone())),
            None => (None, None),
        };
        Account {
            address: self.public_address.clone(),
            name: address_to_name(&self.public_address),
            create_head,
            create_hash: create_hash.clone(),
            head: create_head.unwrap_or(MAINNET_GENESIS_SEQUENCE),
            hash: create_hash.unwrap_or(genesis_hash),
            in_vk: self.incoming_view_key.clone(),
            out_vk: self.outgoing_view_key.clone(),
            vk: self.view_key.clone(),
            need_scan: false,
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TransactionDetail {
    pub hash: String,
    pub fee: String,
    pub r#type: String,
    pub status: String,
    pub block_sequence: Option<u64>,
    pub timestamp: u64,
    pub asset_balance_deltas: Vec<AssetBalanceDelta>,
    pub sender: String,
    pub receiver: String,
    pub memo: Option<String>,
    pub value: String,
}

impl TransactionDetail {
    pub fn from(tx: TransactionWithNotes) -> Result<Self, OreoError> {
        let TransactionWithNotes {
            hash,
            fee,
            r#type,
            status,
            block_sequence,
            timestamp,
            asset_balance_deltas,
            notes,
        } = tx;
        let mut notes = notes.unwrap();
        let note = match notes
            .iter()
            .filter(|asset| asset.owner != asset.sender)
            .collect::<Vec<&RpcNote>>()
            .len()
        {
            0 => notes.pop(),
            _ => notes
                .into_iter()
                .filter(|asset| asset.owner != asset.sender)
                .collect::<Vec<RpcNote>>()
                .pop(),
        };
        match note {
            Some(RpcNote {
                value,
                memo,
                sender,
                owner,
            }) => Ok(Self {
                hash,
                fee,
                r#type,
                status,
                block_sequence,
                timestamp,
                asset_balance_deltas,
                sender,
                receiver: owner,
                memo: Some(memo),
                value,
            }),
            None => Err(OreoError::InternalRpcError("no note".to_string())),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct GetTransactionDetailResponse {
    pub account: String,
    pub transaction: TransactionDetail,
}

impl GetTransactionDetailResponse {
    pub fn from_rpc_data(data: RpcGetAccountTransactionResponse) -> Result<Self, OreoError> {
        match data.transaction {
            Some(tx) => TransactionDetail::from(tx).map(|x| Self {
                account: data.account,
                transaction: x,
            }),
            None => Err(OreoError::TransactionNotFound),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateProofRequest {
    pub spend_circuits: Vec<Vec<u8>>,
    pub output_circuits: Vec<Vec<u8>>,
    pub mint_asset_circuits: Vec<Vec<u8>>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GenerateProofResponse {
    pub spend_proofs: Vec<Vec<u8>>,
    pub output_proofs: Vec<Vec<u8>>,
    pub mint_asset_proofs: Vec<Vec<u8>>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct RescanAccountResponse {
    pub success: bool,
}
