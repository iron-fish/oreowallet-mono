use oreo_errors::OreoError;
use serde::{Deserialize, Serialize};

mod handler;

pub use handler::*;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RpcError {
    pub code: String,
    pub status: u16,
    pub message: String,
}

impl TryFrom<RpcError> for OreoError {
    type Error = OreoError;

    fn try_from(value: RpcError) -> Result<Self, Self::Error> {
        match &value.code as &str {
            "insufficient-balance" => Ok(OreoError::BalanceNotEnough),
            "account-exists" => {
                // Should never happen
                Ok(OreoError::Duplicate("0x00".to_string()))
            }
            _ => Ok(OreoError::InternalRpcError(value.message)),
        }
    }
}
