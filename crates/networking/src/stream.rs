use std::io::{BufRead, BufReader, Read};
use std::marker::PhantomData;

use oreo_errors::OreoError;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use ureq::Response;

use crate::rpc_abi::RpcResponseStream;

pub struct StreamReader<T, R> {
    reader: BufReader<R>,
    _marker: PhantomData<T>,
}

#[derive(Deserialize, Serialize)]
#[serde(untagged)]
enum ResponseItem<T> {
    Data(RpcResponseStream<T>),
    Status { status: u16 },
}

impl<T, R> StreamReader<T, R>
where
    T: DeserializeOwned,
    R: Read,
{
    pub fn new(reader: R) -> Self {
        Self {
            reader: BufReader::new(reader),
            _marker: PhantomData,
        }
    }

    fn read_item(&mut self) -> Result<Option<Vec<u8>>, OreoError> {
        let mut item = Vec::new();
        loop {
            let bytes_read = self
                .reader
                .read_until(b'\x0c', &mut item)
                .map_err(|e| OreoError::RpcStreamError(e.to_string()))?;
            if bytes_read == 0 {
                break;
            }
            if item.last() == Some(&b'\x0c') {
                item.pop();
                break;
            }
        }
        match item.len() {
            0 => Ok(None),
            _ => Ok(Some(item)),
        }
    }

    /// Parses a chunk of data into a `ResponseItem<T>`.
    fn parse_item(&self, chunk: &[u8]) -> Result<Option<T>, OreoError> {
        match serde_json::from_slice::<ResponseItem<T>>(chunk) {
            Ok(ResponseItem::Data(item)) => Ok(Some(item.data)),
            Ok(ResponseItem::Status { status: 200 }) => Ok(None),
            Ok(ResponseItem::Status { status }) => Err(OreoError::RpcStreamError(format!(
                "Received error status: {}",
                status
            ))),
            Err(e) => {
                let err_str = format!("Failed to parse JSON object: {:?}", e);
                Err(OreoError::RpcStreamError(err_str))
            }
        }
    }
}

impl<T, R> Iterator for StreamReader<T, R>
where
    T: DeserializeOwned,
    R: Read,
{
    type Item = Result<T, OreoError>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.read_item() {
            Ok(Some(chunk)) => match self.parse_item(&chunk) {
                Ok(Some(data)) => Some(Ok(data)),
                Ok(None) => None, // End of stream
                Err(e) => Some(Err(e)),
            },
            Ok(None) => None, // EOF reached
            Err(e) => Some(Err(e)),
        }
    }
}

pub trait ResponseExt {
    fn into_stream<T: DeserializeOwned>(
        self,
    ) -> StreamReader<T, Box<dyn Read + Send + Sync + 'static>>;
}

impl ResponseExt for Response {
    fn into_stream<T: DeserializeOwned>(
        self,
    ) -> StreamReader<T, Box<dyn Read + Send + Sync + 'static>> {
        let reader = self.into_reader();
        StreamReader::new(Box::new(reader))
    }
}

#[cfg(test)]
mod tests {
    use crate::rpc_abi::TransactionStatus;

    use super::*;

    #[test]
    fn test_stream_reader_with_status() {
        // Prepare some test data.
        let transaction_status = TransactionStatus {
            hash: "cde4c2a5bc7cb6cbad93a414ff76176e07412fbd48f2f3d1ee8f7fc1238626a5".to_string(),
            fee: "1".to_string(),
            r#type: "type".to_string(),
            status: "status".to_string(),
            block_sequence: Some(1),
            timestamp: 1,
            asset_balance_deltas: Vec::new(),
        };
        let item1: RpcResponseStream<TransactionStatus> = RpcResponseStream {
            data: transaction_status.clone(),
        };
        let response1 = ResponseItem::Data(item1);

        let status = ResponseItem::<TransactionStatus>::Status { status: 200 };

        let json1 = serde_json::to_string(&response1).unwrap();
        let json_status = serde_json::to_string(&status).unwrap();

        // Add \x0c separators
        let data = format!("{}\x0c{}\x0c", json1, json_status);

        let reader = StreamReader::<TransactionStatus, _>::new(data.as_bytes());
        let items: Vec<_> = reader.collect();
        let returned_status = items[0].as_ref().unwrap();
        assert_eq!(returned_status.hash, transaction_status.hash);
        assert_eq!(returned_status.fee, transaction_status.fee);
        assert_eq!(returned_status.r#type, transaction_status.r#type);
        assert_eq!(returned_status.status, transaction_status.status);
        assert_eq!(
            returned_status.block_sequence,
            transaction_status.block_sequence
        );
        assert_eq!(returned_status.timestamp, transaction_status.timestamp);
        assert_eq!(
            returned_status.asset_balance_deltas.len(),
            transaction_status.asset_balance_deltas.len()
        );
    }

    #[test]
    fn test_stream_reader_with_error_status() {
        // Prepare some test data.
        let item1 = RpcResponseStream { data: 42u32 };
        let response1 = ResponseItem::Data(item1);

        let status = ResponseItem::<u32>::Status { status: 500 };

        let json1 = serde_json::to_string(&response1).unwrap();
        let json_status = serde_json::to_string(&status).unwrap();

        // Add \x0c separators
        let data = format!("{}\x0c{}\x0c", json1, json_status);

        let reader = StreamReader::<u32, _>::new(data.as_bytes());
        let items: Vec<_> = reader.collect();

        assert_eq!(items.len(), 2);
        assert_eq!(items[0], Ok(42u32));

        match &items[1] {
            Err(OreoError::RpcStreamError(msg)) => {
                assert!(msg.contains("Received error status: 500"))
            }
            _ => panic!("Expected error with status code 500"),
        }
    }
}
