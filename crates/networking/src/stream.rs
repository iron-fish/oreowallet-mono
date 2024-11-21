use std::io::{BufRead, BufReader, Read};
use std::marker::PhantomData;

use oreo_errors::OreoError;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use ureq::Response;

use crate::rpc_abi::RpcResponseStream;

pub struct StreamReader<T, R> {
    reader: BufReader<R>,
    _marker: PhantomData<T>,
}

#[derive(Deserialize)]
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

pub trait RequestExt {
    fn into_stream<T: DeserializeOwned>(
        self,
    ) -> StreamReader<T, Box<dyn Read + Send + Sync + 'static>>;
}

impl RequestExt for Response {
    fn into_stream<T: DeserializeOwned>(
        self,
    ) -> StreamReader<T, Box<dyn Read + Send + Sync + 'static>> {
        let reader = self.into_reader();
        StreamReader::new(Box::new(reader))
    }
}
