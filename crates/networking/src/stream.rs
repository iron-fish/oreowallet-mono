use std::io::{BufRead, BufReader, Read};
use std::marker::PhantomData;

use oreo_errors::OreoError;
use serde::de::DeserializeOwned;
use ureq::Response;

use crate::rpc_abi::RpcResponseStream;

pub struct StreamReader<T> {
    reader: BufReader<Box<dyn Read>>,
    _marker: PhantomData<T>,
}

impl<T> StreamReader<T> {
    pub fn new(reader: Box<dyn Read>) -> Self {
        Self {
            reader: BufReader::new(reader),
            _marker: PhantomData,
        }
    }
}

impl<T> Iterator for StreamReader<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T, OreoError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut chunk = Vec::new();
        match self.reader.read_until(b'\x0c', &mut chunk) {
            Ok(0) => None, // EOF reached
            Ok(_) => {
                if chunk.ends_with(&[b'\x0c']) {
                    chunk.pop();
                }

                if chunk.is_empty() {
                    self.next()
                } else {
                    let item_result = serde_json::from_slice::<RpcResponseStream<T>>(&chunk)
                        .map(|item| item.data)
                        .map_err(|e| OreoError::InternalRpcError(e.to_string()));
                    Some(item_result)
                }
            }
            Err(e) => Some(Err(OreoError::InternalRpcError(e.to_string()))),
        }
    }
}

pub trait RequestExt {
    fn into_stream<T: DeserializeOwned>(self) -> StreamReader<T>;
}

impl RequestExt for Response {
    fn into_stream<T: DeserializeOwned>(self) -> StreamReader<T> {
        let reader = self.into_reader();
        StreamReader::new(Box::new(reader))
    }
}
