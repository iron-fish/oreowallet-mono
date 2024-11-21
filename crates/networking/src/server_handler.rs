use std::{fmt::Debug, time::Duration};

use oreo_errors::OreoError;
use serde::Deserialize;
use tracing::debug;
use ureq::{Agent, AgentBuilder, Error, Response};

use crate::decryption_message::{DecryptionMessage, ScanRequest, ScanResponse, SuccessResponse};

#[derive(Debug, Clone)]
pub struct ServerHandler {
    pub endpoint: String,
    pub agent: Agent,
}

impl ServerHandler {
    pub fn new(endpoint: String) -> Self {
        Self {
            endpoint,
            agent: AgentBuilder::new()
                .timeout_read(Duration::from_secs(5))
                .timeout_write(Duration::from_secs(5))
                .build(),
        }
    }

    pub fn submit_scan_request(
        &self,
        request: DecryptionMessage<ScanRequest>,
    ) -> Result<SuccessResponse, OreoError> {
        let path = format!("http://{}/scanAccount", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(request);
        handle_response(resp)
    }

    pub fn submit_scan_response(
        &self,
        request: DecryptionMessage<ScanResponse>,
    ) -> Result<SuccessResponse, OreoError> {
        let path = format!("http://{}/updateScan", self.endpoint);
        let resp = self.agent.clone().post(&path).send_json(request);
        handle_response(resp)
    }
}

fn handle_response<S: Debug + for<'a> Deserialize<'a>>(
    resp: Result<Response, Error>,
) -> Result<S, OreoError> {
    let res = match resp {
        Ok(response) => match response.into_json::<S>() {
            Ok(data) => Ok(data),
            Err(err) => Err(OreoError::DServerError(err.to_string())),
        },
        Err(err) => Err(OreoError::DServerError(err.to_string())),
    };
    debug!("Handle rpc response: {:?}", res);
    res
}
