use axum_extra::{
    headers::{authorization::Basic, Authorization},
    TypedHeader,
};
use params::{mainnet::Mainnet, network::Network, testnet::Testnet};
use sha2::{Digest, Sha256};
use std::str::{self, FromStr};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use utils::Signer;

use anyhow::Result;
use axum::{
    body::Body,
    error_handling::HandleErrorLayer,
    extract::State,
    http::{Request, StatusCode},
    middleware::{from_fn_with_state, Next},
    response::IntoResponse,
    routing::{get, post},
    BoxError, Router,
};
use db_handler::DBHandler;
use networking::{rpc_abi::BlockInfo, rpc_handler::RpcHandler, server_handler::ServerHandler};
use tokio::net::TcpListener;
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

use crate::handlers::{
    account_status_handler, add_transaction_handler, create_transaction_handler,
    get_balances_handler, get_ores_handler, get_transaction_handler, get_transactions_handler,
    health_check_handler, import_account_handler, latest_block_handler, remove_account_handler,
    rescan_account_handler, update_scan_status_handler,
};

mod handlers;

pub struct SharedState {
    pub db_handler: Box<dyn Send + Sync + DBHandler>,
    pub rpc_handler: RpcHandler,
    pub scan_handler: ServerHandler,
    pub operator: Signer,
    pub network: u8,
}

impl SharedState {
    pub fn new(
        db_handler: Box<dyn DBHandler + Send + Sync>,
        endpoint: &str,
        scan: &str,
        operator: String,
        network: u8,
    ) -> Self {
        let operator = Signer::from_str(&operator).expect("Invalid secret key used");
        Self {
            db_handler: db_handler,
            rpc_handler: RpcHandler::new(endpoint.into()),
            scan_handler: ServerHandler::new(scan.into()),
            operator,
            network,
        }
    }

    pub fn network(&self) -> u8 {
        self.network
    }

    pub fn genesis(&self) -> BlockInfo {
        match self.network() {
            Testnet::ID => BlockInfo {
                hash: Testnet::GENESIS_BLOCK_HASH.to_string(),
                sequence: Testnet::GENESIS_BLOCK_HEIGHT,
            },
            _ => BlockInfo {
                hash: Mainnet::GENESIS_BLOCK_HASH.to_string(),
                sequence: Mainnet::GENESIS_BLOCK_HEIGHT,
            },
        }
    }

    pub fn account_version(&self) -> u8 {
        match self.network() {
            Testnet::ID => Testnet::ACCOUNT_VERSION,
            _ => Mainnet::ACCOUNT_VERSION,
        }
    }

    pub fn set_account_limit(&self) -> usize {
        match self.network() {
            Testnet::ID => Testnet::SET_ACCOUNT_LIMIT,
            _ => Mainnet::SET_ACCOUNT_LIMIT,
        }
    }
}

unsafe impl Send for SharedState {}
unsafe impl Sync for SharedState {}

// Authentication middleware function
pub async fn auth(
    State(shared_state): State<Arc<SharedState>>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse {
    match shared_state
        .db_handler
        .get_account(basic.username().to_string())
        .await
    {
        Ok(account) => {
            let bytes =
                hex::decode(account.vk).map_err(|_| (StatusCode::UNAUTHORIZED, "Invalid token"))?;
            let token = Sha256::digest(bytes);
            let token_hex = hex::encode(token);
            if token_hex != basic.password() {
                return Err((StatusCode::UNAUTHORIZED, "Invalid token"));
            }
            return Ok(next.run(req).await);
        }
        Err(_) => {
            // Token is invalid
            return Err((StatusCode::UNAUTHORIZED, "Invalid token"));
        }
    }
}

pub async fn run_server<N: Network>(
    listen: SocketAddr,
    rpc_server: String,
    db_handler: Box<dyn DBHandler + Send + Sync>,
    scan: String,
    operator: String,
) -> Result<()> {
    let genesis_hash;
    {
        let temp_handler: RpcHandler = RpcHandler::new(rpc_server.clone().into());
        let latest_block_response = temp_handler.get_latest_block()?.data;
        genesis_hash = latest_block_response.genesis_block_identifier.hash;
    }

    info!("Genesis hash: {}", genesis_hash);
    if N::GENESIS_BLOCK_HASH.to_lowercase() != genesis_hash.to_lowercase() {
        error!("Network genesis hash doesnt match, exit!");
        return Ok(());
    }

    let shared_resource = Arc::new(SharedState::new(
        db_handler,
        &rpc_server,
        &scan,
        operator,
        N::ID,
    ));
    let auth_middleware = from_fn_with_state(shared_resource.clone(), auth);

    let no_auth_router = Router::new()
        .route("/import", post(import_account_handler))
        .route("/healthCheck", get(health_check_handler))
        .route("/updateScan", post(update_scan_status_handler))
        .with_state(shared_resource.clone());

    let mut auth_router = Router::new()
        .route("/remove", post(remove_account_handler))
        .route("/getBalances", post(get_balances_handler))
        .route("/getTransaction", post(get_transaction_handler))
        .route("/getTransactions", post(get_transactions_handler))
        .route("/createTx", post(create_transaction_handler))
        .route("/broadcastTx", post(add_transaction_handler))
        .route("/addTx", post(add_transaction_handler))
        .route("/accountStatus", post(account_status_handler))
        .route("/latestBlock", get(latest_block_handler))
        .route("/ores", post(get_ores_handler))
        .route("/rescan", post(rescan_account_handler))
        .with_state(shared_resource.clone());

    auth_router = auth_router.layer(auth_middleware);

    let router = no_auth_router
        .merge(auth_router)
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_: BoxError| async {
                    StatusCode::REQUEST_TIMEOUT
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(60))),
        )
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_origin(Any)
                .allow_headers(Any),
        );

    let listener = TcpListener::bind(&listen).await?;
    let app = router.fallback(handler_404);
    info!("Server listening on {}", listen);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Not Found")
}
