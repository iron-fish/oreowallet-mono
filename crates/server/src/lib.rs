use axum_extra::{
    headers::{authorization::Basic, Authorization},
    TypedHeader,
};
use sha2::{Digest, Sha256};
use std::str;
use std::{env, net::SocketAddr, sync::Arc, time::Duration};

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
use db_handler::{DBHandler, PgHandler};
use networking::{rpc_handler::RpcHandler, server_handler::ServerHandler};
use tokio::net::TcpListener;
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::handlers::{
    account_status_handler, add_transaction_handler, create_transaction_handler,
    get_balances_handler, get_ores_handler, get_transaction_handler, get_transactions_handler,
    health_check_handler, import_account_handler, latest_block_handler, remove_account_handler,
    rescan_account_handler, update_scan_status_handler,
};

mod handlers;

#[derive(Debug, Clone)]
pub struct SecpKey {
    pub sk: [u8; 32],
    pub pk: [u8; 33],
}

#[derive(Debug, Clone)]
pub struct SharedState<T: DBHandler> {
    pub db_handler: T,
    pub rpc_handler: RpcHandler,
    pub scan_handler: ServerHandler,
    pub secp: SecpKey,
    pub genesis_hash: String,
}

impl<T> SharedState<T>
where
    T: DBHandler,
{
    pub fn new(
        db_handler: T,
        endpoint: &str,
        scan: &str,
        secp: SecpKey,
        genesis_hash: String,
    ) -> Self {
        Self {
            db_handler: db_handler,
            rpc_handler: RpcHandler::new(endpoint.into()),
            scan_handler: ServerHandler::new(scan.into()),
            secp,
            genesis_hash,
        }
    }
}
// Authentication middleware function
pub async fn auth<T: DBHandler>(
    State(shared_state): State<Arc<SharedState<T>>>,
    TypedHeader(Authorization(basic)): TypedHeader<Authorization<Basic>>,
    req: Request<Body>,
    next: Next,
) -> impl IntoResponse
where
    T: DBHandler + Send + Sync + 'static,
{
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

pub async fn run_server(
    listen: SocketAddr,
    rpc_server: String,
    db_handler: PgHandler,
    scan: String,
    sk_u8: [u8; 32],
    pk_u8: [u8; 33],
) -> Result<()> {
    let genesis_hash;
    {
        let temp_handler: RpcHandler = RpcHandler::new(rpc_server.clone().into());
        let latest_block_response = temp_handler.get_latest_block()?.data;
        genesis_hash = latest_block_response.genesis_block_identifier.hash;
    }
    info!("Genesis hash: {}", genesis_hash);
    let shared_resource = Arc::new(SharedState::new(
        db_handler,
        &rpc_server,
        &scan,
        SecpKey {
            sk: sk_u8,
            pk: pk_u8,
        },
        genesis_hash,
    ));
    let auth_middleware = from_fn_with_state(shared_resource.clone(), auth);

    let no_auth_router = Router::new()
        .route("/import", post(import_account_handler))
        .with_state(shared_resource.clone());

    let mut auth_router = Router::new()
        .route("/remove", post(remove_account_handler))
        .route("/getBalances", post(get_balances_handler))
        .route("/getTransaction", post(get_transaction_handler))
        .route("/getTransactions", post(get_transactions_handler))
        .route("/createTx", post(create_transaction_handler))
        .route("/broadcastTx", post(add_transaction_handler)) // TODO: Remove once front end is updated to addTx
        .route("/addTx", post(add_transaction_handler))
        .route("/accountStatus", post(account_status_handler))
        .route("/latestBlock", get(latest_block_handler))
        .route("/ores", post(get_ores_handler))
        .route("/rescan", post(rescan_account_handler))
        .route("/healthCheck", get(health_check_handler))
        .route("/updateScan", post(update_scan_status_handler))
        .with_state(shared_resource.clone());

    if env::var("ENABLE_AUTH").unwrap_or_else(|_| "false".to_string()) == "true" {
        auth_router = auth_router.layer(auth_middleware);
    }

    let router = no_auth_router
        .merge(auth_router)
        .layer(
            ServiceBuilder::new()
                .layer(HandleErrorLayer::new(|_: BoxError| async {
                    StatusCode::REQUEST_TIMEOUT
                }))
                .layer(TimeoutLayer::new(Duration::from_secs(30))),
        )
        .layer(
            CorsLayer::new()
                .allow_methods(Any)
                .allow_origin(Any)
                .allow_headers(Any),
        );

    let app = router.fallback(handler_404);

    let listener = TcpListener::bind(&listen).await?;
    info!("Server listening on {}", listen);
    axum::serve(listener, app).await?;
    Ok(())
}

async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Not Found")
}
