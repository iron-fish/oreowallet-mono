use std::{net::SocketAddr, time::Duration};

use axum::{
    error_handling::HandleErrorLayer,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    BoxError, Router,
};
use tokio::{net::TcpListener, signal, time::sleep};
use tower::{timeout::TimeoutLayer, ServiceBuilder};
use tower_http::cors::{Any, CorsLayer};
use tracing::info;

use crate::handlers::{generate_proof_handler, health_check_handler};

pub mod handlers;
pub const MAX_SPEND_PROOFS: usize = 15;

pub async fn run_prover(listen: SocketAddr) -> anyhow::Result<()> {
    let router = Router::new()
        .route("/generateProofs", post(generate_proof_handler))
        .route("/healthCheck", get(health_check_handler))
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
    let listener = TcpListener::bind(&listen).await?;
    let app = router.fallback(handler_404);
    info!("Prover listening on {}", listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn handler_404() -> impl IntoResponse {
    (StatusCode::NOT_FOUND, "Not Found")
}

async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("Ctrl+C received, exit");
            sleep(Duration::from_secs(3)).await;
        },
        _ = terminate => {
            info!("terminate signal received, exit");
            sleep(Duration::from_secs(3)).await;
        },
    }
}
