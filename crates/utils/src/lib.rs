use std::{cmp, ops::Range, time::Duration};

use secp256k1::{
    ecdsa,
    hashes::{sha256, Hash},
    All, Error, Message, PublicKey, Secp256k1, SecretKey, Signing, Verification,
};
use tokio::sync::oneshot;
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

pub use secp256k1::ecdsa::Signature;

pub async fn handle_signals() -> anyhow::Result<()> {
    let (router, handler) = oneshot::channel();
    tokio::spawn(async move {
        let _ = router.send(());
        match tokio::signal::ctrl_c().await {
            Ok(()) => {
                info!("Shutdowning...");
                tokio::time::sleep(Duration::from_millis(5000)).await;
                info!("Goodbye");
                std::process::exit(0);
            }
            Err(error) => warn!("tokio::signal::ctrl_c encountered an error: {}", error),
        }
    });
    let _ = handler.await;
    info!("Signal handler installed");
    Ok(())
}

pub fn initialize_logger(verbosity: u8) {
    match verbosity {
        0 => std::env::set_var("RUST_LOG", "info"),
        1 => std::env::set_var("RUST_LOG", "debug"),
        2..=4 => std::env::set_var("RUST_LOG", "trace"),
        _ => std::env::set_var("RUST_LOG", "info"),
    };
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("ironfish-server=info".parse().unwrap()),
        )
        .init();
}

pub fn blocks_range(blocks: Range<u64>, batch: u64) -> Vec<Range<u64>> {
    let end = blocks.end;
    let mut result = vec![];
    for block in blocks.step_by(batch as usize) {
        let start = block;
        let end = cmp::min(start + batch - 1, end);
        result.push(start..end)
    }
    result
}

pub fn verify<C: Verification>(
    secp: &Secp256k1<C>,
    msg: &[u8],
    sig: [u8; 64],
    pubkey: &[u8; 33],
) -> Result<bool, Error> {
    let msg = sha256::Hash::hash(msg);
    let msg = Message::from_digest_slice(msg.as_ref())?;
    let sig = ecdsa::Signature::from_compact(&sig)?;
    let pubkey = PublicKey::from_slice(pubkey)?;

    Ok(secp.verify_ecdsa(&msg, &sig, &pubkey).is_ok())
}

pub fn sign<C: Signing>(
    secp: &Secp256k1<C>,
    msg: &[u8],
    seckey: &[u8; 32],
) -> Result<ecdsa::Signature, Error> {
    let msg = sha256::Hash::hash(msg);
    let msg = Message::from_digest_slice(msg.as_ref())?;
    let seckey = SecretKey::from_slice(seckey)?;
    Ok(secp.sign_ecdsa(&msg, &seckey))
}

pub fn default_secp() -> Secp256k1<All> {
    Secp256k1::new()
}
