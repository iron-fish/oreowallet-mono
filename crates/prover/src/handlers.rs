use axum::{extract, response::IntoResponse, Json};
use bellperson::groth16;
use ironfish_rust::sapling_bls12::SAPLING;
use ironfish_zkp::proofs::{MintAsset, Output, Spend};
use networking::web_abi::{GenerateProofRequest, GenerateProofResponse};
use oreo_errors::OreoError;
use rand::thread_rng;
use rayon::iter::{IndexedParallelIterator, IntoParallelRefIterator, ParallelIterator};
use serde_json::json;
use tracing::info;

use crate::MAX_SPEND_PROOFS;

pub async fn generate_proof_handler(
    extract::Json(request): extract::Json<GenerateProofRequest>,
) -> impl IntoResponse {
    info!("New proof generation task");
    let spend_proofs_needed = request.spend_circuits.len();
    if spend_proofs_needed >= MAX_SPEND_PROOFS {
        return OreoError::TooManyProofs.into_response();
    }

    let spend_proofs = request
        .spend_circuits
        .par_iter()
        .enumerate()
        .map(|(_, bytes)| {
            let proof_bytes = if let Ok(spend) = Spend::read(&bytes[..]) {
                let proof =
                    groth16::create_random_proof(spend, &SAPLING.spend_params, &mut thread_rng());
                if let Ok(proof) = proof {
                    let mut writer = vec![];
                    proof.write(&mut writer).unwrap();
                    Some(writer)
                } else {
                    None
                }
            } else {
                None
            };
            proof_bytes
        })
        .flatten()
        .collect::<Vec<Vec<u8>>>();

    if spend_proofs.len() < spend_proofs_needed {
        return OreoError::GenerateProofError("spend".to_string()).into_response();
    }

    let output_proofs_needed = request.output_circuits.len();
    let output_proofs = request
        .output_circuits
        .par_iter()
        .enumerate()
        .map(|(_, bytes)| {
            let proof_bytes = if let Ok(output) = Output::read(&bytes[..]) {
                let proof =
                    groth16::create_random_proof(output, &SAPLING.output_params, &mut thread_rng());
                if let Ok(proof) = proof {
                    let mut writer = vec![];
                    proof.write(&mut writer).unwrap();
                    Some(writer)
                } else {
                    None
                }
            } else {
                None
            };
            proof_bytes
        })
        .flatten()
        .collect::<Vec<Vec<u8>>>();

    if output_proofs.len() < output_proofs_needed {
        return OreoError::GenerateProofError("output".to_string()).into_response();
    }

    let mint_asset_proofs_needed = request.mint_asset_circuits.len();
    let mint_asset_proofs = request
        .mint_asset_circuits
        .par_iter()
        .enumerate()
        .map(|(_, bytes)| {
            let proof_bytes = if let Ok(mint_asset) = MintAsset::read(&bytes[..]) {
                let proof = groth16::create_random_proof(
                    mint_asset,
                    &SAPLING.mint_params,
                    &mut thread_rng(),
                );
                if let Ok(proof) = proof {
                    let mut writer = vec![];
                    proof.write(&mut writer).unwrap();
                    Some(writer)
                } else {
                    None
                }
            } else {
                None
            };
            proof_bytes
        })
        .flatten()
        .collect::<Vec<Vec<u8>>>();

    if mint_asset_proofs.len() < mint_asset_proofs_needed {
        return OreoError::GenerateProofError("mint asset".to_string()).into_response();
    }

    let proof = GenerateProofResponse {
        spend_proofs,
        output_proofs,
        mint_asset_proofs,
    };
    info!("New proof generated");
    Json(json!({"code": 200, "data": proof})).into_response()
}

pub async fn health_check_handler() -> impl IntoResponse {
    Json(json!({"code": 200, "data": "Hello prover!"})).into_response()
}
