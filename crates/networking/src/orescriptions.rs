use params::network::Network;
use serde::{Deserialize, Serialize};

use crate::rpc_abi::AssetBalance;

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Ores {
    pub asset_id: String,
    pub mint_height: u32,
    pub tick: String,
    pub tick_index: u32,
    pub data: String,
    pub removed_by_owner: bool,
}

pub fn is_ores_local<N: Network>(asset: &AssetBalance) -> bool {
    asset.asset_id != N::NATIVE_ASSET_ID.to_string()
        && asset.confirmed == "1".to_string()
        && asset.unconfirmed == "1".to_string()
}

pub async fn get_ores<N: Network>(id: &str) -> anyhow::Result<Ores> {
    let path = format!("{}/orescription/{}", N::OREOSRIPTIONS_ENDPOINT, id);
    Ok(ureq::get(&path).call()?.into_json()?)
}

#[cfg(test)]
mod tests {
    use params::mainnet::Mainnet;

    use crate::orescriptions::get_ores;

    #[tokio::test]
    pub async fn check_ores_should_work() {
        let asset_id = "6272e464d84761d9c6247d9d4d2feb42964a5b2a71b9b179df27bbe0730c88af";
        let ores = get_ores::<Mainnet>(asset_id).await;
        assert!(ores.is_ok());
        match ores {
            Ok(ore) => {
                println!("{:?}", ore);
            }
            Err(_) => println!("error should never happen"),
        }
    }

    #[tokio::test]
    pub async fn check_ores_should_fail() {
        let asset_id = "6272e464d84761d9c6247d9d4d2feb42964a5b2a71b9b179df27bbe0x30c88af";
        let ores = get_ores::<Mainnet>(asset_id).await;
        assert!(ores.is_err());
        match ores {
            Ok(ore) => {
                println!("{:?}", ore);
            }
            Err(e) => println!("error here: {}", e),
        }
    }
}
