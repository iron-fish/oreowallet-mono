use crate::network::Network;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Testnet;

impl Network for Testnet {
    const ID: u8 = 0;

    const NAME: &'static str = "testnet";

    const GENESIS_BLOCK_HASH: &'static str =
        "7999c680bbd15d9adb7392e0c27a7caac7e596de5560c18e96365d0fd68140e3";

    const GENESIS_BLOCK_HEIGHT: u64 = 1;

    const NATIVE_ASSET_ID: &'static str =
        "51f33a2f14f92735e562dc658a5639279ddca3d5079a6d1242b2a588a9cbf44c";

    const ACCOUNT_VERSION: u8 = 2;

    const OREOSRIPTIONS_ENDPOINT: &'static str = "https://testnet_api.orescriptions.com/v1/api";

    const REORG_DEPTH: i64 = 100;

    const PRIMARY_BATCH: u64 = 100;

    const SECONDARY_BATCH: i64 = 10000;

    const RESCHEDULING_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

    const LOCAL_BLOCKS_CHECKPOINT: u64 = 830_000;

    const SET_ACCOUNT_LIMIT: usize = 20;
}
