use crate::network::Network;

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub struct Mainnet;

impl Network for Mainnet {
    const ID: u8 = 1;

    const NAME: &'static str = "mainnet";

    const GENESIS_BLOCK_HASH: &'static str =
        "eac623b099b8081d2bde92d43a4a7795385c94e2c0ae4097ef488972e83ff2b3";

    const GENESIS_BLOCK_HEIGHT: u64 = 1;

    const NATIVE_ASSET_ID: &'static str =
        "51f33a2f14f92735e562dc658a5639279ddca3d5079a6d1242b2a588a9cbf44c";

    const ACCOUNT_VERSION: u8 = 2;

    const OREOSRIPTIONS_ENDPOINT: &'static str = "https://api.orescriptions.com/v1/api";

    const REORG_DEPTH: i64 = 50;

    const PRIMARY_BATCH: u64 = 100;

    const SECONDARY_BATCH: i64 = 10000;

    const RESCHEDULING_DURATION: std::time::Duration = std::time::Duration::from_secs(30);

    const LOCAL_BLOCKS_CHECKPOINT: u64 = 939_400;

    const SET_ACCOUNT_LIMIT: usize = 20;
}
