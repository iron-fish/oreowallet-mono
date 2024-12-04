use std::time::Duration;

pub const ACCOUNT_VERSION: u8 = 2;
pub const IRON_NATIVE_ASSET: &str =
    "51f33a2f14f92735e562dc658a5639279ddca3d5079a6d1242b2a588a9cbf44c";
pub const OREOS_VALUE: &str = "1";
pub const OREOSRIPTIONS_ENDPOINT: &str = "http://localhost:20001/api";
pub const MAINNET_GENESIS_HASH: &str =
    "eac623b099b8081d2bde92d43a4a7795385c94e2c0ae4097ef488972e83ff2b3";
pub const MAINNET_GENESIS_SEQUENCE: i64 = 1;
pub const REORG_DEPTH: i64 = 100;
pub const SECONDARY_BATCH: i64 = 10000;
pub const RESCHEDULING_DURATION: Duration = Duration::from_secs(30);
pub const PRIMARY_BATCH: u64 = 200;
// pub const LOCAL_BLOCKS_CHECKPOINT: u64 = 767_000;
pub const LOCAL_BLOCKS_CHECKPOINT: u64 = 40_000;
