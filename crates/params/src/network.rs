use std::{fmt::Debug, time::Duration};

pub trait Network: 'static + Send + Sync + Debug + Eq + PartialEq + Copy + Clone {
    /// The network ID.
    const ID: u8;
    /// The network name.
    const NAME: &'static str;
    /// The block hash of the genesis block.
    const GENESIS_BLOCK_HASH: &'static str;
    /// The block height of the genesis block.
    const GENESIS_BLOCK_HEIGHT: u64;
    /// The native token ID.
    const NATIVE_ASSET_ID: &'static str;
    /// The account version.
    const ACCOUNT_VERSION: u8;
    /// The orescriptions endpoint.
    const OREOSRIPTIONS_ENDPOINT: &'static str;
    /// The reorg-depth to handle.
    const REORG_DEPTH: i64;
    /// The batch size for primary scan scheduling.
    const PRIMARY_BATCH: u64;
    /// The batch size for secondary scan scheduling.
    const SECONDARY_BATCH: i64;
    /// The time duration to rescheduling scan task.
    const RESCHEDULING_DURATION: Duration;
    /// The local block checkpoint for scanning.
    const LOCAL_BLOCKS_CHECKPOINT: u64;
    /// The set account head request limit.
    const SET_ACCOUNT_LIMIT: usize;
}
