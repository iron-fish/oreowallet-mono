use std::str::FromStr;

use futures::executor::block_on;
use oreo_errors::OreoError;
use sqlx::{postgres::PgConnectOptions, ConnectOptions, PgPool, Row};

use crate::{BonusAddress, DBTransaction, InnerBlock};

use super::{Account, DBHandler};

#[derive(Debug, Clone)]
pub struct PgHandler {
    pub pool: PgPool,
}

impl PgHandler {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    pub async fn insert(&self, account: Account) -> Result<String, sqlx::Error> {
        let result = sqlx::query(
            "INSERT INTO wallet.account (name, create_head, create_hash, head, hash, in_vk, out_vk, vk, address) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) RETURNING name"
        )
        .bind(account.name.clone())
        .bind(account.create_head.clone())
        .bind(account.create_hash.clone())
        .bind(account.head)
        .bind(account.hash.clone())
        .bind(account.in_vk.clone())
        .bind(account.out_vk.clone())
        .bind(account.vk.clone())
        .bind(account.address.clone())
        .fetch_one(&self.pool)
        .await?.get(0);
        Ok(result)
    }

    pub async fn get_one(&self, address: String) -> Result<Account, sqlx::Error> {
        let result =
            sqlx::query_as::<_, Account>("SELECT * FROM wallet.account WHERE address = $1")
                .bind(address)
                .fetch_one(&self.pool)
                .await?;
        Ok(result)
    }

    pub async fn get_one_by_name(&self, name: String) -> Result<Account, sqlx::Error> {
        let result = sqlx::query_as::<_, Account>("SELECT * FROM wallet.account WHERE name = $1")
            .bind(name)
            .fetch_one(&self.pool)
            .await?;
        Ok(result)
    }

    pub async fn update_one(&self, state: Account) -> Result<String, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE wallet.account SET head = $1, hash = $2, create_head = $3, create_hash = $4 WHERE address = $5 RETURNING name",
        )
        .bind(state.head)
        .bind(state.hash.clone())
        .bind(state.create_head)
        .bind(state.create_hash.clone())
        .bind(state.address.clone())
        .fetch_one(&self.pool)
        .await?
        .get(0);
        Ok(result)
    }

    pub async fn delete(&self, address: String) -> Result<String, sqlx::Error> {
        let result = sqlx::query("DELETE FROM wallet.account WHERE address = $1 RETURNING name")
            .bind(address)
            .fetch_one(&self.pool)
            .await?
            .get(0);
        Ok(result)
    }

    pub async fn set_scan(&self, address: String, new_status: bool) -> Result<String, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE wallet.account SET need_scan = $1 WHERE address = $2 RETURNING name",
        )
        .bind(new_status)
        .bind(address)
        .fetch_one(&self.pool)
        .await?
        .get(0);
        Ok(result)
    }

    pub async fn set_head(&self, address: String, index: i64) -> Result<String, sqlx::Error> {
        let result = sqlx::query(
            "UPDATE wallet.account SET head = $1 WHERE address = $2 RETURNING name",
        )
        .bind(index)
        .bind(address)
        .fetch_one(&self.pool)
        .await?
        .get(0);
        Ok(result)
    }

    pub async fn get_many_need_scan(&self) -> Result<Vec<Account>, sqlx::Error> {
        let result = sqlx::query_as("SELECT * FROM wallet.account WHERE need_scan = true")
            .fetch_all(&self.pool)
            .await?;
        Ok(result)
    }

    pub async fn insert_compact_block(&self, block: InnerBlock) -> Result<i64, sqlx::Error> {
        let result = sqlx::query(
            "INSERT INTO wallet.blocks (hash, sequence, transactions) VALUES ($1, $2, $3) RETURNING sequence"
        )
        .bind(block.hash.clone())
        .bind(block.sequence)
        .bind(block.transactions)
        .fetch_one(&self.pool)
        .await?.get(0);
        Ok(result)
    }

    pub async fn get_compact_blocks(
        &self,
        start: i64,
        end: i64,
    ) -> Result<Vec<InnerBlock>, sqlx::Error> {
        let result =
            sqlx::query_as("SELECT * FROM wallet.blocks WHERE sequence >= $1 AND sequence <= $2")
                .bind(start)
                .bind(end)
                .fetch_all(&self.pool)
                .await?;
        Ok(result)
    }

    pub async fn get_compact_transactions(
        &self,
        block_hash: String,
    ) -> Result<Vec<DBTransaction>, sqlx::Error> {
        let result =
            sqlx::query_as("SELECT hash, serialized_notes FROM wallet.txs WHERE block_hash = $1")
                .bind(block_hash)
                .fetch_all(&self.pool)
                .await?;
        Ok(result)
    }

    pub async fn insert_first_seen(&self, address: String) -> Result<(), sqlx::Error> {
        let result = sqlx::query("INSERT INTO wallet.firstseen (address) VALUES ($1)")
            .bind(address)
            .fetch_one(&self.pool)
            .await?
            .get(0);
        Ok(result)
    }

    pub async fn get_unpaid_addresses(&self) -> Result<Vec<BonusAddress>, sqlx::Error> {
        let result =
            sqlx::query_as("SELECT address, paid FROM wallet.firstseen WHERE paid = False")
                .fetch_all(&self.pool)
                .await?;
        Ok(result)
    }

    pub async fn update_firstseen_status(&self, address: String) -> Result<(), sqlx::Error> {
        let result = sqlx::query("UPDATE wallet.firstseen SET paid = $1 WHERE address = $2")
            .bind(true)
            .bind(address)
            .fetch_one(&self.pool)
            .await?
            .get(0);
        Ok(result)
    }
}

#[async_trait::async_trait]
impl DBHandler for PgHandler {
    fn from_config(config: &crate::config::DbConfig) -> Self {
        let url = config.server_url();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .disable_statement_logging()
            .clone();
        let pool = block_on(async { PgPool::connect_with(options).await.unwrap() });
        Self::new(pool)
    }

    async fn save_account(&self, account: Account, _worker_id: u32) -> Result<String, OreoError> {
        let _ = self.insert_first_seen(account.address.clone()).await;
        let old_account = self.get_one(account.address.clone()).await;
        match old_account {
            Ok(_) => Err(OreoError::Duplicate(account.address)),
            Err(e) => match e {
                sqlx::Error::RowNotFound => {
                    self.insert(account).await.map_err(|_| OreoError::DBError)
                }
                _ => Err(OreoError::DBError),
            },
        }
    }

    async fn get_account(&self, address: String) -> Result<Account, OreoError> {
        self.get_one(address.clone()).await.map_err(|e| match e {
            sqlx::Error::RowNotFound => OreoError::NoImported(address),
            _ => OreoError::DBError,
        })
    }

    async fn remove_account(&self, address: String) -> Result<String, OreoError> {
        self.delete(address.clone()).await.map_err(|e| match e {
            sqlx::Error::RowNotFound => OreoError::NoImported(address),
            _ => OreoError::DBError,
        })
    }

    async fn update_scan_status(
        &self,
        address: String,
        new_status: bool,
    ) -> Result<String, OreoError> {
        match self.get_one(address.clone()).await {
            Ok(account) => self
                .set_scan(account.address, new_status)
                .await
                .map_err(|e| match e {
                    sqlx::Error::RowNotFound => OreoError::NoImported(address),
                    _ => OreoError::DBError,
                }),
            Err(_) => Err(OreoError::NoImported(address)),
        }
    }

    async fn set_head(
        &self,
        address: String,
        index: i64,
    ) -> Result<String, OreoError> {
        match self.get_one(address.clone()).await {
            Ok(account) => self
                .set_head(account.address, index)
                .await
                .map_err(|e| match e {
                    sqlx::Error::RowNotFound => OreoError::NoImported(address),
                    _ => OreoError::DBError,
                }),
            Err(_) => Err(OreoError::NoImported(address)),
        }
    }


    async fn get_scan_accounts(&self) -> Result<Vec<Account>, OreoError> {
        self.get_many_need_scan()
            .await
            .map_err(|_| OreoError::DBError)
    }

    async fn save_blocks(&self, blocks: Vec<InnerBlock>) -> Result<(), OreoError> {
        let transaction = self.pool.begin().await.unwrap();
        for block in blocks {
            let _ = self.insert_compact_block(block).await;
        }
        transaction.rollback().await.unwrap();
        Ok(())
    }

    async fn get_blocks(&self, start: i64, end: i64) -> Result<Vec<InnerBlock>, OreoError> {
        let blocks = self
            .get_compact_blocks(start, end)
            .await
            .map_err(|_| OreoError::DBError)?;
        match blocks.len() as i64 == (end - start + 1) {
            true => Ok(blocks),
            false => Err(OreoError::DBError),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use constants::{MAINNET_GENESIS_HASH, MAINNET_GENESIS_SEQUENCE};
    use sqlx::types::Json;
    use sqlx_db_tester::TestPg;

    use crate::{address_to_name, Account, DBHandler, DBTransaction, InnerBlock};

    use super::PgHandler;

    const VK: &str = "4ae4eb9606ba57b3b17a444100a9ac6453cd67e6fe4c860e63a2e18b1200978ab5ecce68e8639d5016cbe73b0ea9a3c8e906fc881af2e9ccfa7a7b63fb73d555";
    const IN_VK: &str = "4a08bec0ec5a471352f340d737e4b3baec2aec8d0a2e12201d92d8ad71aadd07";
    const OUT_VK: &str = "cee4ff41d7d8da5eedc6493134981eaad7b26a8b0291a4eac9ba95090fa47bf7";
    const ADDRESS: &str = "d63ba13d7c35caf942c64d5139b948b885ec931977a3f248c13e7f3c1bd0aa64";

    fn get_tdb() -> TestPg {
        TestPg::new(
            "postgres://postgres:postgres@localhost:5432".to_string(),
            Path::new("../../migrations"),
        )
    }

    fn get_test_account() -> Account {
        Account {
            name: address_to_name(ADDRESS),
            create_head: None,
            create_hash: None,
            head: MAINNET_GENESIS_SEQUENCE,
            hash: MAINNET_GENESIS_HASH.to_string(),
            in_vk: IN_VK.to_string(),
            out_vk: OUT_VK.to_string(),
            vk: VK.to_string(),
            address: ADDRESS.to_string(),
            need_scan: false,
        }
    }

    fn get_test_block() -> InnerBlock {
        InnerBlock {
            hash: "dd6653ad5ec58e6174586d8a54e6c60731520d0c3b41c2e3266a05965cad0dae".to_string(),
            sequence: 10,
            transactions: Json(vec![DBTransaction {
                hash: "dd6653ad5ec58e6174586d8a54e6c60731520d0c3b41c2e3266a05965cad0da1".to_string(),
                serialized_notes: vec!["dd6653ad5ec58e6174586d8a54e6c60731520d0c3b41c2e3266a05965cad0daedd6653ad5ec58e6174586d8a54e6c60731520d0c3b41c2e3266a05965cad0dae1".to_string()],
            }, DBTransaction {
                hash: "dd6653ad5ec58e6174586d8a54e6c60731520d0c3b41c2e3266a05965cad0d2".to_string(),
                serialized_notes: vec!["dd6653ad5ec58e6174586d8a54e6c60731520d0c3b41c2e3266a05965cad0daedd6653ad5ec58e6174586d8a54e6c60731520d0c3b41c2e3266a05965cad0dae2".to_string()],
            }]),
        }
    }

    #[tokio::test]
    async fn save_account_should_work_pg() {
        let tdb = get_tdb();
        let pool = tdb.get_pool().await;
        let pg_handler = PgHandler::new(pool);
        let account = get_test_account();
        let saved = pg_handler.save_account(account, 0).await;
        assert!(saved.is_ok());
        let saved = saved.unwrap();
        assert_eq!(address_to_name(&ADDRESS.to_string()), saved);
    }

    #[tokio::test]
    async fn get_account_should_work_pg() {
        let tdb = get_tdb();
        let pool = tdb.get_pool().await;
        let pg_handler = PgHandler::new(pool);
        let account = get_test_account();
        let _ = pg_handler.save_account(account.clone(), 0).await.unwrap();
        let saved = pg_handler.get_account(ADDRESS.to_string()).await;
        assert!(saved.is_ok());
        let saved = saved.unwrap();
        assert_eq!(account, saved);
    }

    #[tokio::test]
    async fn remove_account_should_work_pg() {
        let tdb = get_tdb();
        let pool = tdb.get_pool().await;
        let pg_handler = PgHandler::new(pool);
        let account = get_test_account();
        let saved = pg_handler.save_account(account.clone(), 0).await.unwrap();
        let result = pg_handler.remove_account(ADDRESS.to_string()).await;
        assert!(result.is_ok());

        // run remove once again
        let should_error = pg_handler.remove_account(saved).await;
        assert!(should_error.is_err());
    }

    #[tokio::test]
    async fn save_blocks_should_work_pg() {
        let tdb = get_tdb();
        let pool = tdb.get_pool().await;
        let pg_handler = PgHandler::new(pool);
        let block = get_test_block();
        let result = pg_handler.save_blocks(vec![block]).await;
        println!("{:?}", result);
    }

    #[tokio::test]
    async fn get_blocks_should_work_pg() {
        let tdb = get_tdb();
        let pool = tdb.get_pool().await;
        let pg_handler = PgHandler::new(pool);
        let block = get_test_block();
        let x = pg_handler.save_blocks(vec![block]).await;
        println!("saved: {:?}", x);

        let blocks = pg_handler.get_blocks(9, 11).await.unwrap();
        println!("get blocks test: {:?}", blocks);
    }

    #[tokio::test]
    async fn get_firstseen_should_work_pg() {
        let tdb = get_tdb();
        let pool = tdb.get_pool().await;
        let pg_handler = PgHandler::new(pool);
        let account = get_test_account();
        let saved = pg_handler.save_account(account, 0).await;
        assert!(saved.is_ok());
        let unpaid = pg_handler.get_unpaid_addresses().await;
        assert!(unpaid.is_ok());
        let unpaid = unpaid.unwrap();
        assert!(unpaid.len() == 1);
        println!("{:?}", unpaid);
    }
}
