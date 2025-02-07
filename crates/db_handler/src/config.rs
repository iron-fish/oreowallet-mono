use anyhow::anyhow;
use anyhow::Result;
use serde::{Deserialize, Serialize};
use tracing::info;

use std::{fs, path::Path};

use crate::DBType;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DbConfig {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: String,
    pub dbname: String,
    #[serde(default = "default_pool_size")]
    pub default_pool_size: u32,
    pub protocol: String,
}

fn default_pool_size() -> u32 {
    50
}

impl DbConfig {
    pub fn server_url(&self) -> String {
        let db_name = if self.dbname.is_empty() {
            "".to_string()
        } else {
            format!("/{}", self.dbname)
        };
        if self.password.is_empty() {
            format!(
                "{}://{}@{}:{}{}",
                self.protocol, self.user, self.host, self.port, db_name
            )
        } else {
            format!(
                "{}://{}:{}@{}:{}{}",
                self.protocol, self.user, self.password, self.host, self.port, db_name
            )
        }
    }

    pub fn url(&self) -> String {
        format!("{}/{}", self.server_url(), self.dbname)
    }

    pub fn protocol(&self) -> DBType {
        match self.protocol.as_str() {
            "postgres" => DBType::Postgres,
            "redis" => DBType::Redis,
            _ => DBType::Unknown,
        }
    }
}

impl DbConfig {
    pub fn load(filename: impl AsRef<Path>) -> Result<Self> {
        let config = fs::read_to_string(filename.as_ref())
            .map_err(|_| anyhow!("Failed to read db config"))?;
        info!("DB config: {:?}", config);
        serde_yaml::from_str(&config).map_err(|_| anyhow!("Failed to parse db config"))
    }
}

#[cfg(test)]
mod tests {
    use super::DbConfig;

    #[test]
    fn redis_config_should_be_loaded() {
        let config = DbConfig::load("./fixtures/redis-config.yml");
        assert_eq!(
            config.unwrap(),
            DbConfig {
                host: "localhost".to_string(),
                port: 6379,
                user: "".to_string(),
                password: "".to_string(),
                dbname: "oreowallet".to_string(),
                default_pool_size: 200,
                protocol: "redis".to_string()
            }
        );
    }

    #[test]
    fn postgres_config_should_be_loaded() {
        let config = DbConfig::load("./fixtures/postgres-config.yml");
        let config = config.unwrap();
        assert_eq!(
            config,
            DbConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                dbname: "oreowallet".to_string(),
                default_pool_size: 200,
                protocol: "postgres".to_string()
            }
        );
    }
}
