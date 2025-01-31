use std::{collections::HashMap, time::Duration};

use async_trait::async_trait;
use redis::{
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig},
    Client, Cmd, ConnectionInfo,
};

use crate::{command::Params, config::Config, error, pool::Pool};

type PoolManager = bb8_redis::RedisConnectionManager;

#[derive(Clone)]
pub struct Node {
    pub info: ConnectionInfo,
    single: ConnectionManager,
    pool: bb8::Pool<PoolManager>,
    pub id: Option<String>,
}

impl Node {
    pub async fn new(info: ConnectionInfo, config: Config) -> Result<Self, error::RedisError> {
        let client = Client::open(info.clone())?;
        let mut cfg = ConnectionManagerConfig::new();
        if let Some(max_delay) = config.max_delay {
            cfg = cfg.set_max_delay(max_delay);
        }
        let single = ConnectionManager::new_with_config(client, cfg).await?;

        let pool = bb8::Pool::builder()
            .max_size(config.max_size)
            .min_idle(0)
            .idle_timeout(Some(Duration::new(60, 0)))
            .build(PoolManager::new(info.clone())?)
            .await?;

        Ok(Self {
            single,
            pool,
            info,
            id: None,
        })
    }
}

#[async_trait]
impl Pool for Node {
    async fn execute(&self, cmd: Cmd, params: Params) -> Result<redis::Value, error::RedisError> {
        let value = if params.block {
            let mut c = self.pool.get().await?;
            c.req_packed_command(&cmd).await?
        } else {
            let mut c = self.single.clone();
            c.req_packed_command(&cmd).await?
        };
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Boolean(false));
        result.insert("impl", redis::Value::SimpleString("client_async".into()));
        result.insert("cluster", redis::Value::Boolean(false));

        let state = self.pool.state();
        result.insert(
            "connections",
            redis::Value::Int(state.connections as i64 + 1),
        );
        result.insert(
            "idle_connections",
            redis::Value::Int(state.idle_connections as i64),
        );
        result
    }
}
