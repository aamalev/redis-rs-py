use crate::{
    error,
    pool::{Connection, Pool},
};
use async_trait::async_trait;
use deadpool_redis_cluster::{Config, PoolError, Runtime};
use redis::{aio::ConnectionLike, Cmd};
use std::collections::HashMap;

pub struct DeadPoolCluster {
    pool: deadpool_redis_cluster::Pool,
}

impl From<PoolError> for error::RedisError {
    fn from(e: PoolError) -> Self {
        match e {
            PoolError::Backend(e) => error::RedisError::PoolError(e),
            _ => todo!(),
        }
    }
}

impl DeadPoolCluster {
    pub fn new(initial_nodes: Vec<String>, max_size: u32) -> Self {
        let cfg = Config::from_urls(initial_nodes);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .expect("Error with redis pool");
        pool.resize(max_size as usize);
        Self { pool }
    }
}

#[async_trait]
impl Pool for DeadPoolCluster {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let c = self.pool.get().await?;
        Ok(Connection { c: Box::new(c) })
    }

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let mut conn = self.pool.get().await.expect("Error with redis pool");
        let value = conn.req_packed_command(&cmd).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Int(0));
        result.insert("impl", redis::Value::Data("deadpool_redis_cluster".into()));
        result.insert("cluster", redis::Value::Int(1));
        result
    }
}
