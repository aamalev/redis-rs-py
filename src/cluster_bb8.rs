use std::collections::HashMap;

use crate::{
    error,
    pool::{Connection, Pool},
};
use async_trait::async_trait;
use bb8_redis_cluster::RedisConnectionManager;
use redis::{aio::ConnectionLike, Cmd};

pub struct BB8Cluster {
    pool: bb8::Pool<RedisConnectionManager>,
}

impl BB8Cluster {
    pub async fn new(initial_nodes: Vec<String>, max_size: u32) -> Self {
        let manager = RedisConnectionManager::new(initial_nodes).unwrap();
        let pool = bb8::Pool::builder()
            .max_size(max_size)
            .build(manager)
            .await
            .unwrap();
        Self { pool }
    }
}

#[async_trait]
impl Pool for BB8Cluster {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let c = self.pool.get().await?;
        Ok(Connection {
            c: Box::new(c.to_owned()),
        })
    }

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let mut conn = self.pool.get().await?;
        let value = conn.req_packed_command(&cmd).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Int(0));
        result.insert("impl", redis::Value::Data("bb8_cluster".into()));
        result.insert("cluster", redis::Value::Int(1));
        let state = self.pool.state();
        result.insert("connections", redis::Value::Int(state.connections.into()));
        result.insert(
            "idle_connections",
            redis::Value::Int(state.idle_connections.into()),
        );
        result
    }
}
