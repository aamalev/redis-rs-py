use std::collections::HashMap;

use crate::{
    error,
    pool::{Connection, Pool},
    types,
};
use async_trait::async_trait;
use bb8_redis::RedisMultiplexedConnectionManager;
use redis::aio::ConnectionLike;

pub struct BB8Pool {
    pool: bb8::Pool<RedisMultiplexedConnectionManager>,
}

impl BB8Pool {
    pub async fn new(initial_nodes: Vec<String>, max_size: u32) -> Self {
        let addr = initial_nodes.get(0).unwrap().to_string();
        let manager = RedisMultiplexedConnectionManager::new(addr).unwrap();
        let pool = bb8::Pool::builder()
            .max_size(max_size)
            .build(manager)
            .await
            .unwrap();
        Self { pool }
    }
}

#[async_trait]
impl Pool for BB8Pool {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let c = self.pool.get().await?;
        Ok(Connection {
            c: Box::new(c.to_owned()),
        })
    }

    async fn execute(
        &self,
        cmd: &str,
        args: Vec<types::Arg>,
    ) -> Result<redis::Value, error::RedisError> {
        let mut conn = self.pool.get().await?;
        let value = conn.req_packed_command(redis::cmd(cmd).arg(&args)).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("impl", redis::Value::Data("bb8_redis".into()));
        result.insert("closed", redis::Value::Int(0));
        result.insert("cluster", redis::Value::Int(0));
        let state = self.pool.state();
        result.insert("connections", redis::Value::Int(state.connections.into()));
        result.insert(
            "idle_connections",
            redis::Value::Int(state.idle_connections.into()),
        );
        result
    }
}
