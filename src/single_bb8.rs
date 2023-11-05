use std::{collections::HashMap, time::Duration};

use crate::{
    error,
    pool::{Connection, Pool},
    types,
};
use async_trait::async_trait;
use redis::{aio::ConnectionLike, ConnectionInfo};

type Manager = bb8_redis::RedisMultiplexedConnectionManager;
type InnerPool = bb8::Pool<Manager>;

#[derive(Clone)]
pub struct BB8Pool {
    pub info: ConnectionInfo,
    pool: InnerPool,
    pub id: Option<String>,
}

impl BB8Pool {
    pub async fn new(info: ConnectionInfo, max_size: u32) -> Result<Self, error::RedisError> {
        let manager = Manager::new(info.clone())?;
        let pool = bb8::Pool::builder()
            .max_size(max_size)
            .idle_timeout(Some(Duration::new(60, 0)))
            .build(manager)
            .await?;
        Ok(Self {
            pool,
            info,
            id: None,
        })
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

impl ConnectionLike for BB8Pool {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        Box::pin(async move {
            let mut c = self.pool.get().await.map_err(error::RedisError::from)?;
            c.req_packed_command(cmd).await
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        Box::pin(async move {
            let mut c = self.pool.get().await.map_err(error::RedisError::from)?;
            c.req_packed_commands(cmd, offset, count).await
        })
    }

    fn get_db(&self) -> i64 {
        self.info.redis.db
    }
}
