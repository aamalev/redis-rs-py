use crate::{
    error,
    pool::{Connection, Pool},
};
use async_trait::async_trait;
use deadpool_redis::{Config, CreatePoolError, PoolError, Runtime};
use redis::{aio::ConnectionLike, Cmd, ConnectionInfo};
use std::collections::HashMap;

#[derive(Clone)]
pub struct DeadPool {
    pub info: ConnectionInfo,
    pool: deadpool_redis::Pool,
    pub id: Option<String>,
}

impl From<CreatePoolError> for error::RedisError {
    fn from(e: CreatePoolError) -> Self {
        error::RedisError::PoolError(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "",
            e.to_string(),
        )))
    }
}

impl From<PoolError> for error::RedisError {
    fn from(e: PoolError) -> Self {
        match e {
            PoolError::Backend(e) => error::RedisError::PoolError(e),
            _ => todo!(),
        }
    }
}

impl DeadPool {
    pub async fn new(info: ConnectionInfo, max_size: u32) -> Result<Self, error::RedisError> {
        let cfg = Config::from_connection_info(info.clone());
        let pool = cfg.create_pool(Some(Runtime::Tokio1))?;
        pool.resize(max_size as usize);
        Ok(Self {
            pool,
            info,
            id: None,
        })
    }
}

#[async_trait]
impl Pool for DeadPool {
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
        result.insert("impl", redis::Value::Data("deadpool_redis".into()));
        result.insert("cluster", redis::Value::Int(1));
        result
    }
}

impl ConnectionLike for DeadPool {
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
