use std::collections::HashMap;

use crate::error;
use crate::types;
use async_trait::async_trait;

use redis::aio::ConnectionLike;

pub struct Connection {
    pub(crate) c: Box<dyn ConnectionLike + Send>,
}

impl ConnectionLike for Connection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        Box::pin(async move { self.c.req_packed_command(cmd).await })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        Box::pin(async move { self.c.req_packed_commands(cmd, offset, count).await })
    }

    fn get_db(&self) -> i64 {
        self.c.get_db()
    }
}

#[async_trait]
pub trait Pool {
    async fn get_connection(&self) -> Result<Connection, error::RedisError>;

    async fn execute(
        &self,
        cmd: &str,
        args: Vec<types::Arg>,
    ) -> Result<redis::Value, error::RedisError>;

    fn status(&self) -> HashMap<&str, redis::Value>;
}

pub struct ClosedPool;

#[async_trait]
impl Pool for ClosedPool {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        Err(error::RedisError::not_initialized())
    }

    async fn execute(
        &self,
        _cmd: &str,
        _args: Vec<types::Arg>,
    ) -> Result<redis::Value, error::RedisError> {
        Err(error::RedisError::not_initialized())
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Int(1));
        result
    }
}
