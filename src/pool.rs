use std::collections::HashMap;

use crate::{command::Params, error};
use async_trait::async_trait;

use redis::aio::ConnectionLike;
use redis::Cmd;

pub struct Connection {
    pub(crate) c: Box<dyn ConnectionLike + Send>,
}

impl ConnectionLike for Connection {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        Box::pin(self.c.req_packed_command(cmd))
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        Box::pin(self.c.req_packed_commands(cmd, offset, count))
    }

    fn get_db(&self) -> i64 {
        self.c.get_db()
    }
}

#[async_trait]
pub trait Pool {
    async fn execute_params(
        &self,
        cmd: Cmd,
        params: Params,
    ) -> Result<redis::Value, error::RedisError>;

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let params = Params::from(&cmd);
        self.execute_params(cmd, params).await
    }

    fn status(&self) -> HashMap<&str, redis::Value>;
}

pub struct ClosedPool;

#[async_trait]
impl Pool for ClosedPool {
    async fn execute_params(
        &self,
        _cmd: Cmd,
        _params: Params,
    ) -> Result<redis::Value, error::RedisError> {
        Err(error::RedisError::not_initialized())
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Boolean(true));
        result
    }
}
