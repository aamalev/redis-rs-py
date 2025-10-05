use std::collections::HashMap;

use crate::{command::Params, error};
use async_trait::async_trait;

use redis::Cmd;

#[async_trait]
pub trait Pool {
    async fn execute(&self, cmd: Cmd, params: Params) -> Result<redis::Value, error::RedisError>;

    fn status(&self) -> HashMap<&str, redis::Value>;
}

pub struct ClosedPool;

#[async_trait]
impl Pool for ClosedPool {
    async fn execute(&self, _cmd: Cmd, _params: Params) -> Result<redis::Value, error::RedisError> {
        Err(error::RedisError::not_initialized())
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Boolean(true));
        result
    }
}
