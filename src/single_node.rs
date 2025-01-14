use std::collections::HashMap;

use async_trait::async_trait;
use redis::{
    aio::{ConnectionLike, ConnectionManager, ConnectionManagerConfig},
    Client, Cmd, ConnectionInfo,
};

use crate::{
    config::Config,
    error,
    pool::{Connection, Pool},
};

#[derive(Clone)]
pub struct Node {
    pub info: ConnectionInfo,
    manager: ConnectionManager,
    pub id: Option<String>,
}

impl Node {
    pub async fn new(info: ConnectionInfo, config: Config) -> Result<Self, error::RedisError> {
        let client = Client::open(info.clone())?;
        let mut cfg = ConnectionManagerConfig::new();
        if let Some(max_delay) = config.max_delay {
            cfg = cfg.set_max_delay(max_delay);
        }
        let manager = ConnectionManager::new_with_config(client, cfg).await?;
        Ok(Self {
            manager,
            info,
            id: None,
        })
    }
}

#[async_trait]
impl Pool for Node {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let c = self.manager.clone();
        Ok(Connection { c: Box::new(c) })
    }

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let mut c = self.manager.clone();
        let value = c.req_packed_command(&cmd).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Boolean(false));
        result.insert("impl", redis::Value::SimpleString("client_async".into()));
        result.insert("cluster", redis::Value::Boolean(false));
        result
    }
}

impl ConnectionLike for Node {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        self.manager.req_packed_command(cmd)
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        cmd: &'a redis::Pipeline,
        offset: usize,
        count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        self.manager.req_packed_commands(cmd, offset, count)
    }

    fn get_db(&self) -> i64 {
        self.manager.get_db()
    }
}
