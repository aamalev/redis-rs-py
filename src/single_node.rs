use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use redis::{
    aio::{ConnectionLike, MultiplexedConnection},
    Client, Cmd, ConnectionInfo,
};
use tokio::sync::Semaphore;

use crate::{
    error,
    pool::{Connection, Pool},
};

#[derive(Clone)]
pub struct Node {
    pub info: ConnectionInfo,
    connection: MultiplexedConnection,
    semaphore: Arc<Semaphore>,
    pub id: Option<String>,
}

impl Node {
    pub async fn new(info: ConnectionInfo, max_size: u32) -> Result<Self, error::RedisError> {
        let client = Client::open(info.clone())?;
        let semaphore = Arc::new(Semaphore::new(max_size as usize));
        let connection = client.get_multiplexed_async_connection().await?;
        Ok(Self {
            connection,
            semaphore,
            info,
            id: None,
        })
    }
}

#[async_trait]
impl Pool for Node {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let _ = self
            .semaphore
            .acquire()
            .await
            .map_err(error::RedisError::from)?;
        let c = self.connection.clone();
        Ok(Connection { c: Box::new(c) })
    }

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let _ = self
            .semaphore
            .acquire()
            .await
            .map_err(error::RedisError::from)?;
        let mut c = self.connection.clone();
        let value = c.req_packed_command(&cmd).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Int(0));
        result.insert("impl", redis::Value::Data("client_async".into()));
        result.insert("cluster", redis::Value::Int(0));
        result
    }
}

impl ConnectionLike for Node {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        Box::pin(async move {
            let _ = self
                .semaphore
                .acquire()
                .await
                .map_err(error::RedisError::from)?;
            let mut c = self.connection.clone();
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
            let _ = self
                .semaphore
                .acquire()
                .await
                .map_err(error::RedisError::from)?;
            let mut c = self.connection.clone();
            c.req_packed_commands(cmd, offset, count).await
        })
    }

    fn get_db(&self) -> i64 {
        self.connection.get_db()
    }
}
