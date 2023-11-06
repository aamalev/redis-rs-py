use std::collections::HashMap;

use crate::{
    error,
    pool::{Connection, Pool},
};
use async_trait::async_trait;
use redis::{aio::ConnectionLike, cluster::ClusterClient, Cmd};
use tokio::sync::Semaphore;

pub struct Cluster {
    semaphore: Semaphore,
    connection: redis::cluster_async::ClusterConnection,
}

impl Cluster {
    pub async fn new(initial_nodes: Vec<String>, max_size: u32) -> Result<Self, error::RedisError> {
        let client = ClusterClient::new(initial_nodes).unwrap();
        let semaphore = Semaphore::new(max_size as usize);
        let connection = client.get_async_connection().await?;
        Ok(Self {
            semaphore,
            connection,
        })
    }
}

#[async_trait]
impl Pool for Cluster {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let _ = self.semaphore.acquire().await?;
        let c = self.connection.clone();
        Ok(Connection { c: Box::new(c) })
    }

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let _ = self.semaphore.acquire().await?;
        let mut conn = self.connection.clone();
        let value = conn.req_packed_command(&cmd).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Int(0));
        result.insert("impl", redis::Value::Data("cluster_async".into()));
        result.insert("cluster", redis::Value::Int(1));
        result
    }
}
