use std::collections::HashMap;

use crate::{
    error,
    pool::{Connection, Pool},
    types,
};
use async_trait::async_trait;
use redis::{aio::ConnectionLike, cluster::ClusterClient};

pub struct Cluster {
    client: ClusterClient,
}

impl Cluster {
    pub fn new(initial_nodes: Vec<String>, _max_size: u32) -> Self {
        let client = ClusterClient::new(initial_nodes).unwrap();
        Self { client }
    }
}

#[async_trait]
impl Pool for Cluster {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let c = self.client.get_async_connection().await?;
        Ok(Connection { c: Box::new(c) })
    }

    async fn execute(
        &self,
        cmd: &str,
        args: Vec<types::Arg>,
    ) -> Result<redis::Value, error::RedisError> {
        let mut conn = self.client.get_async_connection().await?;
        let value = conn.req_packed_command(redis::cmd(cmd).arg(&args)).await?;
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
