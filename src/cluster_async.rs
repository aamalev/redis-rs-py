use std::collections::HashMap;

use crate::{cluster_bb8::ClusterManager, command::Params, error, pool::Pool};
use async_trait::async_trait;
use redis::{aio::ConnectionLike, cluster::ClusterClient, Cmd, IntoConnectionInfo};

pub struct Cluster {
    pool: bb8::Pool<ClusterManager>,
    connection: redis::cluster_async::ClusterConnection,
}

impl Cluster {
    pub async fn new<T>(initial_nodes: Vec<T>, max_size: u32) -> Result<Self, error::RedisError>
    where
        T: IntoConnectionInfo + Clone,
    {
        let client = ClusterClient::new(initial_nodes.clone())?;
        let connection = client.get_async_connection().await?;

        let pool = bb8::Pool::builder()
            .max_size(max_size)
            .build(ClusterManager::new(initial_nodes)?)
            .await?;

        Ok(Self { pool, connection })
    }
}

#[async_trait]
impl Pool for Cluster {
    async fn execute_params(
        &self,
        cmd: Cmd,
        params: Params,
    ) -> Result<redis::Value, error::RedisError> {
        let value = if params.block {
            let mut conn = self.pool.get().await?;
            conn.req_packed_command(&cmd).await?
        } else {
            let mut conn = self.connection.clone();
            conn.req_packed_command(&cmd).await?
        };
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Boolean(false));
        result.insert("impl", redis::Value::SimpleString("cluster_async".into()));
        result.insert("cluster", redis::Value::Boolean(true));

        let state = self.pool.state();
        result.insert(
            "connections",
            redis::Value::Int(state.connections as i64 + 1),
        );
        result.insert(
            "idle_connections",
            redis::Value::Int(state.idle_connections as i64),
        );
        result
    }
}
