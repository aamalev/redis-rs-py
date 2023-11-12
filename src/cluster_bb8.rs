use std::collections::HashMap;

use crate::{
    error,
    pool::{Connection, Pool},
};
use async_trait::async_trait;
use redis::{
    aio::ConnectionLike, cluster::ClusterClient, cluster_async::ClusterConnection, Cmd, ErrorKind,
    IntoConnectionInfo, RedisError,
};

pub struct ClusterManager {
    pub(crate) client: ClusterClient,
}

impl ClusterManager {
    pub fn new<T>(initial_nodes: Vec<T>) -> Result<Self, RedisError>
    where
        T: IntoConnectionInfo,
    {
        let client = ClusterClient::new(initial_nodes)?;
        Ok(Self { client })
    }
}

#[async_trait]
impl bb8::ManageConnection for ClusterManager {
    type Connection = ClusterConnection;
    type Error = RedisError;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.client.get_async_connection().await
    }

    async fn is_valid(&self, conn: &mut Self::Connection) -> Result<(), Self::Error> {
        let pong: String = redis::cmd("PING").query_async(conn).await?;
        match pong.as_str() {
            "PONG" => Ok(()),
            _ => Err((ErrorKind::ResponseError, "ping request").into()),
        }
    }

    fn has_broken(&self, _: &mut Self::Connection) -> bool {
        false
    }
}

type Manager = ClusterManager;

pub struct BB8Cluster {
    pool: bb8::Pool<Manager>,
}

impl BB8Cluster {
    pub async fn new<T>(initial_nodes: Vec<T>, max_size: u32) -> Self
    where
        T: IntoConnectionInfo,
    {
        let manager = Manager::new(initial_nodes).unwrap();
        let pool = bb8::Pool::builder()
            .max_size(max_size)
            .build(manager)
            .await
            .unwrap();
        Self { pool }
    }
}

#[async_trait]
impl Pool for BB8Cluster {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let c = self.pool.get().await?;
        Ok(Connection {
            c: Box::new(c.to_owned()),
        })
    }

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let mut conn = self.pool.get().await?;
        let value = conn.req_packed_command(&cmd).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Int(0));
        result.insert("impl", redis::Value::Data("bb8_cluster".into()));
        result.insert("cluster", redis::Value::Int(1));
        let state = self.pool.state();
        result.insert("connections", redis::Value::Int(state.connections.into()));
        result.insert(
            "idle_connections",
            redis::Value::Int(state.idle_connections.into()),
        );
        result
    }
}
