use std::{collections::HashMap, sync::Arc};

use redis::{Cmd, ConnectionInfo, FromRedisValue};

use crate::{
    client::Client,
    client_result_async::AsyncClientResult,
    cluster_async::Cluster,
    cluster_bb8::BB8Cluster,
    error,
    pool::{ClosedPool, Connection, Pool},
    shards_async::AsyncShards,
    single_bb8::BB8Pool,
    single_node::Node,
    types,
};

impl From<PoolManager> for Client {
    fn from(value: PoolManager) -> Self {
        let client_id = value.client_id.clone();
        let ac = AsyncClientResult {
            cm: Arc::new(tokio::sync::RwLock::new(value)),
        };
        Self {
            cr: Box::new(ac),
            client_id,
        }
    }
}

pub struct PoolManager {
    pub(crate) is_cluster: Option<bool>,
    pub(crate) initial_nodes: Vec<ConnectionInfo>,
    pub(crate) max_size: u32,
    pub(crate) pool: Box<dyn Pool + Send + Sync>,
    pub(crate) client_id: String,
    pub(crate) features: Vec<types::Feature>,
}

impl PoolManager {
    pub fn new(initial_nodes: Vec<ConnectionInfo>) -> Result<Self, error::RedisError> {
        Ok(Self {
            initial_nodes,
            is_cluster: Some(false),
            max_size: 10,
            pool: Box::new(ClosedPool),
            client_id: String::default(),
            features: vec![],
        })
    }

    pub async fn init(&mut self) -> Result<&Self, error::RedisError> {
        let mut nodes = self.initial_nodes.clone();
        let ms = self.max_size;
        match self.is_cluster {
            None => {
                self.pool = Box::new(AsyncShards::new(nodes, ms, self.is_cluster).await?);
            }
            Some(true) => {
                self.pool = match self.features.as_slice() {
                    [types::Feature::BB8, ..] => Box::new(BB8Cluster::new(nodes, ms).await),
                    [types::Feature::Shards, ..] => {
                        Box::new(AsyncShards::new(nodes, ms, Some(true)).await?)
                    }
                    _ => Box::new(Cluster::new(nodes, ms).await?),
                };
            }
            Some(false) => {
                self.pool = match self.features.as_slice() {
                    [types::Feature::BB8, ..] => Box::new(BB8Pool::new(nodes.remove(0), ms).await?),
                    [types::Feature::Shards, ..] => {
                        Box::new(AsyncShards::new(nodes, ms, Some(false)).await?)
                    }
                    _ => Box::new(Node::new(nodes.remove(0), ms).await?),
                };
            }
        };
        Ok(self)
    }

    pub async fn close(&mut self) -> &Self {
        self.pool = Box::new(ClosedPool);
        self
    }

    pub fn status(&self) -> HashMap<String, redis::Value> {
        let mut result = self.pool.status();
        let initial_nodes = self
            .initial_nodes
            .iter()
            .map(|s| {
                if let Some(username) = s.redis.username.clone() {
                    result.insert("username", redis::Value::SimpleString(username));
                }
                if s.redis.password.is_some() {
                    result.insert("auth", redis::Value::Int(1));
                }
                redis::Value::SimpleString(s.addr.to_string())
            })
            .collect();
        result.insert("initial_nodes", redis::Value::Array(initial_nodes));
        result.insert("max_size", redis::Value::Int(self.max_size as i64));
        result
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    pub async fn execute<T: FromRedisValue>(&self, cmd: Cmd) -> Result<T, error::RedisError> {
        let value = self.pool.execute(cmd).await?;
        let result: T = FromRedisValue::from_redis_value(&value)?;
        Ok(result)
    }

    pub async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        self.pool.get_connection().await
    }
}
