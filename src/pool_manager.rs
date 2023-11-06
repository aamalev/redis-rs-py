use std::{collections::HashMap, sync::Arc};

use redis::{Cmd, FromRedisValue, IntoConnectionInfo};

use crate::{
    client::Client,
    client_result_async::AsyncClientResult,
    cluster_async::Cluster,
    cluster_bb8::BB8Cluster,
    cluster_deadpool::DeadPoolCluster,
    error,
    pool::{ClosedPool, Connection, Pool},
    shards_async::AsyncShards,
    single_bb8::BB8Pool,
    single_deadpool::DeadPool,
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
    pub(crate) is_cluster: bool,
    pub(crate) initial_nodes: Vec<String>,
    pub(crate) max_size: u32,
    pub(crate) pool: Box<dyn Pool + Send + Sync>,
    pub(crate) client_id: String,
    pub(crate) features: Vec<types::Feature>,
}

impl PoolManager {
    pub fn new_cluster(initial_nodes: Vec<String>) -> Self {
        Self {
            initial_nodes,
            is_cluster: true,
            max_size: 10,
            pool: Box::new(ClosedPool),
            client_id: String::default(),
            features: vec![],
        }
    }

    pub fn new(addr: &str) -> Self {
        Self {
            initial_nodes: vec![addr.to_string()],
            is_cluster: false,
            max_size: 10,
            pool: Box::new(ClosedPool),
            client_id: String::default(),
            features: vec![],
        }
    }

    pub async fn init(&mut self) -> Result<&Self, error::RedisError> {
        let nodes = self.initial_nodes.clone();
        let ms = self.max_size;
        let is_cluster = self.is_cluster;
        if is_cluster {
            self.pool = match self.features.as_slice() {
                [types::Feature::BB8, ..] => Box::new(BB8Cluster::new(nodes, ms).await),
                [types::Feature::DeadPool, ..] => Box::new(DeadPoolCluster::new(nodes, ms)),
                [types::Feature::Shards, ..] => {
                    Box::new(AsyncShards::new(nodes, ms, Some(true)).await?)
                }
                _ => Box::new(Cluster::new(nodes, ms).await?),
            };
        } else {
            let info = nodes.clone().remove(0).into_connection_info()?;
            self.pool = match self.features.as_slice() {
                [types::Feature::BB8, ..] => Box::new(BB8Pool::new(info, ms).await?),
                [types::Feature::DeadPool, ..] => Box::new(DeadPool::new(info, ms).await?),
                [types::Feature::Shards, ..] => {
                    Box::new(AsyncShards::new(nodes, ms, Some(false)).await?)
                }
                _ => Box::new(Node::new(info, ms).await?),
            };
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
            .map(|s| redis::Value::Data(s.as_bytes().to_vec()))
            .collect();
        result.insert("initial_nodes", redis::Value::Bulk(initial_nodes));
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
