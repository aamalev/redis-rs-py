use std::{collections::HashMap, sync::Arc};

use redis::{FromRedisValue, IntoConnectionInfo};

use crate::{
    client::Client,
    client_result_async::AsyncClientResult,
    cluster::Cluster,
    cluster_bb8::BB8Cluster,
    cluster_deadpool::DeadPoolCluster,
    error,
    pool::{ClosedPool, Connection, Pool},
    single_bb8::BB8Pool,
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
    pub(crate) pool_type: String,
    pub(crate) pool: Box<dyn Pool + Send + Sync>,
    pub(crate) client_id: String,
}

impl PoolManager {
    pub fn new_cluster(initial_nodes: Vec<String>) -> Self {
        Self {
            initial_nodes,
            is_cluster: true,
            max_size: 10,
            pool_type: "bb8".to_string(),
            pool: Box::new(ClosedPool),
            client_id: String::default(),
        }
    }

    pub fn new(addr: &str) -> Self {
        Self {
            initial_nodes: vec![addr.to_string()],
            is_cluster: false,
            max_size: 10,
            pool_type: "bb8".to_string(),
            pool: Box::new(ClosedPool),
            client_id: String::default(),
        }
    }

    pub async fn init(&mut self) -> &Self {
        let nodes = self.initial_nodes.clone();
        let ms = self.max_size;
        let is_cluster = self.is_cluster;
        if is_cluster {
            self.pool = match self.pool_type.as_str() {
                "bb8" => Box::new(BB8Cluster::new(nodes, ms).await),
                "dp" => Box::new(DeadPoolCluster::new(nodes, ms)),
                _ => Box::new(Cluster::new(nodes, ms)),
            };
        } else {
            let info = nodes.clone().remove(0).into_connection_info().unwrap();
            self.pool = match self.pool_type.as_str() {
                _ => Box::new(BB8Pool::new(info, ms).await.unwrap()),
            };
        };
        self
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

    pub async fn execute<T: FromRedisValue>(
        &self,
        cmd: &str,
        args: Vec<types::Arg>,
    ) -> Result<T, error::RedisError> {
        let value = self.pool.execute(cmd.to_uppercase().as_str(), args).await?;
        let result: T = FromRedisValue::from_redis_value(&value)?;
        Ok(result)
    }

    pub async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        self.pool.get_connection().await
    }
}
