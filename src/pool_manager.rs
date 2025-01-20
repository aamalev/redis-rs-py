use std::{collections::HashMap, sync::Arc};

use redis::{Cmd, FromRedisValue, Value};

use crate::{
    client_async::Client,
    client_result_async::AsyncClientResult,
    cluster_async::Cluster,
    cluster_bb8::BB8Cluster,
    config::Config,
    error,
    pool::{ClosedPool, Connection, Pool},
    shards_async::AsyncShards,
    single_bb8::BB8Pool,
    single_node::Node,
};

impl From<PoolManager> for Client {
    fn from(value: PoolManager) -> Self {
        let client_id = value.config.client_id.clone();
        let ac = AsyncClientResult {
            cm: Arc::new(tokio::sync::RwLock::new(value)),
        };
        Self { cr: ac, client_id }
    }
}

pub struct PoolManager {
    pub(crate) pool: Box<dyn Pool + Send + Sync>,
    pub(crate) config: Config,
}

impl PoolManager {
    pub fn new(config: Config) -> Result<Self, error::RedisError> {
        Ok(Self {
            pool: Box::new(ClosedPool),
            config,
        })
    }

    pub async fn init(&mut self) -> Result<(), error::RedisError> {
        let mut nodes = self.config.initial_nodes.clone();
        let ms = self.config.max_size;
        self.pool = match (self.config.cluster, self.config.shards, self.config.bb8) {
            (None, _, _) | (_, true, _) => Box::new(AsyncShards::new(self.config.clone()).await?),
            (Some(true), false, false) => Box::new(Cluster::new(nodes, ms).await?),
            (Some(false), false, false) => {
                Box::new(Node::new(nodes.remove(0), self.config.clone()).await?)
            }
            (Some(true), false, true) => Box::new(BB8Cluster::new(nodes, ms).await),
            (Some(false), false, true) => Box::new(BB8Pool::new(nodes.remove(0), ms).await?),
        };
        Ok(())
    }

    pub async fn close(&mut self) {
        self.pool = Box::new(ClosedPool);
    }

    pub fn status(&self) -> HashMap<String, redis::Value> {
        let mut result = self.pool.status();
        let initial_nodes = self
            .config
            .initial_nodes
            .iter()
            .map(|s| {
                if let Some(username) = s.redis.username.clone() {
                    result.insert("username", redis::Value::SimpleString(username));
                }
                if s.redis.password.is_some() {
                    result.insert("auth", redis::Value::Boolean(true));
                }
                redis::Value::SimpleString(s.addr.to_string())
            })
            .collect();
        result.insert("initial_nodes", redis::Value::Array(initial_nodes));
        result.insert("max_size", redis::Value::Int(self.config.max_size as i64));
        result
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    pub async fn execute<T: FromRedisValue>(&self, cmd: Cmd) -> Result<T, error::RedisError> {
        let value = self.pool.execute(cmd).await?;
        if let Value::ServerError(err) = value {
            Err(redis::RedisError::from(err))?
        } else {
            let result: T = FromRedisValue::from_redis_value(&value)?;
            Ok(result)
        }
    }

    pub async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        self.pool.get_connection().await
    }
}

#[cfg(test)]
mod tests {
    use crate::config::Config;

    use super::PoolManager;

    #[test]
    fn status() {
        let pm = PoolManager::new(Config::default()).unwrap();
        let result = pm.status();
        assert_eq!(result.len(), 3);
    }
}
