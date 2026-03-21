use std::{collections::HashMap, sync::Arc};

use redis::{Cmd, FromRedisValue};

use crate::{
    client_async::Client,
    client_result_async::AsyncClientResult,
    cluster_async::Cluster,
    command::Params,
    config::Config,
    error,
    mock::MockRedis,
    node::Node,
    pool::{ClosedPool, Pool},
    shards_async::AsyncShards,
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
        self.pool = if self.config.mock {
            let db = nodes.first().map(|a| a.redis_settings().db()).unwrap_or(0);
            Box::new(MockRedis::new(db).await?)
        } else if self.config.shards || self.config.cluster.is_none() {
            Box::new(AsyncShards::new(self.config.clone()).await?)
        } else {
            match self.config.cluster.unwrap() {
                true => Box::new(Cluster::new(nodes, ms).await?),
                false => Box::new(Node::new(nodes.remove(0), self.config.clone()).await?),
            }
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
                if let Some(username) = s.redis_settings().username() {
                    result.insert("username", redis::Value::SimpleString(username.to_string()));
                }
                if s.redis_settings().password().is_some() {
                    result.insert("auth", redis::Value::Boolean(true));
                }
                redis::Value::SimpleString(s.addr().to_string())
            })
            .collect();
        result.insert("initial_nodes", redis::Value::Array(initial_nodes));
        result.insert("max_size", redis::Value::Int(self.config.max_size as i64));
        result
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }

    pub async fn execute<T: FromRedisValue>(
        &self,
        cmd: Cmd,
        params: Params,
    ) -> Result<T, error::RedisError> {
        let value = self.pool.execute(cmd, params).await?;
        let result: T = FromRedisValue::from_redis_value(value)
            .map_err(|e| error::RedisError::RedisError(e.into()))?;
        Ok(result)
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

    #[tokio::test]
    async fn pm_mock() {
        let cfg = Config::mock();
        let mut pm = PoolManager::new(cfg).unwrap();
        pm.init().await.unwrap();
        let result = pm.status();
        pm.close().await;
        assert_eq!(result.len(), 5);
    }
}
