use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;
use redis::{
    aio::ConnectionLike, Cmd, ConnectionInfo, IntoConnectionInfo, RedisError, RedisResult, Value,
};
use tokio::sync::RwLock;

use crate::{
    config::Config,
    error,
    pool::{Connection, Pool},
    shards::Slots,
};

type Node = crate::single_node::Node;

#[derive(Default, Clone)]
pub struct AsyncShards {
    slots: Arc<RwLock<Slots>>,
    nodes: Arc<RwLock<HashMap<String, Node>>>,
    config: Config,
    is_cluster: bool,
}

impl AsyncShards {
    pub async fn new(config: Config) -> RedisResult<AsyncShards> {
        let is_cluster = config.cluster;
        let init_nodes = config.initial_nodes.clone();
        let mut result = Self {
            config,
            ..Default::default()
        };
        for info in init_nodes.clone().into_iter() {
            let node = result.create_node(info.clone()).await?;
            result
                .nodes
                .write()
                .await
                .insert(info.addr.to_string(), node);
        }

        result.is_cluster = match is_cluster {
            Some(false) => false,
            _ => result.init_cluster().await.unwrap_or(false),
        };
        Ok(result)
    }

    async fn create_node(&self, info: ConnectionInfo) -> Result<Node, error::RedisError> {
        Node::new(info, self.config.clone()).await
    }

    async fn init_cluster(&self) -> Result<bool, error::RedisError> {
        self.update_slots().await?;
        let mut nodes = self.nodes.write().await;
        let vn: Vec<String> = nodes.keys().cloned().collect();
        let mut info = None;
        for addr in vn.into_iter() {
            let mut node = nodes.remove(&addr).unwrap();
            let id = node
                .req_packed_command(redis::cmd("CLUSTER").arg("MYID"))
                .await;
            if let Ok(Value::BulkString(id)) = id {
                info = Some(node.info.clone());
                let id = String::from_utf8_lossy(&id).to_string();
                node.id = Some(id.clone());
                let shard_node = self.slots.read().await.get_node_by_id(id).unwrap();
                nodes.insert(shard_node.addr, node);
            } else {
                nodes.insert(addr, node);
            }
        }
        if let Some(info) = info {
            for snode in self.slots.read().await.get_nodes() {
                if !nodes.contains_key(snode.addr.as_str()) {
                    let ninfo = snode.info_on(info.clone());
                    let pool = self.create_node(ninfo).await.map_err(RedisError::from)?;
                    nodes.insert(snode.addr, pool);
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn update_slots(&self) -> Result<(), error::RedisError> {
        let nodes = self.nodes.read().await;
        for node in nodes.values() {
            let mut node = node.clone();
            if let Ok(r) = node
                .req_packed_command(redis::cmd("CLUSTER").arg("SLOTS"))
                .await
            {
                if self.slots.write().await.set(r).is_ok() {
                    return Ok(());
                }
            }
        }
        Err(error::RedisError::not_initialized())
    }

    async fn send_command(&self, cmd: &redis::Cmd) -> Result<redis::Value, error::RedisError> {
        let slots = self.slots.read().await;
        let nodes = self.nodes.read().await;
        let addr = if let Some(shard) = slots.get_shard_from(cmd) {
            shard.master
        } else {
            nodes
                .keys()
                .next()
                .cloned()
                .ok_or(error::RedisError::NoSlot)?
        };
        let node = nodes
            .get(addr.as_str())
            .ok_or(error::RedisError::NotFoundNode)?;
        let mut node = node.clone();
        let r = node.req_packed_command(cmd).await?;
        Ok(r)
        // Ok(redis::Value::Nil)
    }

    async fn send_command_with_add_node(
        &self,
        cmd: &redis::Cmd,
    ) -> Result<redis::Value, error::RedisError> {
        let slots = self.slots.read().await;
        let shard = slots.get_shard_from(cmd).ok_or(error::RedisError::NoSlot)?;
        let addr = shard.master.as_str();
        let mut nodes = self.nodes.write().await;
        let node = if let Some(node) = nodes.get(addr) {
            node
        } else {
            let ninfo = ("redis://".to_string() + shard.master.as_str()).into_connection_info()?;
            let node = self.create_node(ninfo).await?;
            nodes.insert(addr.to_string(), node);
            nodes.get(addr).ok_or(error::RedisError::NotFoundNode)?
        };
        let mut node = node.clone();
        let r = node.req_packed_command(cmd).await?;
        Ok(r)
    }
}

#[async_trait]
impl ConnectionLike for AsyncShards {
    fn req_packed_command<'a>(
        &'a mut self,
        cmd: &'a redis::Cmd,
    ) -> redis::RedisFuture<'a, redis::Value> {
        let cmd = cmd.clone();
        Box::pin(async move {
            match self.send_command(&cmd).await {
                Ok(r) => Ok(r),
                Err(error::RedisError::NotFoundNode) => {
                    Ok(self.send_command_with_add_node(&cmd).await?)
                }
                Err(e) => Err(e.into()),
            }
        })
    }

    fn req_packed_commands<'a>(
        &'a mut self,
        _cmd: &'a redis::Pipeline,
        _offset: usize,
        _count: usize,
    ) -> redis::RedisFuture<'a, Vec<redis::Value>> {
        todo!()
    }

    fn get_db(&self) -> i64 {
        todo!()
    }
}

#[async_trait]
impl Pool for AsyncShards {
    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        let pool = self.clone();
        Ok(Connection { c: Box::new(pool) })
    }

    async fn execute(&self, cmd: Cmd) -> Result<redis::Value, error::RedisError> {
        let mut pool = self.clone();
        let value = pool.req_packed_command(&cmd).await?;
        Ok(value)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Boolean(false));
        result.insert("impl", redis::Value::SimpleString("shards_async".into()));
        result.insert("cluster", redis::Value::Boolean(self.is_cluster));
        if let Ok(nodes) = self.nodes.try_read() {
            let mut addrs: Vec<String> = nodes.keys().cloned().collect();
            addrs.sort();
            let addrs =
                redis::Value::Array(addrs.into_iter().map(redis::Value::SimpleString).collect());
            result.insert("nodes", addrs);
        }
        result
    }
}
