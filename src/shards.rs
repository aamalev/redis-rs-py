use std::{
    collections::{BTreeMap, HashMap},
    path::PathBuf,
};

use redis::{
    cluster_routing::get_slot, ConnectionAddr, ConnectionInfo, FromRedisValue, RedisResult, Value,
};

use crate::command::Params;

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub(crate) struct ShardNode {
    pub addr: String,
    master: bool,
}

impl ShardNode {
    pub(crate) fn addr_on(&self, addr: ConnectionAddr) -> ConnectionAddr {
        match addr {
            ConnectionAddr::Tcp(_, _) => {
                let (h, p) = self.split();
                ConnectionAddr::Tcp(h, p)
            }
            ConnectionAddr::TcpTls {
                insecure,
                host: _,
                port: _,
                tls_params,
            } => {
                let (host, port) = self.split();
                ConnectionAddr::TcpTls {
                    insecure,
                    host,
                    port,
                    tls_params,
                }
            }
            ConnectionAddr::Unix(_) => ConnectionAddr::Unix(PathBuf::from(self.addr.as_str())),
        }
    }
    pub(crate) fn info_on(&self, mut info: ConnectionInfo) -> ConnectionInfo {
        info.addr = self.addr_on(info.addr);
        info
    }
    fn split(&self) -> (String, u16) {
        if let Some((h, p)) = self.addr.split_once(':') {
            let port = p.parse().unwrap_or_default();
            (h.to_string(), port)
        } else {
            (self.addr.clone(), 0)
        }
    }
}

impl From<&str> for ShardNode {
    fn from(value: &str) -> Self {
        Self {
            addr: value.to_string(),
            master: false,
        }
    }
}

#[derive(Default, Debug, PartialEq, Clone)]
pub(crate) struct Shard {
    pub master: String,
    pub slaves: Vec<String>,
}

impl From<&str> for Shard {
    fn from(value: &str) -> Self {
        Shard {
            master: value.to_string(),
            slaves: vec![],
        }
    }
}

impl From<String> for Shard {
    fn from(value: String) -> Self {
        Shard {
            master: value,
            slaves: vec![],
        }
    }
}

pub struct Route {
    pub _slot: Option<u16>,
    pub shard: Option<Shard>,
}

#[derive(Default, Debug)]
pub(crate) struct Slots {
    slots: BTreeMap<u16, Shard>,
    id_map: HashMap<String, ShardNode>,
}

impl Slots {
    pub fn set(&mut self, value: Value) -> RedisResult<()> {
        let donor = Slots::from_redis_value(&value)?;
        for (id, sn) in donor.id_map.into_iter() {
            if !sn.addr.starts_with(':') {
                self.id_map.insert(id, sn);
            }
        }
        self.slots = donor.slots;
        Ok(())
    }

    fn get_shard(&self, slot: u16) -> Option<Shard> {
        let max = self.slots.keys().max().copied().unwrap_or_default();
        if max >= slot {
            self.slots.range(slot..).next().map(|(_, s)| s.clone())
        } else {
            None
        }
    }

    pub fn get_node_by_id(&self, id: String) -> Option<ShardNode> {
        self.id_map.get(&id).cloned()
    }

    pub fn get_route(&self, params: &Params) -> Route {
        let slot = params.keys.first().map(|k| get_slot(k));
        let shard = slot.and_then(|s| self.get_shard(s));
        Route { _slot: slot, shard }
    }

    pub(crate) fn get_nodes(&self) -> Vec<ShardNode> {
        self.id_map.values().cloned().collect()
    }
}

impl FromRedisValue for Slots {
    fn from_redis_value(v: &Value) -> RedisResult<Self> {
        if let Value::Array(v) = v {
            let mut id_map = HashMap::new();
            let m = v
                .iter()
                .filter_map(|v2| {
                    if let Value::Array(v) = v2 {
                        let mut iter = v.iter();
                        let _ = iter.next();
                        let n = iter
                            .next()
                            .map(|x| u16::from_redis_value(x).unwrap_or_default())
                            .unwrap_or_default();
                        let mut nodes: Vec<String> = iter
                            .filter_map(|v| {
                                if let Value::Array(mut v) = v.clone() {
                                    v.truncate(3);
                                    let mut addrs: Vec<String> = v
                                        .into_iter()
                                        .filter_map(|x| match x {
                                            Value::BulkString(d) => {
                                                Some(String::from_utf8_lossy(&d).to_string())
                                            }
                                            Value::SimpleString(s) => Some(s),
                                            Value::Int(n) => Some(n.to_string()),
                                            _ => None,
                                        })
                                        .collect();

                                    if let Some(id) = addrs.pop() {
                                        let addr = addrs.join(":");
                                        id_map.insert(id, ShardNode::from(addr.as_str()));
                                        Some(addr)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            })
                            .collect();
                        if !nodes.is_empty() {
                            let shard = Shard {
                                master: nodes.remove(0),
                                slaves: nodes,
                            };
                            Some((n, shard))
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .collect();
            Ok(Self { slots: m, id_map })
        } else {
            Ok(Self::default())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Shard, Slots};
    use crate::{command::Params, shards::ShardNode};
    use redis::{FromRedisValue, Value};

    const SLOT_SIZE: u16 = 16384;

    #[test]
    fn slots_parse() {
        let cluster_slots = Value::Array(vec![Value::Array(vec![
            Value::Int(0),
            Value::Int(2),
            Value::Array(vec![
                Value::BulkString(b"1.2.3.4".to_vec()),
                Value::Int(6379),
                Value::BulkString(b"123456789".to_vec()),
            ]),
        ])]);
        let slots = Slots::from_redis_value(&cluster_slots).expect("Slots parse error");
        let shard = Shard::from("1.2.3.4:6379");
        assert!(!slots.slots.is_empty(), "Slots is empty");
        assert_eq!(slots.slots.get(&2), Some(&shard));

        assert_eq!(
            slots.get_node_by_id("123456789".to_string()),
            Some(ShardNode::from(shard.master.as_str()))
        );
    }

    #[test]
    fn slots_get_shard() {
        let mut shards = Slots::default();

        let shard1 = Shard::from("1.2.3.3");
        shards.slots.insert(3, shard1.clone());

        let shard2 = Shard::from("1.2.3.5");
        shards.slots.insert(5, shard2.clone());

        assert_eq!(shards.get_shard(0), Some(shard1.clone()));
        assert_eq!(shards.get_shard(1), Some(shard1.clone()));
        assert_eq!(shards.get_shard(2), Some(shard1.clone()));
        assert_eq!(shards.get_shard(3), Some(shard1));
        assert_eq!(shards.get_shard(4), Some(shard2.clone()));
        assert_eq!(shards.get_shard(5), Some(shard2.clone()));
        assert_eq!(shards.get_shard(6), None);
    }

    #[test]
    fn slots_get_none_shard() {
        let mut shards = Slots::default();
        assert_eq!(shards.get_shard(89), None);

        let shard = Shard::from("1.2.3.4");
        shards.slots.insert(1, shard);
        assert_eq!(shards.get_shard(4), None);
    }

    #[test]
    fn slots_get_route() {
        let mut slots = Slots::default();

        let shard1 = Shard::from("1.2.3.3");
        slots.slots.insert(SLOT_SIZE, shard1.clone());
        let shard0 = Shard::from("1.2.3.4");
        slots.slots.insert(0, shard0);

        let cmd = redis::cmd("GET").arg("a").to_owned();
        let params = Params::from(&cmd);
        let route = slots.get_route(&params);
        assert_eq!(Some(shard1), route.shard);
    }
}
