use std::{
    collections::{BTreeMap, HashMap, HashSet},
    iter::zip,
    sync::Arc,
    time::{Duration, SystemTime},
};

use async_trait::async_trait;
use redis::Cmd;
use tokio::sync::RwLock;

use crate::{command::Params, error, pool::Pool};

#[derive(Clone, Default)]
enum InnerValue {
    #[default]
    Nil,
    Bytes(Vec<u8>),
    String(String),
    Boolean(bool),
    Int(i64),
    Array(Vec<redis::Value>),
    Set(HashSet<Vec<u8>>),
    Map(BTreeMap<Vec<u8>, redis::Value>),
}

impl From<redis::Value> for InnerValue {
    fn from(value: redis::Value) -> Self {
        match value {
            redis::Value::Nil => InnerValue::Nil,
            redis::Value::BulkString(v) => InnerValue::Bytes(v),
            redis::Value::SimpleString(b) => InnerValue::String(b),
            redis::Value::Int(i) => InnerValue::Int(i),
            redis::Value::Boolean(b) => InnerValue::Boolean(b),
            redis::Value::Array(a) => InnerValue::Array(a),
            redis::Value::Set(s) => {
                let mut result = HashSet::new();
                for e in s {
                    if let redis::Value::BulkString(e) = e {
                        result.insert(e);
                    } else {
                        unreachable!()
                    }
                }
                InnerValue::Set(result)
            }
            redis::Value::Map(v) => {
                let mut result = BTreeMap::new();
                for (k, val) in v {
                    if let redis::Value::BulkString(k) = k {
                        result.insert(k, val);
                    } else {
                        unreachable!()
                    }
                }
                InnerValue::Map(result)
            }
            _ => unreachable!(),
        }
    }
}

impl From<InnerValue> for redis::Value {
    fn from(value: InnerValue) -> Self {
        match value {
            InnerValue::Nil => redis::Value::Nil,
            InnerValue::Bytes(v) => redis::Value::BulkString(v),
            InnerValue::String(b) => redis::Value::SimpleString(b),
            InnerValue::Int(i) => redis::Value::Int(i),
            InnerValue::Boolean(b) => redis::Value::Boolean(b),
            InnerValue::Array(a) => redis::Value::Array(a),
            InnerValue::Map(m) => {
                let mut result = Vec::new();
                for (k, v) in m {
                    result.push((redis::Value::BulkString(k), v));
                }
                redis::Value::Map(result)
            }
            InnerValue::Set(s) => {
                let mut result = Vec::new();
                for e in s {
                    result.push(redis::Value::BulkString(e));
                }
                redis::Value::Set(result)
            }
        }
    }
}

#[derive(Default)]
struct Value {
    value: InnerValue,
    ts: Option<SystemTime>,
    groups: RwLock<HashMap<String, String>>,
}

impl Value {
    fn empty_map() -> Self {
        Self {
            value: InnerValue::Map(Default::default()),
            ..Default::default()
        }
    }

    fn empty_array() -> Self {
        Self {
            value: InnerValue::Array(Default::default()),
            ..Default::default()
        }
    }

    fn empty_set() -> Self {
        Self {
            value: InnerValue::Set(Default::default()),
            ..Default::default()
        }
    }

    fn ttl(&self) -> i64 {
        self.ts
            .and_then(|ts| ts.duration_since(SystemTime::now()).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(-1)
    }

    fn set_ex(&mut self, v: &[u8]) {
        let r = String::from_utf8_lossy(v).parse::<u64>();
        self.ts = r.map(|v| SystemTime::now() + Duration::from_secs(v)).ok();
    }

    fn set_px(&mut self, v: &[u8]) {
        let r = String::from_utf8_lossy(v).parse::<u64>();
        self.ts = r.map(|v| SystemTime::now() + Duration::from_millis(v)).ok();
    }

    fn get_array_mut(&mut self) -> &mut Vec<redis::Value> {
        if let InnerValue::Array(ref mut m) = self.value {
            m
        } else {
            self.value = InnerValue::Array(vec![]);
            if let InnerValue::Array(ref mut m) = self.value {
                m
            } else {
                unreachable!()
            }
        }
    }

    fn get_set_mut(&mut self) -> &mut HashSet<Vec<u8>> {
        if let InnerValue::Set(ref mut s) = self.value {
            s
        } else {
            self.value = InnerValue::Set(Default::default());
            if let InnerValue::Set(ref mut s) = self.value {
                s
            } else {
                unreachable!()
            }
        }
    }

    fn get_map_mut(&mut self) -> &mut BTreeMap<Vec<u8>, redis::Value> {
        if let InnerValue::Map(ref mut m) = self.value {
            m
        } else {
            self.value = InnerValue::Map(Default::default());
            if let InnerValue::Map(ref mut m) = self.value {
                m
            } else {
                unreachable!()
            }
        }
    }
}

impl From<redis::Value> for Value {
    fn from(value: redis::Value) -> Self {
        Self {
            value: value.into(),
            ..Default::default()
        }
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self {
            value: InnerValue::Bytes(value.to_vec()),
            ..Default::default()
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self {
            value: InnerValue::Boolean(value),
            ..Default::default()
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self {
            value: InnerValue::Int(value),
            ..Default::default()
        }
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Self {
            value: InnerValue::Int(value as i64),
            ..Default::default()
        }
    }
}

type DataBase = Arc<tokio::sync::RwLock<HashMap<Vec<u8>, Value>>>;

fn get_db() -> Arc<RwLock<HashMap<i64, DataBase>>> {
    use std::sync::OnceLock;
    static DBS: OnceLock<Arc<RwLock<HashMap<i64, DataBase>>>> = OnceLock::new();
    DBS.get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
        .clone()
}

#[derive(Default, Clone)]
pub struct MockRedis {
    pub db: i64,
    values: DataBase,
}

impl MockRedis {
    pub async fn new(db: i64) -> Result<Self, error::RedisError> {
        Ok(Self {
            db,
            values: get_db()
                .write()
                .await
                .entry(db)
                .or_insert_with(|| Arc::new(RwLock::new(HashMap::new())))
                .clone(),
        })
    }
}

#[async_trait]
impl Pool for MockRedis {
    async fn execute(&self, cmd: Cmd, _params: Params) -> Result<redis::Value, error::RedisError> {
        let mut cmd_iter = cmd.args_iter().filter_map(|arg| match arg {
            redis::Arg::Simple(s) => Some(s),
            _ => None,
        });
        let result: redis::Value = match cmd_iter.next() {
            Some(b"AUTH") => redis::Value::Boolean(true),
            Some(b"ACL") => match cmd_iter.next() {
                Some(b"GENPASS") => redis::Value::BulkString(b"GENPASS".to_vec()),
                Some(b"WHOAMI") => redis::Value::BulkString(b"test".to_vec()),
                _ => redis::Value::Boolean(true),
            },
            Some(b"INFO") => {
                let line = b"redis_version:7.2.0".to_vec();
                redis::Value::BulkString(line)
            }
            Some(b"KEYS") => {
                let mut result = vec![];
                if let Some(mut key) = cmd_iter.next() {
                    if key.ends_with(b"*") {
                        key = key[..key.len() - 1].as_ref();
                    }
                    let values = self.values.read().await;
                    for k in values.keys() {
                        if k.starts_with(key) {
                            result.push(redis::Value::BulkString(k.clone()));
                        }
                    }
                }
                redis::Value::Array(result)
            }
            Some(b"SET") => {
                let mut result = redis::Value::Nil;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    if let Some(value) = cmd_iter.next() {
                        let mut value: Value = value.into();
                        while let Some(v) = cmd_iter.next() {
                            match v {
                                b"EX" => cmd_iter.next().map(|v| value.set_ex(v)),
                                b"PX" => cmd_iter.next().map(|v| value.set_px(v)),
                                _ => None,
                            };
                        }
                        if let Some(v) = values.insert(key.into(), value) {
                            result = v.value.into();
                        }
                    }
                };
                result
            }
            Some(b"GET") => {
                let mut result = redis::Value::Nil;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(v) = values.get(key) {
                        result = v.value.clone().into();
                    };
                }
                result
            }
            Some(b"TTL") => {
                let mut result = -1;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(v) = values.get(key) {
                        result = v.ttl();
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"EXPIRE") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    if let Some(value) = values.get_mut(key) {
                        if let Some(v) = cmd_iter.next() {
                            value.set_ex(v);
                            result = 1;
                        }
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"EXISTS") => {
                let mut result = false;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if values.contains_key(key) {
                        result = true;
                    }
                }
                redis::Value::Boolean(result)
            }
            Some(b"DEL") => {
                let mut result = 0;
                let mut values = self.values.write().await;
                for key in cmd_iter {
                    if values.remove(key).is_some() {
                        result += 1;
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"HGETALL") => {
                let mut result: Option<InnerValue> = None;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    result = values.get(key).map(|v| v.value.clone());
                }
                result.unwrap_or(InnerValue::Map(Default::default())).into()
            }
            Some(b"HSET") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_map);
                    let m = value.get_map_mut();
                    while let Some(f) = cmd_iter.next() {
                        if let Some(v) = cmd_iter.next() {
                            m.insert(f.to_vec(), redis::Value::BulkString(v.to_vec()));
                            result += 1;
                        }
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"HDEL") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_map);
                    let m = value.get_map_mut();
                    for f in cmd_iter {
                        if m.remove(f).is_some() {
                            result += 1;
                        }
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"HMGET") => {
                let mut result = vec![];
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(Value {
                        value: InnerValue::Map(m),
                        ..
                    }) = values.get(key)
                    {
                        for f in cmd_iter {
                            if let Some(v) = m.get(f) {
                                result.push(v.clone());
                            } else {
                                result.push(InnerValue::Nil.into());
                            }
                        }
                    };
                }
                redis::Value::Array(result)
            }
            Some(b"HGET") => {
                let mut result = redis::Value::Nil;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(Value {
                        value: InnerValue::Map(m),
                        ..
                    }) = values.get(key)
                    {
                        if let Some(v) = cmd_iter.next().and_then(|f| m.get(f)) {
                            result = v.clone();
                        }
                    };
                }
                result
            }
            Some(b"HEXISTS") => {
                let mut result = false;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(Value {
                        value: InnerValue::Map(m),
                        ..
                    }) = values.get(key)
                    {
                        if let Some(f) = cmd_iter.next() {
                            result = m.contains_key(f);
                        }
                    };
                }
                redis::Value::Boolean(result)
            }
            Some(b"LPUSH") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_array);
                    let a = value.get_array_mut();
                    if let Some(v) = cmd_iter.next() {
                        a.insert(0, redis::Value::BulkString(v.to_vec()))
                    }
                    result = a.len() as i64;
                }
                redis::Value::Int(result)
            }
            Some(b"RPUSH") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_array);
                    let a = value.get_array_mut();
                    if let Some(v) = cmd_iter.next() {
                        a.push(redis::Value::BulkString(v.to_vec()))
                    }
                    result = a.len() as i64;
                }
                redis::Value::Int(result)
            }
            Some(b"LPOP") => {
                let mut result = redis::Value::Nil;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_array);
                    let a = value.get_array_mut();

                    if let Some(count) = cmd_iter.next() {
                        let count = String::from_utf8_lossy(count).parse().unwrap_or(1);
                        let mut r = Vec::new();
                        for _ in 0..count {
                            if !a.is_empty() {
                                r.push(a.remove(0));
                            }
                        }
                        result = redis::Value::Array(r);
                    } else if !a.is_empty() {
                        result = a.remove(0);
                    }
                }
                result
            }
            Some(b"BLPOP") => {
                let mut result = redis::Value::Nil;
                let mut keys: Vec<_> = cmd_iter.collect();
                let _timeout = keys
                    .pop()
                    .and_then(|v| String::from_utf8_lossy(v).parse::<usize>().ok())
                    .unwrap_or(0);
                for key in keys {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_array);
                    let a = value.get_array_mut();
                    if !a.is_empty() {
                        result = redis::Value::Array(vec![
                            redis::Value::BulkString(key.to_vec()),
                            a.remove(0),
                        ]);
                        break;
                    }
                }
                result
            }
            Some(b"LLEN") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(value) = values.get(key) {
                        if let InnerValue::Array(ref a) = value.value {
                            result = a.len() as i64;
                        };
                    };
                };
                redis::Value::Int(result)
            }
            Some(b"LRANGE") => {
                let mut result = Vec::new();
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(Value {
                        value: InnerValue::Array(ref a),
                        ..
                    }) = values.get(key)
                    {
                        let start = cmd_iter
                            .next()
                            .and_then(|v| String::from_utf8_lossy(v).parse::<usize>().ok())
                            .filter(|b| *b < a.len())
                            .unwrap_or(0);
                        let end = cmd_iter
                            .next()
                            .and_then(|v| String::from_utf8_lossy(v).parse::<usize>().ok())
                            .filter(|b| *b < a.len())
                            .unwrap_or(a.len() - 1);
                        if start <= end {
                            result.extend(a[start..=end].to_vec());
                        }
                    };
                }
                redis::Value::Array(result)
            }
            Some(b"LREM") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_array);
                    let a = value.get_array_mut();
                    if let Some(count) = cmd_iter
                        .next()
                        .and_then(|v| String::from_utf8_lossy(v).parse::<i64>().ok())
                    {
                        if let Some(v) = cmd_iter.next() {
                            let v = redis::Value::BulkString(v.to_vec());
                            match count {
                                0 => a.retain(|x| {
                                    let r = x != &v;
                                    if r {
                                        result += 1;
                                    }
                                    r
                                }),
                                1.. => {
                                    let mut n = 0;
                                    a.retain(|x| {
                                        if x == &v {
                                            n += 1;
                                            n <= count
                                        } else {
                                            result += 1;
                                            true
                                        }
                                    });
                                }
                                _ => {
                                    let mut n = 0;
                                    a.retain(|x| {
                                        if x == &v {
                                            n += 1;
                                            n <= -count
                                        } else {
                                            result += 1;
                                            true
                                        }
                                    });
                                }
                            }
                        }
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"XADD") => {
                let mut result = redis::Value::Nil;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let mut nomkstream = false;
                    let mut cmd_inv: Vec<&[u8]> = cmd_iter.collect();
                    if cmd_inv.contains(&b"NOMKSTREAM".to_vec().as_slice()) {
                        cmd_inv.retain(|x| !x.eq(b"NOMKSTREAM"));
                        nomkstream = true;
                    }
                    if !nomkstream || values.contains_key(key) {
                        let value = values.entry(key.into()).or_insert_with(Value::empty_array);
                        let a = value.get_array_mut();
                        let id = format!(
                            "{}-{}",
                            SystemTime::now()
                                .duration_since(SystemTime::UNIX_EPOCH)
                                .unwrap()
                                .as_millis(),
                            a.len(),
                        );
                        result = redis::Value::SimpleString(id.clone());
                        let mut m = Vec::new();
                        while let Some(v) = cmd_inv.pop() {
                            if let Some(f) = cmd_inv.pop() {
                                m.push((
                                    redis::Value::BulkString(f.to_vec()),
                                    redis::Value::BulkString(v.to_vec()),
                                ));
                            }
                        }
                        m.push((
                            redis::Value::SimpleString("id".to_string()),
                            redis::Value::SimpleString(id.clone()),
                        ));
                        a.push(redis::Value::Map(m));
                    }
                }
                result
            }
            Some(b"XGROUP") => {
                let result = redis::Value::Nil;
                if let Some(b"CREATE") = cmd_iter.next() {
                    if let Some(stream) = cmd_iter.next() {
                        let mut values = self.values.write().await;
                        let value: &mut Value = values
                            .entry(stream.into())
                            .or_insert_with(Value::empty_array);
                        let a = value.get_array_mut();
                        let mut id = "0".to_string();
                        if let Some(redis::Value::Map(msg)) = a.last() {
                            if let Some((
                                redis::Value::SimpleString(_),
                                redis::Value::SimpleString(ref msg_id),
                            )) = msg.last()
                            {
                                id = msg_id.clone();
                            }
                        }
                        let mut groups = value.groups.write().await;
                        if let Some(group) = cmd_iter
                            .next()
                            .map(|v| String::from_utf8_lossy(v).to_string())
                        {
                            groups.insert(group, id);
                        }
                    }
                }
                result
            }
            Some(b"XREAD") | Some(b"XREADGROUP") => {
                let mut result = HashMap::new();
                let values = self.values.read().await;
                let mut group = None;
                let mut _consumer = None;
                while let Some(v) = cmd_iter.next() {
                    if v.eq(b"GROUP") {
                        group = cmd_iter
                            .next()
                            .map(|v| String::from_utf8_lossy(v).to_string());
                        _consumer = cmd_iter
                            .next()
                            .map(|v| String::from_utf8_lossy(v).to_string());
                    } else if v.eq(b"STREAMS") {
                        let mut streams = Vec::new();
                        for x in cmd_iter.by_ref() {
                            streams.push(x);
                        }
                        let mut ids = Vec::new();
                        for _ in 0..streams.len() / 2 {
                            ids.push(
                                streams
                                    .pop()
                                    .map(|v| String::from_utf8_lossy(v).to_string())
                                    .unwrap(),
                            );
                        }
                        ids.reverse();
                        for (stream, mut id) in zip(streams, ids) {
                            if let Some(value) = values.get(stream) {
                                if id.eq(">") {
                                    if let Some(ref group) = group {
                                        let groups = value.groups.read().await;
                                        if let Some(group) = groups.get(group) {
                                            id = group.clone();
                                        } else {
                                            Err(error::RedisError::CommandError(
                                                "NOGROUP No such consumer group".into(),
                                            ))?;
                                        }
                                    }
                                } else if id.eq("$") {
                                    id = format!(
                                        "{}-99",
                                        SystemTime::now()
                                            .duration_since(SystemTime::UNIX_EPOCH)
                                            .unwrap()
                                            .as_millis(),
                                    );
                                }
                                let stream = stream.to_vec();
                                if let InnerValue::Array(ref a) = value.value {
                                    let m = result
                                        .entry(stream)
                                        .or_insert_with(HashMap::<String, redis::Value>::new);
                                    for x in a {
                                        if let redis::Value::Map(msg) = x {
                                            if let Some((
                                                redis::Value::SimpleString(f),
                                                redis::Value::SimpleString(msg_id),
                                            )) = msg.last()
                                            {
                                                if f.eq("id")
                                                    && msg_id
                                                        .cmp(&id)
                                                        .eq(&std::cmp::Ordering::Greater)
                                                {
                                                    let mut msg = msg.clone();
                                                    msg.pop();
                                                    m.insert(
                                                        msg_id.to_string(),
                                                        redis::Value::Array(
                                                            msg.into_iter()
                                                                .flat_map(|(k, v)| vec![k, v])
                                                                .collect(),
                                                        ),
                                                    );
                                                    if let Some(ref group) = group {
                                                        let mut groups = value.groups.write().await;
                                                        groups
                                                            .insert(group.clone(), msg_id.clone());
                                                    }
                                                }
                                            }
                                        }
                                    }
                                };
                            }
                        }
                    };
                }
                let mut r = Vec::new();
                for (stream, msgs) in result {
                    let mut items = Vec::new();
                    for (msg_id, msg) in msgs {
                        items.push(redis::Value::Array(vec![
                            redis::Value::SimpleString(msg_id),
                            msg,
                        ]));
                    }
                    if !items.is_empty() {
                        r.push(redis::Value::SimpleString(
                            String::from_utf8_lossy(&stream).to_string(),
                        ));
                        r.push(redis::Value::Array(items));
                    }
                }
                redis::Value::Array(r)
            }
            Some(b"XACK") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(Value {
                        value: InnerValue::Array(_m),
                        ..
                    }) = values.get(key)
                    {
                        result = 1;
                    };
                }
                redis::Value::Int(result)
            }
            Some(b"XINFO") => {
                let mut result = redis::Value::Nil;
                match cmd_iter.next() {
                    Some(b"STREAM") => {
                        if let Some(stream) = cmd_iter.next() {
                            let mut values = self.values.write().await;
                            let value: &mut Value = values
                                .entry(stream.into())
                                .or_insert_with(Value::empty_array);
                            let mut r = vec![];
                            r.push(redis::Value::SimpleString("name".to_string()));
                            r.push(redis::Value::BulkString(stream.to_vec()));

                            let a = value.get_array_mut();
                            r.push(redis::Value::SimpleString("messages".to_string()));
                            r.push(redis::Value::Int(a.len() as i64));

                            let groups = value.groups.read().await;
                            r.push(redis::Value::SimpleString("groups".to_string()));
                            r.push(redis::Value::Int(groups.len() as i64));

                            r.push(redis::Value::SimpleString("a".to_string()));
                            r.push(redis::Value::Int(0));

                            r.push(redis::Value::SimpleString("b".to_string()));
                            r.push(redis::Value::Int(0));

                            r.push(redis::Value::SimpleString("c".to_string()));
                            r.push(redis::Value::Int(0));

                            r.push(redis::Value::SimpleString("d".to_string()));
                            r.push(redis::Value::Int(0));

                            result = redis::Value::Array(r);
                        }
                    }
                    Some(b"GROUPS") => {
                        if let Some(stream) = cmd_iter.next() {
                            let mut values = self.values.write().await;
                            let value: &mut Value = values
                                .entry(stream.into())
                                .or_insert_with(Value::empty_array);

                            let mut r = vec![];

                            let groups = value.groups.read().await;
                            for (group, id) in groups.iter() {
                                r.push(redis::Value::Array(vec![
                                    redis::Value::SimpleString("stream".to_string()),
                                    redis::Value::BulkString(stream.to_vec()),
                                    redis::Value::SimpleString("group".to_string()),
                                    redis::Value::SimpleString(group.clone()),
                                    redis::Value::SimpleString("id".to_string()),
                                    redis::Value::SimpleString(id.clone()),
                                    redis::Value::SimpleString("consumers".to_string()),
                                    redis::Value::BulkString(b"1".to_vec()),
                                ]));
                            }
                            result = redis::Value::Array(r);
                        }
                    }
                    Some(b"CONSUMERS") => {
                        if let Some(stream) = cmd_iter.next() {
                            let mut values = self.values.write().await;
                            let value: &mut Value = values
                                .entry(stream.into())
                                .or_insert_with(Value::empty_array);

                            let mut r = vec![];

                            if let Some(group) = cmd_iter.next() {
                                r.push(redis::Value::SimpleString("stream".to_string()));
                                r.push(redis::Value::BulkString(stream.to_vec()));

                                r.push(redis::Value::SimpleString("group".to_string()));
                                r.push(redis::Value::BulkString(group.to_vec()));

                                let groups = value.groups.read().await;
                                if groups.contains_key(&String::from_utf8_lossy(group).to_string())
                                {
                                    r.push(redis::Value::SimpleString("consumers".to_string()));
                                    r.push(redis::Value::BulkString(b"1".to_vec()));
                                }
                            }
                            result = redis::Value::Array(vec![redis::Value::Array(r)]);
                        }
                    }
                    _ => {}
                }
                result
            }
            Some(b"ZADD") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_map);
                    let a = value.get_map_mut();
                    while let Some(s) = cmd_iter.next() {
                        let score = String::from_utf8_lossy(s).parse::<f64>().unwrap_or(0.0);
                        if let Some(v) = cmd_iter.next() {
                            a.insert(v.to_vec(), redis::Value::Double(score));
                            result += 1;
                        }
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"ZREM") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_map);
                    let a = value.get_map_mut();
                    for v in cmd_iter {
                        if a.remove(v).is_some() {
                            result += 1;
                        }
                    }
                }
                redis::Value::Int(result)
            }
            Some(b"ZRANGE") => {
                let mut result = Vec::new();
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    let mut withscores = false;
                    for v in cmd_iter {
                        if v == b"WITHSCORES" {
                            withscores = true;
                        }
                    }
                    if let Some(Value {
                        value: InnerValue::Map(m),
                        ..
                    }) = values.get(key)
                    {
                        let mut x: Vec<_> = m
                            .clone()
                            .into_iter()
                            .filter_map(|(k, v)| match v {
                                redis::Value::Double(s) => Some((s, k)),
                                _ => None,
                            })
                            .collect();
                        x.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
                        if withscores {
                            result.extend(x.into_iter().flat_map(|(s, v)| {
                                vec![
                                    redis::Value::BulkString(v.to_vec()),
                                    redis::Value::Double(s),
                                ]
                            }));
                        } else {
                            result.extend(
                                x.into_iter()
                                    .map(|(_, v)| redis::Value::BulkString(v.to_vec())),
                            );
                        };
                    };
                }
                redis::Value::Array(result)
            }
            Some(b"ZCARD") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let values = self.values.read().await;
                    if let Some(Value {
                        value: InnerValue::Map(m),
                        ..
                    }) = values.get(key)
                    {
                        result = m.len() as i64;
                    };
                }
                redis::Value::Int(result)
            }
            Some(b"PFADD") => {
                let mut result = false;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_set);
                    let s = value.get_set_mut();
                    let old = s.len();
                    s.extend(cmd_iter.map(|v| v.to_vec()));
                    result = old != s.len();
                }
                redis::Value::Boolean(result)
            }
            Some(b"PFCOUNT") => {
                let mut result = 0;
                if let Some(key) = cmd_iter.next() {
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_set);
                    let s = value.get_set_mut();
                    result = s.len() as i64;
                }
                redis::Value::Int(result)
            }
            Some(b"PFMERGE") => {
                if let Some(key) = cmd_iter.next() {
                    let mut target = HashSet::new();
                    {
                        let mut values = self.values.write().await;
                        let value = values.entry(key.into()).or_insert_with(Value::empty_set);
                        target.extend(value.get_set_mut().iter().cloned());
                    }

                    for key in cmd_iter {
                        let values = self.values.read().await;
                        if let Some(Value {
                            value: InnerValue::Set(s),
                            ..
                        }) = values.get(key)
                        {
                            target.extend(s.iter().cloned());
                        }
                    }
                    let mut values = self.values.write().await;
                    let value = values.entry(key.into()).or_insert_with(Value::empty_set);
                    value.get_set_mut().extend(target);
                }
                redis::Value::Boolean(true)
            }
            Some(b"EVAL") => {
                let mut result = redis::Value::Nil;
                if let Some(b"return ARGV[1]") = cmd_iter.next() {
                    if let Some(b"0") = cmd_iter.next() {
                        if let Some(arg) = cmd_iter.next() {
                            result = redis::Value::BulkString(arg.to_vec());
                        }
                    }
                }
                result
            }
            _ => redis::Value::Nil,
        };

        Ok(result)
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = HashMap::new();
        result.insert("closed", redis::Value::Boolean(false));
        result.insert("impl", redis::Value::SimpleString("mock".into()));
        result.insert("db", redis::Value::Int(self.db));
        result
    }
}

#[cfg(test)]
mod tests {
    use crate::{command::Params, pool::Pool};

    use super::MockRedis;

    #[tokio::test]
    async fn status() {
        let m = MockRedis::new(0).await.unwrap();
        let result = m.status();
        assert_eq!(result.len(), 3);
    }

    #[tokio::test]
    async fn set_get() {
        let key = "key";
        let params = Params::default();
        let m = MockRedis::new(0).await.unwrap();
        let cmd = redis::cmd("SET").arg(key).arg(1).to_owned();
        let result = m.execute(cmd, params.clone()).await.unwrap();
        assert_eq!(result, redis::Value::Nil);
        let cmd = redis::cmd("GET").arg(key).to_owned();
        let result = m.execute(cmd, params).await.unwrap();
        assert_eq!(result, redis::Value::BulkString(b"1".to_vec()));
    }

    #[tokio::test]
    async fn hset_hget_hgetall() {
        let key = "hkey";
        let params = Params::default();
        let m = MockRedis::new(0).await.unwrap();
        let cmd = redis::cmd("HSET").arg(key).arg("f").arg(2).to_owned();
        let result = m.execute(cmd, params.clone()).await.unwrap();
        assert_eq!(result, redis::Value::Int(1));
        let cmd = redis::cmd("HGET").arg(key).arg("f").to_owned();
        let result = m.execute(cmd, params.clone()).await.unwrap();
        assert_eq!(result, redis::Value::BulkString(b"2".to_vec()));
        let cmd = redis::cmd("HGETALL").arg(key).to_owned();
        let result = m.execute(cmd, params).await.unwrap();
        assert_eq!(
            result,
            redis::Value::Map(vec![(
                redis::Value::BulkString(b"f".to_vec()),
                redis::Value::BulkString(b"2".to_vec()),
            )])
        );
    }
}
