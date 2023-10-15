use crate::{
    bb8_cluster::BB8Cluster,
    bb8_single::BB8Pool,
    cluster::Cluster,
    deadpool_cluster::DeadPoolCluster,
    error,
    pool::{ClosedPool, Connection, Pool},
    types,
};
use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict},
};
use pyo3_asyncio::tokio::future_into_py;
use redis::{AsyncCommands, FromRedisValue};
use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

pub struct ContextManager {
    pub(crate) is_cluster: bool,
    pub(crate) initial_nodes: Vec<String>,
    pub(crate) max_size: u32,
    pub(crate) pool_type: String,
    pub(crate) pool: Box<dyn Pool + Send + Sync>,
}

impl ContextManager {
    pub fn new_cluster(initial_nodes: Vec<String>) -> Self {
        Self {
            initial_nodes,
            is_cluster: true,
            max_size: 10,
            pool_type: "bb8".to_string(),
            pool: Box::new(ClosedPool),
        }
    }

    pub fn new(addr: &str) -> Self {
        Self {
            initial_nodes: vec![addr.to_string()],
            is_cluster: false,
            max_size: 10,
            pool_type: "bb8".to_string(),
            pool: Box::new(ClosedPool),
        }
    }

    async fn init(&mut self) -> &Self {
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
            let pool = BB8Pool::new(nodes, ms).await;
            self.pool = Box::new(pool);
        };
        self
    }

    async fn close(&mut self) -> &Self {
        self.pool = Box::new(ClosedPool);
        self
    }

    fn status(&self) -> HashMap<&str, redis::Value> {
        let mut result = self.pool.status();
        let initial_nodes = self
            .initial_nodes
            .iter()
            .map(|s| redis::Value::Data(s.as_bytes().to_vec()))
            .collect();
        result.insert("initial_nodes", redis::Value::Bulk(initial_nodes));
        result.insert("max_size", redis::Value::Int(self.max_size as i64));
        result
    }

    async fn execute<T: FromRedisValue>(
        &self,
        cmd: &str,
        args: Vec<types::Arg>,
    ) -> Result<T, error::RedisError> {
        let value = self.pool.execute(cmd.to_uppercase().as_str(), args).await?;
        let result: T = FromRedisValue::from_redis_value(&value)?;
        Ok(result)
    }

    async fn get_connection(&self) -> Result<Connection, error::RedisError> {
        self.pool.get_connection().await
    }
}

#[pyclass]
pub struct Client {
    pub(crate) cm: Arc<tokio::sync::RwLock<ContextManager>>,
}

impl From<ContextManager> for Client {
    fn from(value: ContextManager) -> Self {
        Self {
            cm: Arc::new(tokio::sync::RwLock::new(value)),
        }
    }
}

#[pymethods]
impl Client {
    fn __aenter__<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let result = Client { cm: cm.clone() };
            cm.write().await.init().await;
            Ok(result)
        })
    }

    fn __aexit__<'a>(
        &mut self,
        py: Python<'a>,
        _exc_type: &PyAny,
        _exc_value: &PyAny,
        _traceback: &PyAny,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            cm.write().await.close().await;
            Ok(())
        })
    }

    fn status(&self, py: Python) -> PyResult<PyObject> {
        if let Ok(cm) = self.cm.try_read() {
            let mut status = cm.status();
            let is_closed = status.remove("closed");
            let is_cluster = status.remove("cluster");
            let result = PyDict::new(py);
            for (k, v) in status.into_iter() {
                let value = types::to_object(py, v, "utf-8");
                result.set_item(k, value)?;
            }
            if let Some(redis::Value::Int(c)) = is_cluster {
                let is_cluster = c == 1;
                result.set_item("cluster", is_cluster.to_object(py))?;
            }
            if let Some(redis::Value::Int(c)) = is_closed {
                let is_closed = c == 1;
                result.set_item("closed", is_closed.to_object(py))?;
            }
            Ok(result.to_object(py))
        } else {
            Err(error::RedisError::PoolError("Try leter".to_string()).into())
        }
    }

    fn get_encoding(&self, kwargs: Option<&PyDict>) -> String {
        let mut encoding = String::default();
        if let Some(kw) = kwargs {
            if let Some(val) = kw.get_item("encoding") {
                if let Ok(val) = val.extract() {
                    encoding = val;
                }
            }
        }
        encoding
    }

    #[pyo3(signature = (cmd, *args, **kwargs))]
    fn execute<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        future_into_py(py, async move {
            let cm = cm.read().await;
            let value = cm.pool.execute(&cmd, args).await?;
            Ok(Python::with_gil(|py| {
                types::to_object(py, value, &encoding)
            }))
        })
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_str<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Option<String> = cm.execute(&String::from(cmd), args).await?;
            Ok(result)
        })
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_bytes<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Vec<u8> = cm.execute(&String::from(cmd), args).await?;
            Ok(Python::with_gil(|py| {
                PyBytes::new(py, &result).to_object(py)
            }))
        })
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_list<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Vec<String> = cm.execute(&String::from(cmd), args).await?;
            Ok(result)
        })
    }

    #[pyo3(signature = (cmd, *args, **kwargs))]
    fn fetch_dict<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        future_into_py(py, async move {
            let cm = cm.read().await;
            let value = cm.pool.execute(&cmd, args).await?;
            Ok(Python::with_gil(|py| types::to_dict(py, value, &encoding)))
        })
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_scores<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let map: HashMap<String, f64> = cm.execute(&String::from(cmd), args).await?;
            Ok(Python::with_gil(|py| {
                let result = PyDict::new(py);
                for (k, v) in map.into_iter() {
                    result.set_item(k, v).unwrap();
                }
                result.to_object(py)
            }))
        })
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_int<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: i64 = cm.execute(&String::from(cmd), args).await?;
            Ok(result)
        })
    }

    #[pyo3(signature = (key))]
    fn exists<'a>(&self, py: Python<'a>, key: types::Str) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: bool = c.exists(key).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    #[pyo3(signature = (key, value))]
    fn set<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: bool = c.set(key, value).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    #[pyo3(signature = (key, **kwargs))]
    fn get<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c.get(key).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::decode(py, result, &encoding)))
        })
    }

    #[pyo3(signature = (key, field, value))]
    fn hset<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Str,
        value: types::Arg,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: bool = c
                .hset(key, field, value)
                .await
                .map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    #[pyo3(signature = (key, field, **kwargs))]
    fn hget<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c.hget(key, field).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::decode(py, result, &encoding)))
        })
    }

    #[pyo3(signature = (key, **kwargs))]
    fn hgetall<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        future_into_py(py, async move {
            let mut cm = cm.read().await.get_connection().await?;
            let value = cm.hgetall(key).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::to_dict(py, value, &encoding)))
        })
    }

    #[pyo3(signature = (key, delta = 1.0))]
    fn incr<'a>(&self, py: Python<'a>, key: types::Str, delta: f64) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: f64 = c.incr(key, delta).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    #[pyo3(signature = (key, value))]
    fn lpush<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: bool = c.lpush(key, value).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    #[pyo3(signature = (key, count = None, **kwargs))]
    fn lpop<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        count: Option<NonZeroUsize>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c.lpop(key, count).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| {
                types::to_object(py, result, &encoding)
            }))
        })
    }

    #[pyo3(signature = (key, start = 0, stop = -1, **kwargs))]
    fn lrange<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        start: isize,
        stop: isize,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c
                .lrange(key, start, stop)
                .await
                .map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| {
                types::to_object(py, result, &encoding)
            }))
        })
    }

    #[pyo3(signature = (stream, *args, id = None, items = None))]
    fn xadd<'a>(
        &self,
        py: Python<'a>,
        stream: types::Str,
        mut args: Vec<types::ScalarOrMap>,
        mut id: Option<types::Str>,
        items: Option<HashMap<String, types::Arg>>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        args.push(types::ScalarOrMap::Map(items.unwrap_or_default()));
        let mut map = HashMap::new();
        let mut flat = vec![];

        for arg in args.into_iter() {
            match arg {
                types::ScalarOrMap::Map(aitems) => map.extend(aitems),
                types::ScalarOrMap::Scalar(s) => flat.push(s),
            };
        }

        if flat.len() % 2 == 1 {
            id = Some(types::Str::String(flat.remove(0).into()));
        }
        while !flat.is_empty() {
            map.insert(flat.remove(0).into(), flat.remove(0));
        }
        let items: Vec<(String, types::Arg)> = map.into_iter().collect();

        let id = id.unwrap_or(types::Str::String("*".to_string()));
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: String = c
                .xadd(stream, id, &items)
                .await
                .map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    #[pyo3(signature = (streams, *args, id=None, **kwargs))]
    fn xread<'a>(
        &self,
        py: Python<'a>,
        streams: types::ScalarOrMap,
        args: Vec<types::Str>,
        id: Option<types::Arg>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        let encoding = self.get_encoding(kwargs);
        let mut keys = vec![];
        let mut ids = vec![];
        let id = id.unwrap_or(types::Arg::Int(0));
        match streams {
            types::ScalarOrMap::Scalar(s) => {
                keys.push(String::from(s));
                for stream in args.into_iter() {
                    keys.push(String::from(stream));
                    ids.push(id.clone());
                }
                ids.push(id);
            }
            types::ScalarOrMap::Map(m) => {
                for (k, v) in m.into_iter() {
                    keys.push(k);
                    ids.push(v);
                }
            }
        }
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c
                .xread(&keys, &ids)
                .await
                .map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::to_dict(py, result, &encoding)))
        })
    }
}
