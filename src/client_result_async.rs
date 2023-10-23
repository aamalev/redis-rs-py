use std::{collections::HashMap, num::NonZeroUsize, sync::Arc};

use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict},
};
use pyo3_asyncio::tokio::future_into_py;
use redis::{
    streams::{StreamMaxlen, StreamReadOptions},
    AsyncCommands, Value,
};

use crate::{client::Client, client_result::ClientResult, error, pool_manager::PoolManager, types};

#[derive(Clone)]
pub struct AsyncClientResult {
    pub(crate) cm: Arc<tokio::sync::RwLock<PoolManager>>,
}

impl ClientResult for AsyncClientResult {
    fn init<'a>(&self, py: Python<'a>, client: &Client) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        let client_id = client.client_id.clone();
        future_into_py(py, async move {
            cm.write().await.init().await;
            let result = Client {
                cr: Box::new(Self { cm }),
                client_id,
            };
            Ok(result)
        })
    }

    fn close<'a>(
        &self,
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

    fn status(&self) -> Result<HashMap<String, Value>, error::RedisError> {
        let cm = self.cm.try_read()?;
        let result = cm.status();
        Ok(result)
    }

    fn execute<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
        encoding: String,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let value = cm.pool.execute(&cmd, args).await?;
            Ok(Python::with_gil(|py| {
                types::to_object(py, value, &encoding)
            }))
        })
    }

    fn fetch_str<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Option<String> = cm.execute(&cmd, args).await?;
            Ok(result)
        })
    }

    fn fetch_bytes<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Vec<u8> = cm.execute(&cmd, args).await?;
            Ok(Python::with_gil(|py| {
                PyBytes::new(py, &result).to_object(py)
            }))
        })
    }

    fn fetch_list<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Vec<String> = cm.execute(&cmd, args).await?;
            Ok(result)
        })
    }

    fn fetch_dict<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
        encoding: String,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let value = cm.pool.execute(&cmd, args).await?;
            Ok(Python::with_gil(|py| types::to_dict(py, value, &encoding)))
        })
    }

    fn fetch_scores<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let map: HashMap<String, f64> = cm.execute(&cmd, args).await?;
            Ok(Python::with_gil(|py| {
                let result = PyDict::new(py);
                for (k, v) in map.into_iter() {
                    result.set_item(k, v).unwrap();
                }
                result.to_object(py)
            }))
        })
    }

    fn fetch_int<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: i64 = cm.execute(&cmd, args).await?;
            Ok(result)
        })
    }

    fn exists<'a>(&self, py: Python<'a>, key: types::Str) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: bool = c.exists(key).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    fn set<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: bool = c.set(key, value).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    fn get<'a>(&self, py: Python<'a>, key: String, encoding: String) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c.get(key).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::decode(py, result, &encoding)))
        })
    }

    fn incr<'a>(&self, py: Python<'a>, key: types::Str, delta: f64) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: f64 = c.incr(key, delta).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

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

    fn hget<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Str,
        encoding: String,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c.hget(key, field).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::decode(py, result, &encoding)))
        })
    }

    fn hgetall<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        encoding: String,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut cm = cm.read().await.get_connection().await?;
            let value = cm.hgetall(key).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::to_dict(py, value, &encoding)))
        })
    }

    fn lpush<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: bool = c.lpush(key, value).await.map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    fn lpop<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        count: Option<NonZeroUsize>,
        encoding: String,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c.lpop(key, count).await.map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| {
                types::to_object(py, result, &encoding)
            }))
        })
    }

    fn lrange<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        start: isize,
        stop: isize,
        encoding: String,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
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

    fn xadd<'a>(
        &self,
        py: Python<'a>,
        stream: types::Str,
        id: types::Str,
        map: HashMap<String, types::Arg>,
        maxlen: Option<StreamMaxlen>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: String = if let Some(ml) = maxlen {
                c.xadd_maxlen_map(stream, ml, id, map).await
            } else {
                c.xadd_map(stream, id, map).await
            }
            .map_err(error::RedisError::from)?;
            Ok(result)
        })
    }

    fn xread<'a>(
        &self,
        py: Python<'a>,
        streams: Vec<String>,
        ids: Vec<types::Arg>,
        options: StreamReadOptions,
        encoding: String,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result = c
                .xread_options(&streams, &ids, &options)
                .await
                .map_err(error::RedisError::from)?;
            Ok(Python::with_gil(|py| types::to_dict(py, result, &encoding)))
        })
    }

    fn xack<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        group: types::Str,
        id: Vec<types::Str>,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let mut c = cm.read().await.get_connection().await?;
            let result: usize = c
                .xack(key, group, &id)
                .await
                .map_err(error::RedisError::from)?;
            Ok(result)
        })
    }
}
