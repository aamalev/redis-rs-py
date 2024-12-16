use std::{collections::HashMap, sync::Arc};

use pyo3::{prelude::*, types::PyBytes};
use pyo3_asyncio::tokio::future_into_py;
use redis::{Cmd, Value};

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
            cm.write().await.init().await?;
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

    fn execute<'a>(&self, py: Python<'a>, cmd: Cmd, encoding: types::Codec) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let value = cm.pool.execute(cmd).await?;
            Python::with_gil(|py| {
                types::to_object(py, value, encoding)
            })
        })
    }

    fn fetch_str<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Option<String> = cm.execute(cmd).await?;
            Ok(result)
        })
    }

    fn fetch_bytes<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Vec<u8> = cm.execute(cmd).await?;
            Ok(Python::with_gil(|py| {
                PyBytes::new(py, &result).to_object(py)
            }))
        })
    }

    fn fetch_list<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: Vec<String> = cm.execute(cmd).await?;
            Ok(result)
        })
    }

    fn fetch_dict<'a>(
        &self,
        py: Python<'a>,
        cmd: Cmd,
        encoding: types::Codec,
    ) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let value = cm.pool.execute(cmd).await?;
            Python::with_gil(|py| types::to_dict(py, value, encoding))
        })
    }

    fn fetch_scores<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let map: HashMap<String, f64> = cm.execute(cmd).await?;
            Ok(Python::with_gil(|py| map.to_object(py)))
        })
    }

    fn fetch_bool<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: bool = cm.execute(cmd).await?;
            Ok(result)
        })
    }

    fn fetch_int<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: i64 = cm.execute(cmd).await?;
            Ok(result)
        })
    }

    fn fetch_float<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny> {
        let cm = self.cm.clone();
        future_into_py(py, async move {
            let cm = cm.read().await;
            let result: f64 = cm.execute(cmd).await?;
            Ok(result)
        })
    }
}
