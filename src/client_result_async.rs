use std::{collections::HashMap, sync::Arc};

use pyo3::prelude::*;
use redis::{Cmd, FromRedisValue, Value};

use crate::{client_async::Client, command::Params, error, pool_manager::PoolManager, types};

#[derive(Clone)]
pub struct AsyncClientResult {
    pub(crate) cm: Arc<tokio::sync::RwLock<PoolManager>>,
}

fn tokio_rt() -> &'static tokio::runtime::Runtime {
    use std::sync::OnceLock;
    static RT: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RT.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

impl AsyncClientResult {
    pub async fn init(&self, client: &Client) -> PyResult<Client> {
        let cm = self.cm.clone();
        tokio_rt()
            .spawn(async move { cm.write().await.init().await })
            .await
            .unwrap()?;
        Ok(client.clone())
    }

    pub async fn close(&self) -> PyResult<()> {
        let cm = self.cm.clone();
        tokio_rt()
            .spawn(async move { cm.write().await.close().await })
            .await
            .unwrap();
        Ok(())
    }

    pub fn status(&self) -> Result<HashMap<String, Value>, error::RedisError> {
        let cm = self.cm.try_read()?;
        let result = cm.status();
        Ok(result)
    }

    pub async fn execute(&self, cmd: Cmd, params: Params) -> PyResult<Py<PyAny>> {
        let cm = self.cm.clone();
        let encoding = params.codec.clone();
        let result = tokio_rt()
            .spawn(async move { cm.read().await.pool.execute(cmd, params).await })
            .await
            .unwrap()?;
        Python::attach(|py| types::to_object(py, result, encoding))
    }

    pub async fn fetch_dict(&self, cmd: Cmd, params: Params) -> PyResult<Py<PyAny>> {
        let cm = self.cm.clone();
        let encoding = params.codec.clone();
        let result = tokio_rt()
            .spawn(async move { cm.read().await.pool.execute(cmd, params).await })
            .await
            .unwrap()?;
        Python::attach(|py| types::to_dict(py, result, encoding))
    }

    pub async fn fetch<T>(&self, cmd: Cmd, params: Params) -> PyResult<T>
    where
        T: FromRedisValue + Send + 'static,
    {
        let cm = self.cm.clone();
        let result = tokio_rt()
            .spawn(async move { cm.read().await.execute(cmd, params).await })
            .await
            .unwrap()?;
        Ok(result)
    }
}
