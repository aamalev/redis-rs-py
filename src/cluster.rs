use deadpool_redis_cluster::{Config, Pool, Runtime};
use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict},
};
use pyo3_asyncio::tokio::future_into_py;
use redis_cluster_async::redis::FromRedisValue;
use std::{collections::HashMap, sync::Arc};

#[pyclass]
pub struct Client {
    pool: Arc<Pool>,
}

async fn execute<T: FromRedisValue>(
    pool: Arc<Pool>,
    cmd: String,
    args: Vec<Vec<u8>>,
) -> Result<T, Box<dyn std::error::Error>> {
    let mut conn = pool.get().await?;
    let result: T = redis_cluster_async::redis::cmd(&cmd)
        .arg(&args)
        .query_async(&mut conn)
        .await?;
    Ok(result)
}

#[pymethods]
impl Client {
    #[new]
    pub fn new(initial_nodes: Vec<String>, max_size: usize) -> Self {
        let cfg = Config::from_urls(initial_nodes);
        let pool = cfg
            .create_pool(Some(Runtime::Tokio1))
            .expect("Error with redis pool");
        pool.resize(max_size);
        let pool = Arc::new(pool);
        Self { pool }
    }

    #[args(args = "*")]
    fn fetch_str<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<Vec<u8>>,
    ) -> PyResult<&'a PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let result: Option<String> = execute(pool, cmd, args).await.expect("RedisError");
            Ok(result)
        })
    }

    #[args(args = "*")]
    fn fetch_bytes<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<Vec<u8>>,
    ) -> PyResult<&'a PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let result: Vec<u8> = execute(pool, cmd, args).await.expect("RedisError");
            Python::with_gil(|py| Ok(PyBytes::new(py, &result).to_object(py)))
        })
    }

    #[args(args = "*")]
    fn fetch_list<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<Vec<u8>>,
    ) -> PyResult<&'a PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let result: Vec<String> = execute(pool, cmd, args).await.expect("RedisError");
            Ok(result)
        })
    }

    #[args(args = "*")]
    fn fetch_dict<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<Vec<u8>>,
    ) -> PyResult<&'a PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let map: HashMap<String, Vec<u8>> = execute(pool, cmd, args).await.expect("RedisError");
            Ok(Python::with_gil(|py| {
                let result = PyDict::new(py);
                for (k, v) in map.into_iter() {
                    let val = PyBytes::new(py, &v);
                    result.set_item(k, val).unwrap();
                }
                result.to_object(py)
            }))
        })
    }

    #[args(args = "*")]
    fn fetch_int<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<Vec<u8>>,
    ) -> PyResult<&'a PyAny> {
        let pool = self.pool.clone();
        future_into_py(py, async move {
            let result: usize = execute(pool, cmd, args).await.expect("RedisError");
            Ok(result)
        })
    }
}
