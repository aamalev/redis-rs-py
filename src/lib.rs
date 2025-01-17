use config::Config;
use pyo3::prelude::*;
use redis::IntoConnectionInfo;
mod client;
mod client_result;
mod client_result_async;
mod cluster_async;
mod cluster_bb8;
mod config;
mod error;
mod exceptions;
mod pool;
mod pool_manager;
mod shards;
mod shards_async;
mod single_bb8;
mod single_node;
mod types;

#[pyfunction]
#[pyo3(signature = (
    *initial_nodes,
    max_size=None,
    cluster=None,
    username = None,
    password = None,
    db = None,
    client_id=None,
    max_delay=None,
    features=None,
))]
#[allow(clippy::too_many_arguments)]
fn create_client(
    initial_nodes: Vec<String>,
    max_size: Option<u32>,
    cluster: Option<bool>,
    username: Option<String>,
    password: Option<String>,
    db: Option<i64>,
    client_id: Option<String>,
    max_delay: Option<u64>,
    features: Option<Vec<String>>,
) -> PyResult<client::Client> {
    let mut nodes = initial_nodes.clone();
    if nodes.is_empty() {
        nodes.push("redis://localhost:6379".to_string());
    }
    let mut infos = vec![];
    for i in nodes.into_iter() {
        let mut info = i.into_connection_info().map_err(error::RedisError::from)?;
        if password.is_some() {
            info.redis.password = password.clone();
        }
        if username.is_some() {
            info.redis.username = username.clone();
        }
        if let Some(db) = db {
            info.redis.db = db;
        }
        infos.push(info);
    }

    let mut cfg = Config {
        initial_nodes: infos,
        max_size: max_size.unwrap_or(10),
        cluster,
        client_id: client_id.unwrap_or_default(),
        max_delay,
        ..Default::default()
    };

    if let Some(ref features) = features {
        cfg.set_features(features);
    }

    let cm = pool_manager::PoolManager::new(cfg)?;
    Ok(cm.into())
}

#[pymodule]
#[pyo3(name = "redis_rs")]
fn redis_rs(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_client, m)?)?;
    m.add_class::<client::Client>()?;
    let exceptions = PyModule::new(py, "exceptions")?;
    m.add_submodule(exceptions)?;
    crate::exceptions::exceptions(py, exceptions)?;
    Ok(())
}
