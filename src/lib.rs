use pyo3::prelude::*;
mod client;
mod client_result;
mod client_result_async;
mod cluster;
mod cluster_bb8;
mod cluster_deadpool;
mod error;
mod exceptions;
mod pool;
mod pool_manager;
mod single_bb8;
mod types;

#[pyfunction]
#[pyo3(signature = (*initial_nodes, max_size=None, cluster=None, client_id=None))]
fn create_client(
    initial_nodes: Vec<String>,
    max_size: Option<u32>,
    cluster: Option<bool>,
    client_id: Option<String>,
) -> PyResult<client::Client> {
    let is_cluster = match cluster {
        None => initial_nodes.len() > 1,
        Some(c) => c,
    };
    let mut cm = if is_cluster {
        pool_manager::PoolManager::new_cluster(initial_nodes)
    } else {
        let addr = initial_nodes
            .get(0)
            .map(String::as_str)
            .unwrap_or("redis://localhost:6379");
        pool_manager::PoolManager::new(addr)
    };
    if let Some(size) = max_size {
        cm.max_size = size;
    }
    if let Some(client_id) = client_id {
        cm.client_id = client_id;
    }
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
