use pyo3::prelude::*;
mod bb8_cluster;
mod bb8_single;
mod client;
mod cluster;
mod deadpool_cluster;
mod error;
mod exceptions;
mod pool;
mod types;

#[pyfunction]
#[pyo3(signature = (*initial_nodes, max_size, cluster))]
fn create_client(
    mut initial_nodes: Vec<String>,
    max_size: Option<u32>,
    cluster: Option<bool>,
) -> PyResult<client::Client> {
    let is_cluster = match cluster {
        None => initial_nodes.len() > 1,
        Some(c) => c,
    };
    let mut cm = if is_cluster {
        client::ContextManager::new_cluster(initial_nodes)
    } else {
        let addr = initial_nodes.remove(0);
        client::ContextManager::new(addr)
    };
    if let Some(size) = max_size {
        cm.max_size = size;
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
