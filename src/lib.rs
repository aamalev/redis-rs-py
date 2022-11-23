use pyo3::prelude::*;
mod cluster;

#[pyfunction]
fn create_client(initial_nodes: Vec<String>, max_size: usize) -> PyResult<cluster::Client> {
    Ok(cluster::Client::new(initial_nodes, max_size))
}

#[pymodule]
fn redis_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(create_client, m)?)?;
    m.add_class::<cluster::Client>()?;
    Ok(())
}
