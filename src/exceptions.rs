use crate::error;
use pyo3::{create_exception, types::PyModule, PyErr, PyResult, Python};

create_exception!(exceptions, RedisError, pyo3::exceptions::PyException);
create_exception!(exceptions, PoolError, pyo3::exceptions::PyException);

pub fn exceptions(py: Python, m: &PyModule) -> PyResult<()> {
    m.add("RedisError", py.get_type::<RedisError>())?;
    m.add("PoolError", py.get_type::<PoolError>())?;
    Ok(())
}

impl From<error::RedisError> for PyErr {
    fn from(e: error::RedisError) -> Self {
        match e {
            error::RedisError::CommandError(s) => RedisError::new_err(s),
            error::RedisError::PoolError(e) => PoolError::new_err(e.to_string()),
            error::RedisError::RedisError(e) => RedisError::new_err(e.to_string()),
        }
    }
}
