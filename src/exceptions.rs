use crate::error;
use pyo3::{create_exception, PyErr};

create_exception!(exceptions, RedisError, pyo3::exceptions::PyException);
create_exception!(exceptions, PoolError, pyo3::exceptions::PyException);

impl From<error::RedisError> for PyErr {
    fn from(e: error::RedisError) -> Self {
        match e {
            error::RedisError::CommandError(s) => RedisError::new_err(s),
            error::RedisError::PoolError(e) => PoolError::new_err(e.to_string()),
            error::RedisError::RedisError(e) => {
                let args = match (e.code(), e.detail()) {
                    (Some("ERR"), Some(detail)) => detail.to_string(),
                    (Some(code), Some(detail)) => format!("{code} {detail}"),
                    (Some(code), None) => code.to_string(),
                    _ => e.to_string(),
                };
                RedisError::new_err(args)
            }
            error::RedisError::NotFoundNode => PoolError::new_err("Not found node".to_string()),
            error::RedisError::NoSlot => PoolError::new_err("Not found slot".to_string()),
        }
    }
}
