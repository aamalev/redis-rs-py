#[derive(Debug)]
pub enum RedisError {
    #[allow(clippy::enum_variant_names)]
    RedisError(redis::RedisError),
    CommandError(String),
    PoolError(redis::RedisError),
    NotFoundNode,
    NoSlot,
}

impl RedisError {
    pub fn not_initialized() -> Self {
        Self::PoolError(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "Not initioalized pool",
        )))
    }
}

impl From<redis::RedisError> for RedisError {
    fn from(error: redis::RedisError) -> Self {
        RedisError::RedisError(error)
    }
}

impl From<bb8::RunError<redis::RedisError>> for RedisError {
    fn from(error: bb8::RunError<redis::RedisError>) -> Self {
        match error {
            bb8::RunError::User(err) => RedisError::PoolError(err),
            bb8::RunError::TimedOut => RedisError::PoolError(redis::RedisError::from((
                redis::ErrorKind::IoError,
                "Timed out in bb8",
            ))),
        }
    }
}

impl From<tokio::sync::TryLockError> for RedisError {
    fn from(_e: tokio::sync::TryLockError) -> Self {
        RedisError::PoolError(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "Try leter",
        )))
    }
}

impl From<tokio::sync::AcquireError> for RedisError {
    fn from(_e: tokio::sync::AcquireError) -> Self {
        RedisError::PoolError(redis::RedisError::from((
            redis::ErrorKind::IoError,
            "Try leter",
        )))
    }
}

impl From<RedisError> for redis::RedisError {
    fn from(e: RedisError) -> Self {
        match e {
            RedisError::RedisError(e) => e,
            RedisError::NotFoundNode => {
                redis::RedisError::from((redis::ErrorKind::IoError, "Not found node"))
            }
            RedisError::NoSlot => {
                redis::RedisError::from((redis::ErrorKind::IoError, "Not found slot"))
            }
            RedisError::PoolError(e) => e,
            RedisError::CommandError(_) => todo!(),
        }
    }
}

#[derive(Debug)]
pub(crate) struct ValueError {
    _err: String,
}

impl From<std::string::FromUtf8Error> for ValueError {
    fn from(value: std::string::FromUtf8Error) -> Self {
        Self {
            _err: value.to_string(),
        }
    }
}

impl From<std::num::ParseIntError> for ValueError {
    fn from(value: std::num::ParseIntError) -> Self {
        Self {
            _err: value.to_string(),
        }
    }
}

impl From<std::num::ParseFloatError> for ValueError {
    fn from(value: std::num::ParseFloatError) -> Self {
        Self {
            _err: value.to_string(),
        }
    }
}

impl From<serde_json::Error> for ValueError {
    fn from(value: serde_json::Error) -> Self {
        Self {
            _err: value.to_string(),
        }
    }
}

impl From<pyo3::PyErr> for ValueError {
    fn from(value: pyo3::PyErr) -> Self {
        Self {
            _err: value.to_string(),
        }
    }
}
