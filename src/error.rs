use bb8::RunError;

#[derive(Debug)]
pub enum RedisError {
    CommandError(String),
    PoolError(String),
}

impl RedisError {
    pub fn not_initialized() -> Self {
        Self::PoolError("Not initioalized pool".to_string())
    }
}

impl From<redis::RedisError> for RedisError {
    fn from(error: redis::RedisError) -> Self {
        RedisError::CommandError(error.to_string())
    }
}

impl From<RunError<redis::RedisError>> for RedisError {
    fn from(error: RunError<redis::RedisError>) -> Self {
        RedisError::PoolError(error.to_string())
    }
}

impl From<tokio::sync::TryLockError> for RedisError {
    fn from(_e: tokio::sync::TryLockError) -> Self {
        RedisError::PoolError("Try leter".to_string())
    }
}

impl From<tokio::sync::AcquireError> for RedisError {
    fn from(_e: tokio::sync::AcquireError) -> Self {
        RedisError::PoolError("Try leter".to_string())
    }
}
