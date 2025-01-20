use std::collections::HashMap;

use pyo3::{
    prelude::*,
    types::{PyBytes, PyDict, PyDictMethods, PyList, PySet, PySetMethods},
    FromPyObject, IntoPyObjectExt, PyAny, PyObject, PyResult, Python,
};
use redis::{FromRedisValue, RedisWrite, ToRedisArgs, Value};

use crate::error;

#[derive(Clone, Default)]
pub enum Codec {
    #[default]
    Bytes,
    String,
    Int,
    Float,
    Info,
    Json,
}

impl From<&str> for Codec {
    fn from(value: &str) -> Self {
        match value {
            "utf-8" | "utf8" | "UTF8" | "UTF-8" | "str" => Codec::String,
            "float" => Codec::Float,
            "int" => Codec::Int,
            "info" => Codec::Info,
            "json" => Codec::Json,
            _ => Codec::Bytes,
        }
    }
}

impl From<Option<String>> for Codec {
    fn from(value: Option<String>) -> Self {
        value.map(|v| Codec::from(v.as_str())).unwrap_or_default()
    }
}

impl From<Option<&Bound<'_, PyDict>>> for Codec {
    fn from(kwargs: Option<&Bound<'_, PyDict>>) -> Self {
        kwargs
            .map(|kw| {
                kw.get_item("encoding")
                    .map(|val| {
                        val.map(|v| v.extract::<&str>().map(Codec::from).unwrap_or_default())
                            .unwrap_or_default()
                    })
                    .unwrap_or_default()
            })
            .unwrap_or_default()
    }
}

fn _decode(py: Python, v: Vec<u8>, codec: Codec) -> Result<PyObject, error::ValueError> {
    match codec {
        Codec::String => Ok(String::from_utf8_lossy(&v).into_py_any(py)?),
        Codec::Float => Ok(String::from_utf8_lossy(&v)
            .parse::<f64>()?
            .into_py_any(py)?),
        Codec::Int => Ok(String::from_utf8_lossy(&v)
            .parse::<i64>()?
            .into_py_any(py)?),
        Codec::Info => {
            let result = PyDict::new(py);
            for (key, value) in String::from_utf8_lossy(&v)
                .split("\r\n")
                .filter_map(|x| x.split_once(':'))
            {
                if (value.len() > 1) & value.starts_with('0') & !value.starts_with("0.") {
                    result.set_item(key, value)?;
                } else if let Ok(value) = value.parse::<i64>() {
                    result.set_item(key, value)?;
                } else if let Ok(value) = value.parse::<f64>() {
                    result.set_item(key, value)?;
                } else {
                    result.set_item(key, value)?;
                }
            }
            Ok(result.into_py_any(py)?)
        }
        Codec::Bytes => Ok(PyBytes::new(py, &v).into_py_any(py)?),
        Codec::Json => {
            let v: serde_json::Value =
                serde_json::from_slice(&v).map_err(error::ValueError::from)?;
            from_json(py, v)
        }
    }
}

fn from_json(py: Python<'_>, v: serde_json::Value) -> Result<PyObject, error::ValueError> {
    Ok(match v {
        serde_json::Value::Null => py.None(),
        serde_json::Value::Bool(b) => b.into_py_any(py)?,
        serde_json::Value::Number(n) => {
            if n.is_f64() {
                n.as_f64().into_py_any(py)?
            } else if n.is_i64() {
                n.as_i64().into_py_any(py)?
            } else {
                n.as_u64().into_py_any(py)?
            }
        }
        serde_json::Value::String(s) => s.into_py_any(py)?,
        serde_json::Value::Array(a) => {
            let mut result = vec![];
            for v in a.into_iter() {
                result.push(from_json(py, v)?);
            }
            PyList::new(py, result)?.into_any().unbind()
        }
        serde_json::Value::Object(m) => {
            let result = PyDict::new(py);
            for (k, v) in m.into_iter() {
                result.set_item(k, from_json(py, v)?)?;
            }
            result.into_py_any(py)?
        }
    })
}

pub fn decode(py: Python, v: Vec<u8>, codec: Codec) -> PyObject {
    if let Ok(result) = _decode(py, v, codec) {
        result
    } else {
        py.None()
    }
}

fn _to_dict(py: Python<'_>, value: Value, codec: Codec) -> PyResult<PyObject> {
    let result = match value {
        Value::BulkString(v) => decode(py, v, codec),
        Value::Nil => py.None(),
        Value::Int(i) => i.into_py_any(py)?,
        Value::Array(_) => to_dict(py, value, codec)?,
        Value::SimpleString(s) => s.into_py_any(py)?,
        Value::Okay => true.into_py_any(py)?,
        Value::Map(_) => to_dict(py, value, codec)?,
        Value::Set(_) => to_object(py, value, codec)?,
        Value::Double(f) => f.into_py_any(py)?,
        Value::Boolean(b) => b.into_py_any(py)?,
        Value::Attribute {
            data: _,
            attributes: _,
        } => todo!(),
        Value::VerbatimString { format: _, text: _ } => todo!(),
        Value::BigNumber(_) => todo!(),
        Value::Push { kind: _, data: _ } => todo!(),
        Value::ServerError(err) => Err(error::RedisError::RedisError(err.into()))?,
    };
    Ok(result)
}

pub fn to_dict(py: Python, value: Value, codec: Codec) -> PyResult<PyObject> {
    if let Value::ServerError(err) = value {
        Err(error::RedisError::RedisError(err.into()))?
    } else {
        let result = PyDict::new(py);
        let map: HashMap<String, Value> =
            FromRedisValue::from_redis_value(&value).unwrap_or_default();
        if !map.is_empty() {
            for (k, value) in map.into_iter() {
                let val = _to_dict(py, value, codec.clone())?;
                result.set_item(k, val)?;
            }
        } else if let Value::Array(v) = value {
            for (n, value) in v.into_iter().enumerate() {
                let map: HashMap<String, Value> =
                    FromRedisValue::from_redis_value(&value).unwrap_or_default();
                if map.len() == 1 {
                    for (k, value) in map.into_iter() {
                        let val = _to_dict(py, value, codec.clone())?;
                        result.set_item(k, val)?;
                    }
                } else {
                    let value = _to_dict(py, value, codec.clone())?;
                    result.set_item(n, value)?;
                }
            }
        }
        Ok(result.into_py_any(py)?)
    }
}

pub fn to_object(py: Python, value: Value, codec: Codec) -> PyResult<Py<PyAny>> {
    let result = match value {
        Value::BulkString(v) => decode(py, v, codec),
        Value::Nil => py.None(),
        Value::Int(i) => i.into_py_any(py)?,
        Value::Array(bulk) => {
            let mut result = vec![];
            for v in bulk.into_iter() {
                result.push(to_object(py, v, codec.clone())?);
            }
            PyList::new(py, result)?.into_py_any(py)?
        }
        Value::SimpleString(s) => s.into_py_any(py)?,
        Value::Okay => true.into_py_any(py)?,
        Value::Map(_) => to_dict(py, value, codec)?,
        Value::Set(s) => {
            let result = PySet::empty(py)?;
            for v in s.into_iter() {
                let _ = result.add(to_object(py, v, codec.clone())?);
            }
            result.into_py_any(py)?
        }
        Value::Double(f) => f.into_py_any(py)?,
        Value::Boolean(b) => b.into_py_any(py)?,
        Value::Attribute {
            data: _,
            attributes: _,
        } => todo!(),
        Value::VerbatimString { format: _, text: _ } => todo!(),
        Value::BigNumber(_) => todo!(),
        Value::Push { kind: _, data: _ } => todo!(),
        Value::ServerError(err) => Err(error::RedisError::RedisError(err.into()))?,
    };
    Ok(result)
}

#[derive(FromPyObject)]
pub enum Str {
    #[pyo3(transparent, annotation = "bytes")]
    Bytes(Vec<u8>),
    #[pyo3(transparent, annotation = "str")]
    String(String),
}

impl From<Str> for String {
    fn from(value: Str) -> Self {
        match value {
            Str::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
            Str::String(s) => s,
        }
    }
}

impl ToRedisArgs for Str {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Str::Bytes(b) => out.write_arg(b),
            Str::String(s) => out.write_arg(s.as_bytes()),
        }
    }
}

#[derive(FromPyObject, Clone, Debug)]
pub enum Arg {
    #[pyo3(transparent, annotation = "bytes")]
    Bytes(Vec<u8>),
    #[pyo3(transparent, annotation = "str")]
    String(String),
    #[pyo3(transparent, annotation = "float")]
    Float(f64),
    #[pyo3(transparent, annotation = "int")]
    Int(i64),
}

impl Arg {
    pub fn to_vec(&self) -> Vec<u8> {
        match self {
            Arg::Bytes(b) => b.clone(),
            Arg::String(s) => s.clone().into_bytes(),
            Arg::Float(f) => f.to_string().into_bytes(),
            Arg::Int(i) => i.to_string().into_bytes(),
        }
    }
}

impl Arg {
    pub(crate) fn to_normalized_stream_msg_id(&self) -> Result<String, error::ValueError> {
        fn from_str(s: &str) -> Result<String, error::ValueError> {
            if !s.eq("*") {
                if let Some(s) = s.strip_suffix("-*") {
                    s.parse::<u64>()?;
                } else {
                    s.replace('-', ".").parse::<f64>()?;
                }
            }
            Ok(s.to_string())
        }

        match self {
            Self::String(s) => from_str(s),
            Self::Bytes(b) => from_str(&String::from_utf8_lossy(b)),
            Self::Float(f) => Ok(f.to_string().replace('.', "-")),
            Self::Int(i) => Ok(i.to_string()),
        }
    }
}

impl From<Arg> for String {
    fn from(value: Arg) -> Self {
        match value {
            Arg::Bytes(b) => String::from_utf8_lossy(&b).to_string(),
            Arg::String(s) => s,
            Arg::Float(f) => f.to_string(),
            Arg::Int(i) => i.to_string(),
        }
    }
}

impl ToRedisArgs for Arg {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            Arg::Bytes(b) => out.write_arg(b),
            Arg::String(s) => out.write_arg(s.as_bytes()),
            Arg::Float(f) => out.write_arg(f.to_string().as_bytes()),
            Arg::Int(i) => out.write_arg(i.to_string().as_bytes()),
        }
    }
}

#[derive(FromPyObject)]
pub enum ScalarOrMap {
    Scalar(Arg),
    Map(HashMap<String, Arg>),
    BMap(HashMap<Vec<u8>, Arg>),
}

impl ScalarOrMap {
    pub fn write_val_key(&self, mut cmd: redis::Cmd) -> redis::Cmd {
        match self {
            Self::Map(m) => {
                for (k, v) in m.iter() {
                    cmd.arg(v);
                    cmd.arg(k);
                }
            }
            Self::BMap(m) => {
                for (k, v) in m.iter() {
                    cmd.arg(v);
                    cmd.arg(k);
                }
            }
            Self::Scalar(arg) => {
                cmd.arg(arg);
            }
        };
        cmd
    }
}

impl ToRedisArgs for ScalarOrMap {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            ScalarOrMap::BMap(m) => m.write_redis_args(out),
            ScalarOrMap::Map(m) => m.write_redis_args(out),
            ScalarOrMap::Scalar(a) => a.write_redis_args(out),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::types::Arg;

    #[test]
    fn stream_msg_id_star() {
        let id = Arg::Bytes(b"*".to_vec());

        assert_eq!(id.to_normalized_stream_msg_id().unwrap(), "*".to_string(),);
    }

    #[test]
    fn stream_msg_id_tail_star() {
        let id = Arg::Bytes(b"1-*".to_vec());

        assert_eq!(id.to_normalized_stream_msg_id().unwrap(), "1-*".to_string(),);
    }

    #[test]
    fn stream_msg_id_double_star() {
        let id = Arg::Bytes(b"*-*".to_vec());

        assert!(id.to_normalized_stream_msg_id().is_err());
    }

    #[test]
    fn stream_msg_id_float() {
        let id = Arg::Float(1.2345678);

        assert_eq!(
            id.to_normalized_stream_msg_id().unwrap(),
            "1-2345678".to_string(),
        );
    }

    #[test]
    fn stream_msg_id_float_int() {
        let id = Arg::Float(1.0);

        assert_eq!(id.to_normalized_stream_msg_id().unwrap(), "1".to_string(),);
    }

    #[test]
    fn stream_msg_id_int() {
        let id = Arg::Int(1);

        assert_eq!(id.to_normalized_stream_msg_id().unwrap(), "1".to_string(),);
    }

    #[test]
    fn stream_msg_id_str() {
        let id = Arg::String("1".to_string());

        assert_eq!(id.to_normalized_stream_msg_id().unwrap(), "1".to_string(),);
    }

    #[test]
    fn stream_msg_id_bytes() {
        let id = Arg::Bytes(b"1".to_vec());

        assert_eq!(id.to_normalized_stream_msg_id().unwrap(), "1".to_string(),);
    }

    #[test]
    fn stream_msg_id_alfa() {
        let id = Arg::Bytes(b"a".to_vec());

        assert!(id.to_normalized_stream_msg_id().is_err());
    }
}
