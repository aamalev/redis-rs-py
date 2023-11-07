use std::collections::HashMap;

use pyo3::{
    types::{PyBytes, PyDict, PyList},
    FromPyObject, PyObject, Python, ToPyObject,
};
use redis::{FromRedisValue, RedisWrite, ToRedisArgs, Value};

use crate::error;

fn _decode(
    py: Python,
    v: Vec<u8>,
    encoding: &str,
) -> Result<PyObject, Box<dyn std::error::Error + 'static>> {
    match encoding {
        "utf-8" | "utf8" | "UTF8" | "UTF-8" => Ok(String::from_utf8(v)?.to_object(py)),
        "float" => Ok(String::from_utf8(v)?.parse::<f64>()?.to_object(py)),
        "int" => Ok(String::from_utf8(v)?.parse::<i64>()?.to_object(py)),
        "info" => {
            let result = PyDict::new(py);
            for (key, value) in String::from_utf8(v)?
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
            Ok(result.to_object(py))
        }
        _ => Ok(PyBytes::new(py, &v).to_object(py)),
    }
}

pub fn decode(py: Python, v: Vec<u8>, encoding: &str) -> PyObject {
    if let Ok(result) = _decode(py, v, encoding) {
        result
    } else {
        py.None()
    }
}

fn _to_dict(py: Python<'_>, value: Value, encoding: &str) -> PyObject {
    match value {
        Value::Data(v) => decode(py, v, encoding),
        Value::Nil => py.None(),
        Value::Int(i) => i.to_object(py),
        Value::Bulk(_) => to_dict(py, value, encoding),
        Value::Status(s) => s.to_object(py),
        Value::Okay => true.to_object(py),
    }
}

pub fn to_dict(py: Python, value: Value, encoding: &str) -> PyObject {
    let result = PyDict::new(py);
    let map: HashMap<String, Value> = FromRedisValue::from_redis_value(&value).unwrap_or_default();
    if !map.is_empty() {
        for (k, value) in map.into_iter() {
            let val = _to_dict(py, value, encoding);
            result.set_item(k, val).unwrap();
        }
    } else if let Value::Bulk(v) = value {
        for (n, value) in v.into_iter().enumerate() {
            let map: HashMap<String, Value> =
                FromRedisValue::from_redis_value(&value).unwrap_or_default();
            if map.len() == 1 {
                for (k, value) in map.into_iter() {
                    let val = _to_dict(py, value, encoding);
                    result.set_item(k, val).unwrap();
                }
            } else {
                let value = _to_dict(py, value, encoding);
                result.set_item(n, value).unwrap();
            }
        }
    }
    result.to_object(py)
}

pub fn to_object(py: Python, value: Value, encoding: &str) -> PyObject {
    match value {
        Value::Data(v) => decode(py, v, encoding),
        Value::Nil => py.None(),
        Value::Int(i) => i.to_object(py),
        Value::Bulk(bulk) => {
            PyList::new(py, bulk.into_iter().map(|v| to_object(py, v, encoding))).to_object(py)
        }
        Value::Status(s) => s.to_object(py),
        Value::Okay => true.to_object(py),
    }
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
            Str::Bytes(b) => String::from_utf8(b).unwrap(),
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
            Self::Bytes(b) => from_str(String::from_utf8(b.to_vec())?.as_str()),
            Self::Float(f) => Ok(f.to_string().replace('.', "-")),
            Self::Int(i) => Ok(i.to_string()),
        }
    }
}

impl From<Arg> for String {
    fn from(value: Arg) -> Self {
        match value {
            Arg::Bytes(b) => String::from_utf8(b).unwrap_or_default(),
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
}

impl ToRedisArgs for ScalarOrMap {
    fn write_redis_args<W>(&self, out: &mut W)
    where
        W: ?Sized + RedisWrite,
    {
        match self {
            ScalarOrMap::Map(m) => m.write_redis_args(out),
            ScalarOrMap::Scalar(a) => a.write_redis_args(out),
        }
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord)]
pub enum Feature {
    Shards,
    BB8,
    DeadPool,
}

impl TryFrom<String> for Feature {
    type Error = String;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "shards" => Ok(Feature::Shards),
            "bb8" => Ok(Feature::BB8),
            "deadpool" | "dead-pool" | "dead_pool" | "dp" => Ok(Feature::DeadPool),
            _ => Err("Unknown".to_string()),
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
