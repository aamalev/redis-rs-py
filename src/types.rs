use std::collections::HashMap;

use pyo3::{
    types::{PyBytes, PyDict, PyList},
    FromPyObject, PyObject, Python, ToPyObject,
};
use redis::{FromRedisValue, RedisWrite, ToRedisArgs, Value};

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

#[derive(FromPyObject, Clone)]
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
