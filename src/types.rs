use deadpool_redis_cluster::redis::{RedisWrite, ToRedisArgs, Value};
use pyo3::{
    types::{PyBytes, PyList},
    FromPyObject, PyObject, Python, ToPyObject,
};

pub fn to_object(py: Python, value: Value) -> PyObject {
    match value {
        Value::Data(d) => PyBytes::new(py, &d).to_object(py),
        Value::Nil => py.None(),
        Value::Int(i) => i.to_object(py),
        Value::Bulk(bulk) => {
            PyList::new(py, bulk.into_iter().map(|v| to_object(py, v))).to_object(py)
        }
        Value::Status(s) => s.to_object(py),
        Value::Okay => true.to_object(py),
    }
}

#[derive(FromPyObject)]
pub enum Cmd {
    #[pyo3(transparent, annotation = "bytes")]
    Bytes(Vec<u8>),
    #[pyo3(transparent, annotation = "str")]
    String(String),
}

impl Cmd {
    pub fn to_string(self) -> String {
        match self {
            Cmd::Bytes(b) => String::from_utf8(b).unwrap(),
            Cmd::String(s) => s,
        }
    }
}

#[derive(FromPyObject)]
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
