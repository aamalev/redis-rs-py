use std::{collections::HashMap, num::NonZeroUsize};

use pyo3::prelude::*;
use redis::Value;

use crate::{error, types};

pub trait ClientResult {
    fn init<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny>;
    fn close<'a>(
        &self,
        py: Python<'a>,
        exc_type: &PyAny,
        exc_value: &PyAny,
        traceback: &PyAny,
    ) -> PyResult<&'a PyAny>;
    fn status(&self) -> Result<HashMap<String, Value>, error::RedisError>;
    fn execute<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
        encoding: String,
    ) -> PyResult<&'a PyAny>;
    fn fetch_str<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny>;
    fn fetch_bytes<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny>;
    fn fetch_list<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny>;
    fn fetch_dict<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
        encoding: String,
    ) -> PyResult<&'a PyAny>;
    fn fetch_scores<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny>;
    fn fetch_int<'a>(
        &self,
        py: Python<'a>,
        cmd: String,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny>;
    fn exists<'a>(&self, py: Python<'a>, key: types::Str) -> PyResult<&'a PyAny>;
    fn set<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny>;
    fn get<'a>(&self, py: Python<'a>, key: String, encoding: String) -> PyResult<&'a PyAny>;
    fn incr<'a>(&self, py: Python<'a>, key: types::Str, delta: f64) -> PyResult<&'a PyAny>;
    fn hset<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Str,
        value: types::Arg,
    ) -> PyResult<&'a PyAny>;
    fn hget<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Str,
        encoding: String,
    ) -> PyResult<&'a PyAny>;
    fn hgetall<'a>(&self, py: Python<'a>, key: types::Str, encoding: String)
        -> PyResult<&'a PyAny>;
    fn lpush<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny>;
    fn lpop<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        count: Option<NonZeroUsize>,
        encoding: String,
    ) -> PyResult<&'a PyAny>;
    fn lrange<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        start: isize,
        stop: isize,
        encoding: String,
    ) -> PyResult<&'a PyAny>;
    fn xadd<'a>(
        &self,
        py: Python<'a>,
        stream: types::Str,
        id: types::Str,
        items: HashMap<String, types::Arg>,
    ) -> PyResult<&'a PyAny>;
    fn xread<'a>(
        &self,
        py: Python<'a>,
        streams: Vec<String>,
        ids: Vec<types::Arg>,
        encoding: String,
    ) -> PyResult<&'a PyAny>;
}
