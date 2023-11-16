use std::collections::HashMap;

use pyo3::prelude::*;
use redis::{Cmd, Value};

use crate::{client::Client, error, types};

pub trait ClientResult {
    fn init<'a>(&self, py: Python<'a>, client: &Client) -> PyResult<&'a PyAny>;
    fn close<'a>(
        &self,
        py: Python<'a>,
        exc_type: &PyAny,
        exc_value: &PyAny,
        traceback: &PyAny,
    ) -> PyResult<&'a PyAny>;
    fn status(&self) -> Result<HashMap<String, Value>, error::RedisError>;
    fn execute<'a>(&self, py: Python<'a>, cmd: Cmd, encoding: types::Codec) -> PyResult<&'a PyAny>;
    fn fetch_str<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny>;
    fn fetch_bytes<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny>;
    fn fetch_list<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny>;
    fn fetch_dict<'a>(
        &self,
        py: Python<'a>,
        cmd: Cmd,
        encoding: types::Codec,
    ) -> PyResult<&'a PyAny>;
    fn fetch_scores<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny>;
    fn fetch_bool<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny>;
    fn fetch_int<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny>;
    fn fetch_float<'a>(&self, py: Python<'a>, cmd: Cmd) -> PyResult<&'a PyAny>;
}
