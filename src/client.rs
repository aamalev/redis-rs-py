use crate::{client_result::ClientResult, types};
use pyo3::{prelude::*, types::PyDict};
use std::{collections::HashMap, num::NonZeroUsize};

#[pyclass]
pub struct Client {
    pub(crate) cr: Box<dyn ClientResult + Send>,
}

#[pymethods]
impl Client {
    fn __aenter__<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        self.cr.init(py)
    }

    fn __aexit__<'a>(
        &self,
        py: Python<'a>,
        exc_type: &PyAny,
        exc_value: &PyAny,
        traceback: &PyAny,
    ) -> PyResult<&'a PyAny> {
        self.cr.close(py, exc_type, exc_value, traceback)
    }

    fn status(&self, py: Python) -> PyResult<PyObject> {
        let mut status = self.cr.status()?;
        let is_closed = status.remove("closed");
        let is_cluster = status.remove("cluster");
        let result = PyDict::new(py);
        for (k, v) in status.into_iter() {
            let value = types::to_object(py, v, "utf-8");
            result.set_item(k, value)?;
        }
        if let Some(redis::Value::Int(c)) = is_cluster {
            let is_cluster = c == 1;
            result.set_item("cluster", is_cluster.to_object(py))?;
        }
        if let Some(redis::Value::Int(c)) = is_closed {
            let is_closed = c == 1;
            result.set_item("closed", is_closed.to_object(py))?;
        }
        Ok(result.to_object(py))
    }

    fn get_encoding(&self, kwargs: Option<&PyDict>) -> String {
        let mut encoding = String::default();
        if let Some(kw) = kwargs {
            if let Some(val) = kw.get_item("encoding") {
                if let Ok(val) = val.extract() {
                    encoding = val;
                }
            }
        }
        encoding
    }

    #[pyo3(signature = (cmd, *args, **kwargs))]
    fn execute<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let encoding = self.get_encoding(kwargs);
        self.cr.execute(py, cmd, args, encoding)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_str<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        self.cr.fetch_str(py, cmd.into(), args)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_bytes<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        self.cr.fetch_bytes(py, cmd.into(), args)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_list<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        self.cr.fetch_list(py, cmd.into(), args)
    }

    #[pyo3(signature = (cmd, *args, **kwargs))]
    fn fetch_dict<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let encoding = self.get_encoding(kwargs);
        self.cr.fetch_dict(py, cmd, args, encoding)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_scores<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        self.cr.fetch_scores(py, cmd.into(), args)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_int<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        self.cr.fetch_int(py, cmd.into(), args)
    }

    #[pyo3(signature = (key))]
    fn exists<'a>(&self, py: Python<'a>, key: types::Str) -> PyResult<&'a PyAny> {
        self.cr.exists(py, key)
    }

    #[pyo3(signature = (key, value))]
    fn set<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        self.cr.set(py, key, value)
    }

    #[pyo3(signature = (key, **kwargs))]
    fn get<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        self.cr.get(py, key.into(), encoding)
    }

    #[pyo3(signature = (key, field, value))]
    fn hset<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Str,
        value: types::Arg,
    ) -> PyResult<&'a PyAny> {
        self.cr.hset(py, key, field, value)
    }

    #[pyo3(signature = (key, field, **kwargs))]
    fn hget<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        self.cr.hget(py, key, field, encoding)
    }

    #[pyo3(signature = (key, **kwargs))]
    fn hgetall<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        self.cr.hgetall(py, key, encoding)
    }

    #[pyo3(signature = (key, delta = 1.0))]
    fn incr<'a>(&self, py: Python<'a>, key: types::Str, delta: f64) -> PyResult<&'a PyAny> {
        self.cr.incr(py, key, delta)
    }

    #[pyo3(signature = (key, value))]
    fn lpush<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        self.cr.lpush(py, key, value)
    }

    #[pyo3(signature = (key, count = None, **kwargs))]
    fn lpop<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        count: Option<NonZeroUsize>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        self.cr.lpop(py, key, count, encoding)
    }

    #[pyo3(signature = (key, start = 0, stop = -1, **kwargs))]
    fn lrange<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        start: isize,
        stop: isize,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        self.cr.lrange(py, key, start, stop, encoding)
    }

    #[pyo3(signature = (stream, *args, id = None, items = None))]
    fn xadd<'a>(
        &self,
        py: Python<'a>,
        stream: types::Str,
        mut args: Vec<types::ScalarOrMap>,
        mut id: Option<types::Str>,
        items: Option<HashMap<String, types::Arg>>,
    ) -> PyResult<&'a PyAny> {
        args.push(types::ScalarOrMap::Map(items.unwrap_or_default()));
        let mut map = HashMap::new();
        let mut flat = vec![];

        for arg in args.into_iter() {
            match arg {
                types::ScalarOrMap::Map(aitems) => map.extend(aitems),
                types::ScalarOrMap::Scalar(s) => flat.push(s),
            };
        }

        if flat.len() % 2 == 1 {
            id = Some(types::Str::String(flat.remove(0).into()));
        }
        while !flat.is_empty() {
            map.insert(flat.remove(0).into(), flat.remove(0));
        }
        let id = id.unwrap_or(types::Str::String("*".to_string()));
        self.cr.xadd(py, stream, id, map)
    }

    #[pyo3(signature = (streams, *args, id=None, **kwargs))]
    fn xread<'a>(
        &self,
        py: Python<'a>,
        streams: types::ScalarOrMap,
        args: Vec<types::Str>,
        id: Option<types::Arg>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        let mut keys = vec![];
        let mut ids = vec![];
        let id = id.unwrap_or(types::Arg::Int(0));
        match streams {
            types::ScalarOrMap::Scalar(s) => {
                keys.push(String::from(s));
                for stream in args.into_iter() {
                    keys.push(String::from(stream));
                    ids.push(id.clone());
                }
                ids.push(id);
            }
            types::ScalarOrMap::Map(m) => {
                for (k, v) in m.into_iter() {
                    keys.push(k);
                    ids.push(v);
                }
            }
        }
        self.cr.xread(py, keys, ids, encoding)
    }
}
