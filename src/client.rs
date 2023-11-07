use crate::{client_result::ClientResult, types};
use pyo3::{prelude::*, types::PyDict};
use redis::streams::{StreamMaxlen, StreamReadOptions};
use std::{collections::HashMap, num::NonZeroUsize};

#[pyclass]
pub struct Client {
    pub(crate) cr: Box<dyn ClientResult + Send>,
    #[pyo3(get)]
    pub client_id: String,
}

#[pymethods]
impl Client {
    fn __aenter__<'a>(&self, py: Python<'a>) -> PyResult<&'a PyAny> {
        self.cr.init(py, self)
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
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let encoding = self.get_encoding(kwargs);
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_str<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        self.cr.fetch_str(py, cmd)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_bytes<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        self.cr.fetch_bytes(py, cmd)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_list<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        self.cr.fetch_list(py, cmd)
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
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let encoding = self.get_encoding(kwargs);
        self.cr.fetch_dict(py, cmd, encoding)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_scores<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        self.cr.fetch_scores(py, cmd)
    }

    #[pyo3(signature = (cmd, *args))]
    fn fetch_int<'a>(
        &self,
        py: Python<'a>,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[pyo3(signature = (key))]
    fn exists<'a>(&self, py: Python<'a>, key: types::Str) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("EXISTS").arg(key).to_owned();
        self.cr.execute(py, cmd, String::default())
    }

    #[pyo3(signature = (key, value))]
    fn set<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("SET").arg(key).arg(value).to_owned();
        self.cr.execute(py, cmd, String::default())
    }

    #[pyo3(signature = (key, **kwargs))]
    fn get<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        let cmd = redis::cmd("GET").arg(key).to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (key, *pairs, mapping = None))]
    fn hset<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        pairs: Vec<types::ScalarOrMap>,
        mapping: Option<types::ScalarOrMap>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("HSET")
            .arg(key)
            .arg(pairs)
            .arg(mapping)
            .to_owned();
        self.cr.fetch_int(py, cmd)
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
        let cmd = redis::cmd("HGET").arg(key).arg(field).to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (key, *fields, **kwargs))]
    fn hmget<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        fields: Vec<types::Str>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        let cmd = redis::cmd("HMGET").arg(key).arg(fields).to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (key, **kwargs))]
    fn hgetall<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        let cmd = redis::cmd("HGETALL").arg(key).to_owned();
        self.cr.fetch_dict(py, cmd, encoding)
    }

    #[pyo3(signature = (key, field))]
    fn hexists<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        field: types::Arg,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("HEXISTS").arg(key).arg(field).to_owned();
        self.cr.fetch_bool(py, cmd)
    }

    #[pyo3(signature = (key, *fields))]
    fn hdel<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        fields: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("HDEL").arg(key).arg(fields).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[pyo3(signature = (key, delta = None))]
    fn incr<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        delta: Option<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = match delta {
            None => redis::cmd("INCR").arg(key).to_owned(),
            Some(types::Arg::Bytes(b)) => redis::cmd("INCRBYFLOAT").arg(key).arg(b).to_owned(),
            Some(types::Arg::String(s)) => redis::cmd("INCRBYFLOAT").arg(key).arg(s).to_owned(),
            Some(types::Arg::Float(f)) => redis::cmd("INCRBYFLOAT").arg(key).arg(f).to_owned(),
            Some(types::Arg::Int(i)) => redis::cmd("INCRBY").arg(key).arg(i).to_owned(),
        };
        self.cr.fetch_float(py, cmd)
    }

    #[pyo3(signature = (key, value))]
    fn lpush<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("LPUSH").arg(key).arg(value).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[pyo3(signature = (key, value))]
    fn rpush<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("RPUSH").arg(key).arg(value).to_owned();
        self.cr.fetch_int(py, cmd)
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
        let cmd = redis::cmd("LPOP").arg(key).arg(count).to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (*keys, timeout, **kwargs))]
    fn blpop<'a>(
        &self,
        py: Python<'a>,
        keys: Vec<types::Str>,
        timeout: f64,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = self.get_encoding(kwargs);
        let cmd = redis::cmd("BLPOP").arg(keys).arg(timeout).to_owned();
        self.cr.execute(py, cmd, encoding)
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
        let cmd = redis::cmd("LRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (key))]
    fn llen<'a>(&self, py: Python<'a>, key: types::Str) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("LLEN").arg(key).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (stream, *args, id = None, items = None, maxlen = None, approx = true))]
    fn xadd<'a>(
        &self,
        py: Python<'a>,
        stream: types::Str,
        mut args: Vec<types::ScalarOrMap>,
        mut id: Option<types::Str>,
        items: Option<HashMap<String, types::Arg>>,
        maxlen: Option<usize>,
        approx: bool,
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
        let maxlen = maxlen.map(|x| {
            if approx {
                StreamMaxlen::Approx(x)
            } else {
                StreamMaxlen::Equals(x)
            }
        });
        let id = id.unwrap_or(types::Str::String("*".to_string()));
        let cmd = redis::cmd("XADD")
            .arg(stream)
            .arg(maxlen)
            .arg(id)
            .arg(map)
            .to_owned();
        self.cr.fetch_str(py, cmd)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (streams, *args, id=None, block=None, count=None, noack=None, group=None, encoding=None))]
    fn xread<'a>(
        &self,
        py: Python<'a>,
        streams: types::ScalarOrMap,
        args: Vec<types::Str>,
        id: Option<types::Arg>,
        block: Option<usize>,
        count: Option<usize>,
        noack: Option<bool>,
        group: Option<types::Str>,
        encoding: Option<String>,
    ) -> PyResult<&'a PyAny> {
        let encoding = encoding.unwrap_or_default();
        let mut id = id.unwrap_or(types::Arg::Int(0));
        let mut options = StreamReadOptions::default();
        if let Some(ms) = block {
            options = options.block(ms);
        }
        if let Some(n) = count {
            options = options.count(n);
        }
        if let Some(true) = noack {
            options = options.noack();
        }
        let mut cmd = redis::cmd(if let Some(g) = group {
            options = options.group(g, self.client_id.clone());
            id = types::Arg::Bytes(b">".to_vec());
            "XREADGROUP"
        } else {
            "XREAD"
        });
        let mut keys = vec![];
        let mut ids = vec![];
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
        cmd.arg(options).arg("STREAMS").arg(keys).arg(ids);
        self.cr.fetch_dict(py, cmd, encoding)
    }

    #[pyo3(signature = (key, group, *id))]
    fn xack<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        group: types::Str,
        id: Vec<types::Str>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("XACK").arg(key).arg(group).arg(id).to_owned();
        self.cr.fetch_int(py, cmd)
    }
}
