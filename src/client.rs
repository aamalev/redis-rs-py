use crate::{client_result::ClientResult, types};
use pyo3::{prelude::*, types::PyDict};
use redis::streams::StreamReadOptions;
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
        let is_auth = status.remove("auth");
        let result = PyDict::new(py);
        for (k, v) in status.into_iter() {
            let value = types::to_object(py, v, types::Codec::String);
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
        if let Some(redis::Value::Int(c)) = is_auth {
            let is_auth = c == 1;
            result.set_item("auth", is_auth.to_object(py))?;
        }
        Ok(result.to_object(py))
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
        let encoding = types::Codec::from(kwargs);
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
        let encoding = types::Codec::from(kwargs);
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

    #[pyo3(signature = (*keys))]
    fn exists<'a>(&self, py: Python<'a>, keys: Vec<types::Str>) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("EXISTS").arg(keys).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[pyo3(signature = (key, seconds, option = None))]
    fn expire<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        seconds: u64,
        option: Option<types::Str>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("EXPIRE")
            .arg(key)
            .arg(seconds)
            .arg(option)
            .to_owned();
        self.cr.fetch_bool(py, cmd)
    }

    #[pyo3(signature = (*keys))]
    fn delete<'a>(&self, py: Python<'a>, keys: Vec<types::Str>) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("DEL").arg(keys).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[pyo3(signature = (pattern, encoding = None))]
    fn keys<'a>(
        &self,
        py: Python<'a>,
        pattern: types::Arg,
        encoding: Option<String>,
    ) -> PyResult<&'a PyAny> {
        let encoding = types::Codec::from(encoding);
        let cmd = redis::cmd("KEYS").arg(pattern).to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (script, numkeys, *args, **kwargs))]
    fn eval<'a>(
        &self,
        py: Python<'a>,
        script: types::Str,
        numkeys: u8,
        args: Vec<types::Arg>,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("EVAL")
            .arg(script)
            .arg(numkeys)
            .arg(args)
            .to_owned();
        let encoding = types::Codec::from(kwargs);
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (key, value))]
    fn set<'a>(&self, py: Python<'a>, key: types::Str, value: types::Arg) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("SET").arg(key).arg(value).to_owned();
        self.cr.execute(py, cmd, types::Codec::default())
    }

    #[pyo3(signature = (key, **kwargs))]
    fn get<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = types::Codec::from(kwargs);
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
        let encoding = types::Codec::from(kwargs);
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
        let encoding = types::Codec::from(kwargs);
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
        let encoding = types::Codec::from(kwargs);
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

    #[pyo3(signature = (key, increment = None))]
    fn incr<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        increment: Option<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = match increment {
            None => redis::cmd("INCR").arg(key).to_owned(),
            Some(types::Arg::Int(i)) => redis::cmd("INCRBY").arg(key).arg(i).to_owned(),
            Some(arg) => redis::cmd("INCRBYFLOAT").arg(key).arg(arg).to_owned(),
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
        let encoding = types::Codec::from(kwargs);
        let cmd = redis::cmd("LPOP").arg(key).arg(count).to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (*keys, timeout, **kwargs))]
    fn blpop<'a>(
        &self,
        py: Python<'a>,
        keys: Vec<types::Str>,
        timeout: types::Arg,
        kwargs: Option<&PyDict>,
    ) -> PyResult<&'a PyAny> {
        let encoding = types::Codec::from(kwargs);
        let cmd = redis::cmd("BLPOP").arg(keys).arg(timeout).to_owned();
        self.cr.execute(py, cmd, encoding)
    }

    #[pyo3(signature = (key, count, element))]
    fn lrem<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        count: isize,
        element: types::Arg,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("LREM")
            .arg(key)
            .arg(count)
            .arg(element)
            .to_owned();
        self.cr.fetch_int(py, cmd)
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
        let encoding = types::Codec::from(kwargs);
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

    #[pyo3(signature = (key, *elements))]
    fn pfadd<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        elements: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("PFADD").arg(key).arg(elements).to_owned();
        self.cr.fetch_bool(py, cmd)
    }

    #[pyo3(signature = (*keys))]
    fn pfcount<'a>(&self, py: Python<'a>, keys: Vec<types::Arg>) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("PFCOUNT").arg(keys).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[pyo3(signature = (destkey, *sourcekeys))]
    fn pfmerge<'a>(
        &self,
        py: Python<'a>,
        destkey: types::Str,
        sourcekeys: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("PFMERGE")
            .arg(destkey)
            .arg(sourcekeys)
            .to_owned();
        self.cr.fetch_bool(py, cmd)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        stream, *args,
        id = None,
        items = None,
        mkstream = true,
        maxlen = None,
        minid = None,
        approx = true,
        limit = None,
    ))]
    fn xadd<'a>(
        &self,
        py: Python<'a>,
        stream: types::Str,
        mut args: Vec<types::ScalarOrMap>,
        id: Option<types::Str>,
        items: Option<HashMap<String, types::Arg>>,
        mkstream: bool,
        maxlen: Option<usize>,
        minid: Option<usize>,
        approx: bool,
        limit: Option<usize>,
    ) -> PyResult<&'a PyAny> {
        let mut cmd = redis::cmd("XADD").arg(stream).to_owned();

        if !mkstream {
            cmd.arg(b"NOMKSTREAM");
        }

        if let Some(threshold) = maxlen {
            cmd.arg(b"MAXLEN")
                .arg(if approx { b"~" } else { b"=" })
                .arg(threshold);
        }
        if let Some(threshold) = minid {
            cmd.arg(b"MINID")
                .arg(if approx { b"~" } else { b"=" })
                .arg(threshold);
        }
        if let Some(limit) = limit {
            cmd.arg("LIMIT").arg(limit);
        }

        if let Some(id) = id {
            cmd.arg(id);
        } else if args.is_empty() {
            cmd.arg(b"*");
        } else {
            match args.remove(0) {
                types::ScalarOrMap::Map(m) => {
                    cmd.arg(b"*");
                    cmd.arg(m);
                }
                types::ScalarOrMap::BMap(m) => {
                    cmd.arg(b"*");
                    cmd.arg(m);
                }
                types::ScalarOrMap::Scalar(arg) => {
                    if let Ok(id) = arg.to_normalized_stream_msg_id() {
                        cmd.arg(id);
                    } else {
                        cmd.arg(b"*");
                        cmd.arg(arg);
                    }
                }
            }
        };

        cmd.arg(args).arg(items);

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
        let encoding = types::Codec::from(encoding);
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
            types::ScalarOrMap::BMap(m) => {
                for (k, v) in m.into_iter() {
                    keys.push(String::from_utf8_lossy(&k).to_string());
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

    #[pyo3(signature = (key, *values, score = None, incr = None, encoding = None))]
    fn zadd<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        values: Vec<types::ScalarOrMap>,
        score: Option<f64>,
        incr: Option<f64>,
        encoding: Option<String>,
    ) -> PyResult<&'a PyAny> {
        let encoding = types::Codec::from(encoding);
        let mut cmd = redis::cmd("ZADD").arg(key).to_owned();
        if let Some(incr) = incr {
            cmd.arg(b"INCR").arg(incr);
        } else {
            cmd.arg(score);
        }
        let cmd = values.into_iter().fold(cmd, |c, som| som.write_val_key(c));
        self.cr.execute(py, cmd, encoding)
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        key,
        start = types::Arg::Int(0),
        stop = types::Arg::Int(-1),
        *args,
        withscores = false,
        encoding = None,
    ))]
    fn zrange<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        start: types::Arg,
        stop: types::Arg,
        args: Vec<types::Arg>,
        withscores: bool,
        encoding: Option<String>,
    ) -> PyResult<&'a PyAny> {
        let mut cmd = redis::cmd("ZRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .arg(args)
            .to_owned();
        if withscores {
            cmd.arg(b"WITHSCORES");
            self.cr.fetch_dict(py, cmd, types::Codec::Float)
        } else {
            let encoding = types::Codec::from(encoding);
            self.cr.execute(py, cmd, encoding)
        }
    }

    #[pyo3(signature = (key))]
    fn zcard<'a>(&self, py: Python<'a>, key: types::Str) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("ZCARD").arg(key).to_owned();
        self.cr.fetch_int(py, cmd)
    }

    #[pyo3(signature = (key, *members))]
    fn zrem<'a>(
        &self,
        py: Python<'a>,
        key: types::Str,
        members: Vec<types::Arg>,
    ) -> PyResult<&'a PyAny> {
        let cmd = redis::cmd("ZREM").arg(key).arg(members).to_owned();
        self.cr.fetch_int(py, cmd)
    }
}
