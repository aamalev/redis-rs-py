use crate::{client_result_async::AsyncClientResult, command::Params, types};
use pyo3::prelude::*;
use redis::streams::StreamReadOptions;
use std::{collections::HashMap, num::NonZeroUsize};

#[pyclass]
#[derive(Clone)]
pub struct Client {
    pub(crate) cr: AsyncClientResult,
    #[pyo3(get)]
    pub client_id: String,
}

#[pymethods]
impl Client {
    async fn __aenter__(&self) -> PyResult<Self> {
        self.cr.init(self).await
    }

    async fn __aexit__(
        &self,
        _exc_type: PyObject,
        _exc_value: PyObject,
        _traceback: PyObject,
    ) -> PyResult<()> {
        self.cr.close().await
    }

    fn status(&self, py: Python) -> PyResult<HashMap<String, PyObject>> {
        let status = self.cr.status()?;
        let mut result = HashMap::new();
        for (k, v) in status.into_iter() {
            let value = types::to_object(py, v, types::Codec::String)?;
            result.insert(k, value);
        }
        Ok(result)
    }

    #[pyo3(signature = (cmd, *args, encoding = None))]
    async fn execute(
        &self,
        cmd: types::Str,
        args: Vec<types::Arg>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let mut params = Params::from(&cmd);
        params.codec = encoding.into();
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (cmd, *args))]
    async fn fetch_str(&self, cmd: types::Str, args: Vec<types::Arg>) -> PyResult<Option<String>> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let params = Params::from(&cmd);
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (cmd, *args))]
    async fn fetch_bytes(&self, cmd: types::Str, args: Vec<types::Arg>) -> PyResult<Vec<u8>> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let params = Params::from(&cmd);
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (cmd, *args))]
    async fn fetch_list(&self, cmd: types::Str, args: Vec<types::Arg>) -> PyResult<Vec<String>> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let params = Params::from(&cmd);
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (cmd, *args, encoding = None))]
    async fn fetch_dict(
        &self,
        cmd: types::Str,
        args: Vec<types::Arg>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let mut params = Params::from(&cmd);
        params.codec = encoding.into();
        self.cr.fetch_dict(cmd, params).await
    }

    #[pyo3(signature = (cmd, *args))]
    async fn fetch_scores(
        &self,
        cmd: types::Str,
        args: Vec<types::Arg>,
    ) -> PyResult<HashMap<String, f64>> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let mut params = Params::from(&cmd);
        params.codec = types::Codec::Float;
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (cmd, *args))]
    async fn fetch_int(&self, cmd: types::Str, args: Vec<types::Arg>) -> PyResult<i64> {
        let cmd = String::from(cmd).to_ascii_uppercase();
        let cmd = redis::cmd(cmd.as_str()).arg(args).to_owned();
        let params = Params::from(&cmd);
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (*keys))]
    async fn exists(&self, keys: Vec<types::Str>) -> PyResult<bool> {
        let params = Params::from(&keys);
        let cmd = redis::cmd("EXISTS").arg(keys).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, seconds, option = None))]
    async fn expire(
        &self,
        key: types::Str,
        seconds: u64,
        option: Option<types::Str>,
    ) -> PyResult<bool> {
        let params = Params::from(&key);
        let cmd = redis::cmd("EXPIRE")
            .arg(key)
            .arg(seconds)
            .arg(option)
            .to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (*keys))]
    async fn delete(&self, keys: Vec<types::Str>) -> PyResult<i64> {
        let params = Params::from(&keys);
        let cmd = redis::cmd("DEL").arg(keys).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (pattern))]
    async fn keys(&self, pattern: types::Str) -> PyResult<Vec<String>> {
        let params = Params::default();
        let cmd = redis::cmd("KEYS").arg(pattern).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (script, numkeys, *args, encoding = None))]
    async fn eval(
        &self,
        script: types::Str,
        numkeys: u8,
        args: Vec<types::Arg>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let cmd = redis::cmd("EVAL")
            .arg(script)
            .arg(numkeys)
            .arg(args)
            .to_owned();
        let encoding = types::Codec::from(encoding);
        let mut params = Params::from(encoding);
        params.block = true;
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (
        key,
        value,
        ex = None,
        px = None,
        *,
        encoding = None,
    ))]
    async fn set(
        &self,
        key: types::Str,
        value: types::Arg,
        ex: Option<usize>,
        px: Option<usize>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let mut cmd = redis::cmd("SET").arg(key).arg(value).to_owned();
        if let Some(ex) = ex {
            cmd.arg(b"EX");
            cmd.arg(ex);
        }
        if let Some(px) = px {
            cmd.arg(b"PX");
            cmd.arg(px);
        }
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (key, *, encoding = None))]
    async fn get(&self, key: types::Str, encoding: Option<String>) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let cmd = redis::cmd("GET").arg(key).to_owned();
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (key, *pairs, mapping = None))]
    async fn hset(
        &self,
        key: types::Str,
        pairs: Vec<types::ScalarOrMap>,
        mapping: Option<types::ScalarOrMap>,
    ) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("HSET")
            .arg(key)
            .arg(pairs)
            .arg(mapping)
            .to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, field, *, encoding = None))]
    async fn hget(
        &self,
        key: types::Str,
        field: types::Str,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let cmd = redis::cmd("HGET").arg(key).arg(field).to_owned();
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (key, *fields, encoding = None))]
    async fn hmget(
        &self,
        key: types::Str,
        fields: Vec<types::Str>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let cmd = redis::cmd("HMGET").arg(key).arg(fields).to_owned();
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (key, *, encoding = None))]
    async fn hgetall(&self, key: types::Str, encoding: Option<String>) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let cmd = redis::cmd("HGETALL").arg(key).to_owned();
        self.cr.fetch_dict(cmd, params).await
    }

    #[pyo3(signature = (key, field))]
    async fn hexists(&self, key: types::Str, field: types::Arg) -> PyResult<bool> {
        let params = Params::from(&key);
        let cmd = redis::cmd("HEXISTS").arg(key).arg(field).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, *fields))]
    async fn hdel(&self, key: types::Str, fields: Vec<types::Arg>) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("HDEL").arg(key).arg(fields).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, increment = None))]
    async fn incr(&self, key: types::Str, increment: Option<types::Arg>) -> PyResult<f64> {
        let params = Params::from(&key);
        let cmd = match increment {
            None => redis::cmd("INCR").arg(key).to_owned(),
            Some(types::Arg::Int(i)) => redis::cmd("INCRBY").arg(key).arg(i).to_owned(),
            Some(arg) => redis::cmd("INCRBYFLOAT").arg(key).arg(arg).to_owned(),
        };
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, value))]
    async fn lpush(&self, key: types::Str, value: types::Arg) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("LPUSH").arg(key).arg(value).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, value))]
    async fn rpush(&self, key: types::Str, value: types::Arg) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("RPUSH").arg(key).arg(value).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, count = None, *, encoding = None))]
    async fn lpop(
        &self,
        key: types::Str,
        count: Option<NonZeroUsize>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let cmd = redis::cmd("LPOP").arg(key).arg(count).to_owned();
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (*keys, timeout, encoding = None))]
    async fn blpop(
        &self,
        keys: Vec<types::Str>,
        timeout: types::Arg,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&keys);
        params.codec = encoding.into();
        params.block = true;
        let cmd = redis::cmd("BLPOP").arg(keys).arg(timeout).to_owned();
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (key, count, element))]
    async fn lrem(&self, key: types::Str, count: isize, element: types::Arg) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("LREM")
            .arg(key)
            .arg(count)
            .arg(element)
            .to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, start = 0, stop = -1, *, encoding = None))]
    async fn lrange(
        &self,
        key: types::Str,
        start: isize,
        stop: isize,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let cmd = redis::cmd("LRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .to_owned();
        self.cr.execute(cmd, params).await
    }

    #[pyo3(signature = (key))]
    async fn llen(&self, key: types::Str) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("LLEN").arg(key).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, *elements))]
    async fn pfadd(&self, key: types::Str, elements: Vec<types::Arg>) -> PyResult<bool> {
        let params = Params::from(&key);
        let cmd = redis::cmd("PFADD").arg(key).arg(elements).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (*keys))]
    async fn pfcount(&self, keys: Vec<types::Str>) -> PyResult<i64> {
        let params = Params::from(&keys);
        let cmd = redis::cmd("PFCOUNT").arg(keys).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (destkey, *sourcekeys))]
    async fn pfmerge(&self, destkey: types::Str, sourcekeys: Vec<types::Str>) -> PyResult<bool> {
        let mut params = Params::from(&destkey);
        params.keys.extend(sourcekeys.iter().map(|k| k.into()));
        let cmd = redis::cmd("PFMERGE")
            .arg(destkey)
            .arg(sourcekeys)
            .to_owned();
        self.cr.fetch(cmd, params).await
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
    async fn xadd(
        &self,
        stream: types::Str,
        mut args: Vec<types::ScalarOrMap>,
        id: Option<types::Str>,
        items: Option<HashMap<String, types::Arg>>,
        mkstream: bool,
        maxlen: Option<usize>,
        minid: Option<usize>,
        approx: bool,
        limit: Option<usize>,
    ) -> PyResult<Option<String>> {
        let params = Params::from(&stream);

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

        self.cr.fetch(cmd, params).await
    }

    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (streams, *args, id=None, block=None, count=None, noack=None, group=None, encoding=None))]
    async fn xread(
        &self,
        streams: types::ScalarOrMap,
        args: Vec<types::Str>,
        id: Option<types::Arg>,
        block: Option<usize>,
        count: Option<usize>,
        noack: Option<bool>,
        group: Option<types::Str>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let encoding = types::Codec::from(encoding);
        let mut params = Params::from(encoding);
        let mut id = id.unwrap_or(types::Arg::Int(0));
        let mut options = StreamReadOptions::default();
        if let Some(ms) = block {
            options = options.block(ms);
            params.block = true;
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
        params.keys = keys.iter().map(|k| k.as_bytes().to_vec()).collect();
        cmd.arg(options).arg("STREAMS").arg(keys).arg(ids);
        self.cr.fetch_dict(cmd, params).await
    }

    #[pyo3(signature = (key, group, *id))]
    async fn xack(&self, key: types::Str, group: types::Str, id: Vec<types::Str>) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("XACK").arg(key).arg(group).arg(id).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, *values, score = None, incr = None, encoding = None))]
    async fn zadd(
        &self,
        key: types::Str,
        values: Vec<types::ScalarOrMap>,
        score: Option<f64>,
        incr: Option<f64>,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = encoding.into();
        let mut cmd = redis::cmd("ZADD").arg(key).to_owned();
        if let Some(incr) = incr {
            cmd.arg(b"INCR").arg(incr);
        } else {
            cmd.arg(score);
        }
        let cmd = values.into_iter().fold(cmd, |c, som| som.write_val_key(c));
        self.cr.execute(cmd, params).await
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
    async fn zrange(
        &self,
        key: types::Str,
        start: types::Arg,
        stop: types::Arg,
        args: Vec<types::Arg>,
        withscores: bool,
        encoding: Option<String>,
    ) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        let mut cmd = redis::cmd("ZRANGE")
            .arg(key)
            .arg(start)
            .arg(stop)
            .arg(args)
            .to_owned();
        if withscores {
            cmd.arg(b"WITHSCORES");
            params.codec = types::Codec::Float;
            self.cr.fetch_dict(cmd, params).await
        } else {
            let encoding = types::Codec::from(encoding);
            params.codec = encoding;
            self.cr.execute(cmd, params).await
        }
    }

    #[pyo3(signature = (key))]
    async fn zcard(&self, key: types::Str) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("ZCARD").arg(key).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, *members))]
    async fn zrem(&self, key: types::Str, members: Vec<types::Arg>) -> PyResult<i64> {
        let params = Params::from(&key);
        let cmd = redis::cmd("ZREM").arg(key).arg(members).to_owned();
        self.cr.fetch(cmd, params).await
    }

    #[pyo3(signature = (key, count = None))]
    async fn zpopmin(&self, key: types::Str, count: Option<i64>) -> PyResult<PyObject> {
        let mut params = Params::from(&key);
        params.codec = types::Codec::Float;
        let cmd = redis::cmd("ZPOPMIN").arg(key).arg(count).to_owned();
        self.cr.fetch_dict(cmd, params).await
    }

    #[pyo3(signature = (*keys, timeout = 0))]
    async fn bzpopmin(&self, keys: Vec<types::Str>, timeout: i64) -> PyResult<PyObject> {
        let mut params = Params::from(&keys);
        params.codec = types::Codec::Float;
        params.block = true;
        let cmd = redis::cmd("BZPOPMIN").arg(keys).arg(timeout).to_owned();
        self.cr.fetch_dict(cmd, params).await
    }
}

#[cfg(test)]
mod tests {
    use crate::{config::Config, pool_manager::PoolManager};

    #[test]
    fn slots_parse() {
        let _pm = PoolManager::new(Config::default()).unwrap();
        // let client = Client::from(pm);
        // let lenght = Python::with_gil(move |py| {
        //     let result = client.status(py).;
        //     // let result = result.downcast::<PyDict>(py).unwrap();
        //     // result.len()
        // });
        // assert_eq!(lenght, ());
    }
}
