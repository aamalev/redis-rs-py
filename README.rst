redis-rs
========

.. image:: https://img.shields.io/badge/License-MIT-blue.svg
   :target: https://lbesson.mit-license.org/

.. image:: https://img.shields.io/pypi/v/redis-rs.svg
  :target: https://pypi.org/project/redis-rs

.. image:: https://img.shields.io/pypi/pyversions/redis-rs.svg
  :target: https://pypi.org/project/redis-rs
  :alt: Python versions

.. image:: https://readthedocs.org/projects/redis-rs/badge/?version=latest
  :target: https://github.com/aamalev/redis-rs-py#redis-rs
  :alt: Documentation Status

.. image:: https://github.com/aamalev/redis-rs-py/workflows/Tests/badge.svg
  :target: https://github.com/aamalev/redis-rs-py/actions?query=workflow%3ATests

.. image:: https://img.shields.io/pypi/dm/redis-rs.svg
  :target: https://pypi.org/project/redis-rs

|

.. image:: https://img.shields.io/badge/Rustc-1.73.0-blue?logo=rust
  :target: https://www.rust-lang.org/

.. image:: https://img.shields.io/badge/PyO3-maturin-blue.svg
  :target: https://github.com/PyO3/maturin

.. image:: https://img.shields.io/badge/PyO3-asyncio-blue.svg
  :target: https://github.com/awestlake87/pyo3-asyncio

.. image:: https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/charliermarsh/ruff/main/assets/badge/v0.json
  :target: https://github.com/charliermarsh/ruff
  :alt: Code style: ruff

.. image:: https://img.shields.io/badge/code%20style-black-000000.svg
  :target: https://github.com/psf/black
  :alt: Code style: black

.. image:: https://img.shields.io/badge/types-Mypy-blue.svg
  :target: https://github.com/python/mypy
  :alt: Code style: Mypy

.. image:: https://img.shields.io/badge/%F0%9F%A5%9A-Hatch-4051b5.svg
  :alt: Hatch project
  :target: https://github.com/pypa/hatch


Python wrapper for:
  | `redis-rs <https://github.com/redis-rs/redis-rs>`_,
  | redis_cluster_async,
  | `bb8 <https://github.com/djc/bb8>`_,
  | bb8-redis,
  | bb8-redis-cluster,
  | deadpool-redis-cluster,


Features
--------

* Async client for single and cluster
* Support typing
* Encoding values from str, int, float
* Decoding values to str, int, float, list, dict


Install
-------

.. code-block:: shell

    pip install redis-rs


Using
-----

.. code-block:: python

  import asyncio
  import redis_rs


  async def main():
      async with redis_rs.create_client(
          "redis://redis-node001",
          "redis://redis-node002",
          max_size=1,
          cluster=True,
      ) as x:
          info = await x.execute("INFO", "SERVER", encoding="info")
          print(info["redis_version"])

          print(await x.execute(b"HSET", "fooh", "a", b"asdfg"))
          print(await x.fetch_int("HSET", "fooh", "b", 11234567890))
          print(await x.fetch_int("HGET", "fooh", "b"))
          print(await x.fetch_str("HGET", "fooh", "a"))
          print(await x.fetch_dict("HGETALL", "fooh", encoding="utf-8"))
          print(await x.hgetall("fooh", encoding="utf-8"))
          print(await x.execute("CLUSTER", "NODES"))
          print(await x.fetch_bytes("GET", "foo"))
          print(await x.fetch_int("GET", "foo"))
          print(await x.execute("HGETALL", "fooh"))
          print(await x.execute("ZADD", "fooz", 1.5678, "b"))
          print(await x.fetch_scores("ZRANGE", "fooz", 0, -1, "WITHSCORES"))
          print(x.status())

          stream = "redis-rs"
          print("x.xadd", await x.xadd(stream, "*", {"a": "1234", "d": 4567}))
          print("x.xadd", await x.xadd(stream, items={"a": "1234", "d": 4567}))
          print("x.xadd", await x.xadd(stream, {"a": "1234", "d": 4567}))
          print("x.xadd", await x.xadd(stream, "*", "a", "1234", "d", 4567))
          print("x.xadd", await x.xadd(stream, "a", "1234", "d", 4567))
          print("xadd", await x.fetch_str("XADD", stream, "*", "a", "1234", "d", 4567))
          print("xread", await x.execute("XREAD", "STREAMS", stream, 0))
          print("xread", await x.fetch_dict("XREAD", "STREAMS", stream, 0, encoding="int"))
          print("x.xread", await x.xread({stream: 0}, encoding="int"))
          print("x.xread", await x.xread(stream, id=0, encoding="int"))
          print("x.xread", await x.xread(stream, stream))


  asyncio.run(main())


Development
-----------

.. code-block:: python

    cargo fmt
    cargo clippy
    maturin develop


or use hatch envs:

.. code-block:: python

    hatch run fmt
    hatch run check
    hatch run build
