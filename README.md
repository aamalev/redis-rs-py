# redis-rs

Python wrapper for redis-rs, bb8, bb8-redis, bb8-redis-cluster, deadpool-redis-cluster, redis_cluster_async

# Install

    pip install redis-rs

# Using

```python
import asyncio
import redis_rs


async def main():
    async with redis_rs.create_client(
        "redis://redis-node001",
        "redis://redis-node002",
        max_size=1,
        cluster=True,
    ) as x:
        print(await x.execute(b"HSET", "fooh", "a", b"asdfg"))
        print(await x.fetch_int("HSET", "fooh", "b", 11234567890))
        print(await x.fetch_int("HGET", "fooh", "b"))
        print(await x.fetch_str("HGET", "fooh", "a"))
        print(await x.fetch_dict("HGETALL", "fooh"), encoding="utf-8")
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


asyncio.run(main())
```

# Development

    cargo fmt
    cargo clippy
    maturin develop

or use hatch envs:

    hatch run fmt
    hatch run check
    hatch run build
