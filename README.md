# redis-rs

Python wrapper for redis-rs and redis_cluster_async

# Install

    pip install redis-rs

# Use

```python
import asyncio
import redis_rs

x = redis_rs.create_client([
    "redis://redis-node001",
    "redis://redis-node002",
], 1)
async def main():
    print(await x.execute(b"hset", "fooh", "a", b"asdfg"))
    print(await x.fetch_int("hset", "fooh", "b", 11234567890))
    print(await x.fetch_int("hget", "fooh", "b"))
    print(await x.fetch_str("hget", "fooh", "a"))
    print(await x.fetch_dict("hgetall", "fooh"))
    print(await x.execute("cluster", "nodes"))
    print(await x.fetch_bytes("get", "foo"))
    print(await x.fetch_int("get", "foo"))
    print(await x.execute("hgetall", "fooh"))
    print(await x.execute("zadd", "fooz", 1.5678, "b"))
    print(await x.fetch_scores("zrange", "fooz", 0, -1, "WITHSCORES"))


asyncio.run(main())
```
