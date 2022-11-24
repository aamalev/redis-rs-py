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
    print(await x.execute("hset", b"fooh", b"a", b"asdfg"))
    print(await x.fetch_int("hset", b"fooh", b"b", b"1234567890"))
    print(await x.fetch_int("hget", b"fooh", b"b"))
    print(await x.fetch_str("hget", b"fooh", b"a"))
    print(await x.fetch_dict("hgetall", b"fooh"))
    print(await x.execute("cluster", b"nodes"))
    print(await x.fetch_bytes("get", b"foo"))
    print(await x.fetch_int("get", b"foo"))
    print(await x.execute("hgetall", b"fooh"))
    print(await x.execute("zadd", b"fooz", b"1", b"b"))
    print(await x.fetch_scores("zrange", b"fooz", b"0", b"-1", b"WITHSCORES"))


asyncio.run(main())
```
