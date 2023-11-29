from uuid import uuid4

import redis_rs


async def test_zadd(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.zadd(key, 2, "a")
    assert n == 1

    n = await async_client.zadd(key, "b", score=3)
    assert n == 1

    n = await async_client.zadd(key, {"c": 4})
    assert n == 1

    n = await async_client.zadd(key, {b"d": 4})
    assert n == 1

    result = await async_client.execute("ZRANGE", key, 0, -1, encoding="utf8")
    assert result == ["a", "b", "c", "d"]


async def test_zrange(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.zadd(key, "a", score=1)
    assert n == 1

    n = await async_client.zadd(key, "b", score=2)
    assert n == 1

    result = await async_client.zrange(key)
    assert result == ["a", "b"]

    result_d = await async_client.zrange(key, withscores=True)
    assert result_d == {"a": 1, "b": 2}


async def test_zcard(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.zadd(key, "a", score=1)
    assert n == 1

    n = await async_client.zadd(key, "b", score=2)
    assert n == 1

    result = await async_client.zcard(key)
    assert result == 2


async def test_zrem(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.zadd(key, "a", score=1)
    assert n == 1

    n = await async_client.zadd(key, "b", score=2)
    assert n == 1

    result = await async_client.zcard(key)
    assert result == 2

    result = await async_client.zrem(key, "a")
    assert result == 1

    result = await async_client.zcard(key)
    assert result == 1
