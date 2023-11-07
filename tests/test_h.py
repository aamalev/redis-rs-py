from uuid import uuid4

import redis_rs


async def test_hgetall(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    result = await async_client.hgetall(key)
    assert result == {}
    assert isinstance(result, dict)

    await async_client.hset(key, "f", 123.456)
    result = await async_client.hgetall(key)
    assert result == {"f": b"123.456"}
    assert isinstance(result, dict)

    result = await async_client.hgetall(key, encoding="float")
    assert result == {"f": 123.456}
    assert isinstance(result, dict)


async def test_hset_map(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    n = await async_client.hset(key, {"a": "2", "b": b"3"})
    assert n == 2
    result = await async_client.hgetall(key, encoding="float")
    assert result == {"a": 2.0, "b": 3.0}
    assert isinstance(result, dict)


async def test_hset_pairs(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    n = await async_client.hset(key, "x", "2", "y", b"3")
    assert n == 2
    result = await async_client.hgetall(key, encoding="int")
    assert result == {"x": 2, "y": 3}
    assert isinstance(result, dict)


async def test_hdel(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    n = await async_client.hset(key, "x", "2", "y", b"3")
    assert n == 2
    n = await async_client.hdel(key, "x")
    assert n == 1
    n = await async_client.hdel(key, "x")
    assert n == 0
    result = await async_client.hgetall(key, encoding="int")
    assert result == {"y": 3}
    assert isinstance(result, dict)


async def test_hexists(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    result = await async_client.hset(key, "x", "2", "y", b"3")
    assert result == 2
    result = await async_client.hexists(key, "x")
    assert result is True
    result = await async_client.hexists(key, "z")
    assert result is False


async def test_hmget(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    n = await async_client.hset(key, "x", "2", "y", b"3", "z", 4)
    assert n == 3
    result = await async_client.hmget(key, "x", "z", encoding="int")
    assert result == [2, 4]
