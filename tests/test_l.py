from uuid import uuid4

import pytest

import redis_rs


async def test_lpush(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.lpush(key, 2)
    assert n == 1

    n = await async_client.lpush(key, 3)
    assert n == 2

    result = await async_client.lrange(key, encoding="int")
    assert result == [3, 2]


async def test_rpush(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.rpush(key, 2)
    assert n == 1

    n = await async_client.rpush(key, 3)
    assert n == 2

    result = await async_client.lrange(key, encoding="int")
    assert result == [2, 3]


async def test_lrange(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.lpush(key, 2)
    assert n == 1

    n = await async_client.lpush(key, 1)
    assert n == 2

    n = await async_client.rpush(key, 3)
    assert n == 3

    result = await async_client.lrange(key, 1, 1, encoding="int")
    assert result == [2]

    result = await async_client.lrange(key, 0, -1, encoding="int")
    assert result == [1, 2, 3]

    result = await async_client.lrange(key, encoding="int")
    assert result == [1, 2, 3]


@pytest.mark.redis(version=6.2)
async def test_lpop(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.lpush(key, 2)
    assert n == 1

    n = await async_client.lpush(key, 3)
    assert n == 2

    result = await async_client.lpop(key, encoding="int")
    assert result == 3

    n = await async_client.lpush(key, 4)
    assert n == 2

    result = await async_client.lpop(key, count=3, encoding="int")
    assert result == [4, 2]

    result = await async_client.lpush(key, 5)
    assert result == 1


@pytest.mark.redis(version=6)
async def test_blpop(async_client: redis_rs.AsyncClient):
    key1 = str(uuid4()) + "{a}"
    key2 = str(uuid4()) + "{a}"

    n = await async_client.lpush(key1, 1)
    assert n == 1

    n = await async_client.lpush(key2, 2)
    assert n == 1

    result = await async_client.blpop(key1, key2, timeout=0, encoding="int")
    assert result == [None, 1]

    result = await async_client.blpop(key1, key2, timeout=0, encoding="int")
    assert result == [None, 2]


async def test_llen(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    result = await async_client.lpush(key, 2)
    assert result == 1

    result = await async_client.rpush(key, 3)
    assert result == 2

    result = await async_client.llen(key)
    assert result == 2


async def test_lrem(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    n = await async_client.lpush(key, 2)
    assert n == 1

    n = await async_client.lpush(key, 3)
    assert n == 2

    result = await async_client.lrem(key, 0, 2)
    assert result == 1
