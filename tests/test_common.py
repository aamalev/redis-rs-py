from uuid import uuid4

import redis_rs


async def test_exists(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    await async_client.set(key, 1)

    result = await async_client.exists(key)
    assert result == 1


async def test_delete(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    await async_client.set(key, 1)

    result = await async_client.delete(key)
    assert result == 1


async def test_keys(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    await async_client.set(key, 1)

    result = await async_client.keys(key, encoding="utf-8")
    assert result == [key]


async def test_expire(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    await async_client.set(key, 1)

    result = await async_client.expire(key, 2)
    assert result is True
