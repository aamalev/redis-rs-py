from uuid import uuid4

import redis_rs


async def test_set(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    await async_client.set(key, 1)
    result = await async_client.get(key)
    assert result == b"1"


async def test_get_int(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    await async_client.set(key, 3)
    result = await async_client.get(key, encoding="int")
    assert result == 3


async def test_set_ex(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    ttl = await async_client.fetch_int("TTL", key)
    assert ttl < 0
    await async_client.set(key, 2, ex=10)
    result = await async_client.get(key)
    assert result == b"2"
    ttl = await async_client.fetch_int("TTL", key)
    assert ttl > 0


async def test_set_px(async_client: redis_rs.AsyncClient):
    key = str(uuid4())
    ttl = await async_client.fetch_int("TTL", key)
    assert ttl < 0
    await async_client.set(key, 2, px=10000)
    result = await async_client.get(key)
    assert result == b"2"
    ttl = await async_client.fetch_int("TTL", key)
    assert ttl > 0
