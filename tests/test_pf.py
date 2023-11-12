from uuid import uuid4

import redis_rs


async def test_pfadd(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    result = await async_client.pfadd(key, 2)
    assert result is True


async def test_pfcount(async_client: redis_rs.AsyncClient):
    key = str(uuid4())

    b = await async_client.pfadd(key, 2)
    assert b is True

    result = await async_client.pfcount(key)
    assert result == 1


async def test_pfmerge(async_client: redis_rs.AsyncClient):
    key1 = str(uuid4())
    key2 = str(uuid4())

    b = await async_client.pfadd(key1, 2)
    assert b is True

    b = await async_client.pfmerge(key2, key1)
    assert b is True

    result = await async_client.pfcount(key2)
    assert result == 1
