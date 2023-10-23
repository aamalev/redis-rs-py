from uuid import uuid4

import pytest

import redis_rs


async def test_execute(async_client: redis_rs.AsyncClient):
    key = uuid4().hex
    result = await async_client.execute("SET", key, 1)
    result = await async_client.execute("GET", key)
    assert isinstance(result, bytes)

    result = await async_client.execute("GET", key, encoding="utf-8")
    assert isinstance(result, str)


@pytest.mark.redis(single=True)
async def test_fetch_str_info(async_client: redis_rs.AsyncClient):
    result = await async_client.fetch_str("INFO")
    assert isinstance(result, str)
    assert "redis_version" in result
