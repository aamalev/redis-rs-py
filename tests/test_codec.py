from uuid import uuid4

import pytest

import redis_rs


@pytest.mark.redis(single=True)
async def test_parse_info(async_client: redis_rs.AsyncClient):
    result = await async_client.execute("INFO", encoding="info")
    assert isinstance(result, dict)
    assert "redis_version" in result


async def test_parse_json(async_client: redis_rs.AsyncClient):
    key = uuid4().hex
    await async_client.set(key, '{"a": 3, "i": ["b"]}')
    result = await async_client.get(key, encoding="json")
    assert isinstance(result, dict)
    assert result == {"a": 3, "i": ["b"]}
