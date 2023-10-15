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
