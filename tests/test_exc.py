import pytest

import redis_rs


async def test_redis_error(async_client: redis_rs.AsyncClient):
    with pytest.raises(redis_rs.exceptions.RedisError):
        await async_client.fetch_int("CLUSTER", "SLOTS")
