import pytest

import redis_rs.client_async
from redis_rs.exceptions import RedisError


async def test_redis_error(async_client: redis_rs.client_async.AsyncClient):
    with pytest.raises(RedisError):
        await async_client.fetch_int("CLUSTER", "SLOTS")
