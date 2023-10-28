import pytest

from redis_rs import AsyncClient
from redis_rs.exceptions import RedisError


async def test_redis_error(async_client: AsyncClient):
    with pytest.raises(RedisError):
        await async_client.fetch_int("CLUSTER", "SLOTS")
