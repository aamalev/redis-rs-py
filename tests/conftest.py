import pytest

import redis_rs


@pytest.fixture
async def async_client():
    async with redis_rs.create_client() as c:
        yield c
