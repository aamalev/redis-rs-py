import redis_rs


async def test_client_id(async_client: redis_rs.AsyncClient):
    assert async_client.client_id
