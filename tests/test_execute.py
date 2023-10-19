import redis_rs


async def test_execute(async_client: redis_rs.AsyncClient):
    result = await async_client.execute("INFO")
    assert isinstance(result, bytes)
    assert b"redis_version" in result

    result = await async_client.execute("INFO", encoding="utf-8")
    assert isinstance(result, str)
    assert "redis_version" in result

    result = await async_client.execute("INFO", encoding="info")
    assert isinstance(result, dict)
    assert result["redis_version"]


async def test_fetch_str(async_client: redis_rs.AsyncClient):
    result = await async_client.fetch_str("INFO")
    assert isinstance(result, str)
    assert "redis_version" in result
