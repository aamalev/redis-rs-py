import redis_rs


async def test_return(async_client: redis_rs.AsyncClient):
    result = await async_client.eval("return ARGV[1]", 0, "a", encoding="utf-8")
    assert result == "a"
