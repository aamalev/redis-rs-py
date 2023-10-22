from uuid import uuid4

import pytest

import redis_rs


async def test_hgetall(async_client: redis_rs.AsyncClient):
    key = uuid4().hex

    await async_client.hset(key, "a", 1)
    await async_client.hset(key, "b", 2)
    result = await async_client.fetch_dict("HGETALL", key)
    assert result
    assert result == {"a": b"1", "b": b"2"}


async def test_zrange(async_client: redis_rs.AsyncClient):
    key = uuid4().hex

    await async_client.execute("ZADD", key, 1.5678, "b")
    await async_client.execute("ZADD", key, 2.6, "a")
    result = await async_client.fetch_dict("ZRANGE", key, 0, -1, "WITHSCORES", encoding="float")
    assert result
    assert result == {"a": 2.6, "b": 1.5678}


async def test_xread(async_client: redis_rs.AsyncClient):
    stream = uuid4().hex

    result = await async_client.xread({stream: 0})
    assert isinstance(result, dict)
    assert result == {}

    ident = await async_client.xadd(stream, {"a": "bcd"})
    assert isinstance(ident, str)

    result = await async_client.xread(stream)
    assert isinstance(result, dict)
    assert result == {stream: {ident: {"a": b"bcd"}}}


async def test_xinfo_stream(async_client: redis_rs.AsyncClient):
    stream = f"stream-{uuid4()}"
    group = f"group-{uuid4()}"

    await async_client.execute("XGROUP", "CREATE", stream, group, 0, "MKSTREAM")
    xinfo = await async_client.fetch_dict("XINFO", "STREAM", stream)
    assert xinfo
    assert len(xinfo) > 6


async def test_xinfo_groups(async_client: redis_rs.AsyncClient):
    stream = f"stream-{uuid4()}"
    group = f"group-{uuid4()}"

    await async_client.execute("XGROUP", "CREATE", stream, group, 0, "MKSTREAM")
    xinfo = await async_client.fetch_dict("XINFO", "GROUPS", stream)
    assert xinfo
    assert len(xinfo) == 1
    assert len(xinfo[0]) > 3

    group = f"group-{uuid4()}"
    await async_client.execute("XGROUP", "CREATE", stream, group, 0, "MKSTREAM")
    xinfo = await async_client.fetch_dict("XINFO", "GROUPS", stream)
    assert xinfo
    assert len(xinfo) == 2
    assert len(xinfo[0]) > 3
    assert len(xinfo[1]) > 3


async def test_xinfo_consumers(async_client: redis_rs.AsyncClient):
    stream = f"stream-{uuid4()}"
    group = f"group-{uuid4()}"

    await async_client.execute("XGROUP", "CREATE", stream, group, 0, "MKSTREAM")
    await async_client.execute("XGROUP", "CREATECONSUMER", stream, group, async_client.client_id)
    xinfo = await async_client.fetch_dict("XINFO", "CONSUMERS", stream, group)
    assert xinfo
    assert len(xinfo) == 1
    assert len(xinfo[0]) > 2


@pytest.mark.redis(version=7, cluster=True)
async def test_cluster_shards(async_client: redis_rs.AsyncClient):
    result = await async_client.fetch_dict("CLUSTER", "SHARDS")
    assert result
    assert len(result) > 1
    assert len(result[0]) > 1


@pytest.mark.redis(cluster=True)
async def test_info(async_client: redis_rs.AsyncClient):
    result = await async_client.fetch_dict("INFO", "SERVER", encoding="info")
    assert result
    assert isinstance(result, dict)
    for k, v in result.items():
        assert isinstance(k, str)
        assert "redis_version" in v
