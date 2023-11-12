import time
from uuid import uuid4

import pytest

import redis_rs


@pytest.mark.redis(version=6.2)
async def test_xadd(async_client: redis_rs.AsyncClient):
    stream = str(uuid4())

    ident = await async_client.xadd(stream, {"a": "bcd"})
    assert isinstance(ident, str)

    ident = await async_client.xadd(stream, {"a": "bcd"}, maxlen=2, approx=True, limit=5)
    assert isinstance(ident, str)

    ident = await async_client.xadd(stream, {"a": "bcd"}, maxlen=2, approx=True)
    assert isinstance(ident, str)

    ident = await async_client.xadd(stream, {"a": "bcd"}, minid=2, approx=True, limit=5)
    assert isinstance(ident, str)

    ident = await async_client.xadd(stream, {"a": "bcd"}, minid=2, approx=True)
    assert isinstance(ident, str)


@pytest.mark.redis(version=6.2)
async def test_xadd_nomkstream(async_client: redis_rs.AsyncClient):
    stream = str(uuid4())

    ident = await async_client.xadd(stream, {"a": "bcd"}, mkstream=False)
    assert ident is None


@pytest.mark.redis(version=6.2)
async def test_xadd_flat(async_client: redis_rs.AsyncClient):
    stream = str(uuid4())

    ident = await async_client.xadd(stream, "a", "b", "c", "d")
    assert isinstance(ident, str)


@pytest.mark.redis(version=7)
@pytest.mark.parametrize("id", ["2-*", b"3-*"])
async def test_xadd_flat_id_star(id, async_client: redis_rs.AsyncClient):
    stream = str(uuid4())

    ident = await async_client.xadd(stream, id, "a", "b", minid=1)
    assert isinstance(ident, str), ident


@pytest.mark.redis(version=6.2)
@pytest.mark.parametrize(
    "id",
    [
        1,
        1.0,
        1.01,
        "1-0",
        "1-1",
        1.00001,
        time.time(),
        int(time.time()),
        time.time() * 1000,
        str(int(time.time())),
        str(time.time()).replace(".", "-"),
        str(int(time.time())).encode("utf-8"),
    ],
)
async def test_xadd_flat_id(id, async_client: redis_rs.AsyncClient):
    stream = str(uuid4())

    ident = await async_client.xadd(stream, id, "a", "b", minid=1)
    assert isinstance(ident, str), ident


async def test_xread(async_client: redis_rs.AsyncClient):
    stream = str(uuid4()) + "{a}"
    stream1 = str(uuid4()) + "{a}"
    stream2 = str(uuid4()) + "{a}"

    result = await async_client.xread({stream: 0})
    assert result == {}

    ident = await async_client.xadd(stream, {"a": "bcd"})
    assert isinstance(ident, str)

    result = await async_client.xread({stream: 0})
    assert result == {stream: {ident: {"a": b"bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread({stream: 0}, encoding="utf-8")
    assert result == {stream: {ident: {"a": "bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread(stream, encoding="utf-8")
    assert result == {stream: {ident: {"a": "bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread(stream, id=0, encoding="utf-8")
    assert result == {stream: {ident: {"a": "bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread(stream, id="$", encoding="utf-8")
    assert result == {}
    assert isinstance(result, dict)

    result = await async_client.xread(stream1, stream, stream2, encoding="utf-8")
    assert result == {stream: {ident: {"a": "bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread(stream1, stream, stream2, id="0", encoding="utf-8")
    assert result == {stream: {ident: {"a": "bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread(stream1, stream, stream2, id="$", encoding="utf-8")
    assert result == {}
    assert isinstance(result, dict)


async def test_xreadgroup(async_client: redis_rs.AsyncClient):
    stream = f"stream-{uuid4()}"
    group = f"group-{uuid4()}"

    result = await async_client.xread(stream)
    assert result == {}

    ident = await async_client.xadd(stream, {"a": "1"})
    assert isinstance(ident, str)

    result = await async_client.xread(stream)
    assert result
    assert result == {stream: {ident: {"a": b"1"}}}
    assert isinstance(result, dict)

    await async_client.execute("XGROUP", "CREATE", stream, group, "$", "MKSTREAM")

    ident = await async_client.xadd(stream, {"a": "2"})
    assert isinstance(ident, str)

    result = await async_client.xread(stream, group=group)
    assert result == {stream: {ident: {"a": b"2"}}}
    assert await async_client.xack(stream, group, ident)
    assert isinstance(result, dict)

    ident = await async_client.xadd(stream, {"a": "3"})
    assert isinstance(ident, str)

    result = await async_client.xread(stream, group=group, count=2, block=5000)
    assert result == {stream: {ident: {"a": b"3"}}}
    assert isinstance(result, dict)
