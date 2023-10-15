from uuid import uuid4

import redis_rs


async def test_xadd(async_client: redis_rs.AsyncClient):
    stream = str(uuid4())

    ident = await async_client.xadd(stream, {"a": "bcd"})
    assert isinstance(ident, str)


async def test_xread(async_client: redis_rs.AsyncClient):
    stream = str(uuid4())

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

    result = await async_client.xread(str(uuid4()), stream, str(uuid4()), encoding="utf-8")
    assert result == {stream: {ident: {"a": "bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread(str(uuid4()), stream, str(uuid4()), id="0", encoding="utf-8")
    assert result == {stream: {ident: {"a": "bcd"}}}
    assert isinstance(result, dict)

    result = await async_client.xread(str(uuid4()), stream, str(uuid4()), id="$", encoding="utf-8")
    assert result == {}
    assert isinstance(result, dict)
