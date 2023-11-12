import pytest

import redis_rs


async def test_client_id(async_client: redis_rs.AsyncClient):
    assert async_client.client_id


@pytest.mark.redis(single=True)
async def test_password(async_client: redis_rs.AsyncClient, client_factory):
    user = "test"
    password = await async_client.execute("ACL", "GENPASS", encoding="utf-8")
    assert await async_client.execute("ACL", "SETUSER", user, "nopass")
    assert await async_client.execute("ACL", "SETUSER", user, "on", ">" + password, "+acl|whoami", "+cluster|slots")
    assert await async_client.execute("AUTH", user, password)
    assert user == await async_client.execute("ACL", "WHOAMI", encoding="utf-8")

    async with client_factory(username=user, password=password) as client:
        assert user == await client.execute("ACL", "WHOAMI", encoding="utf-8")
        status = client.status()
        assert status.get("username")
        assert status.get("auth")
