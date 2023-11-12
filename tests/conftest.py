import asyncio
import os

import pytest

import redis_rs


def to_addr(s: str) -> str:
    if s.isdigit():
        return f"redis://localhost:{s}"
    elif not s.startswith("redis://"):
        return f"redis://{s}"
    return s


async def get_redis_version(nodes: list) -> str:
    key = "redis_version"
    async with redis_rs.create_client(
        *nodes,
    ) as c:
        infos = await c.execute("INFO", "SERVER", encoding="info")
        if isinstance(infos, dict):
            result = infos.get(key, "")
        else:
            assert isinstance(infos, list)
            info = filter(lambda x: isinstance(x, dict), infos)
            result = min(map(lambda x: x[key], info))
        return result


NODES = [to_addr(node) for node in os.environ.get("REDIS_NODES", "").split(",") if node]
IS_CLUSTER = os.environ.get("REDIS_CLUSTER", "0") not in {"0"}
VERSION = ""


@pytest.fixture
def client_factory():
    return lambda **kwargs: redis_rs.create_client(
        *NODES,
        cluster=IS_CLUSTER,
        **kwargs,
    )


@pytest.fixture
async def async_client(client_factory):
    async with client_factory() as c:
        yield c


def pytest_runtest_setup(item):
    for marker in item.iter_markers():
        if marker.name == "redis":
            if marker.kwargs.get("cluster") and not IS_CLUSTER:
                pytest.skip("Single redis")
            elif marker.kwargs.get("single") and IS_CLUSTER:
                pytest.skip("Cluster redis")
            elif version := marker.kwargs.get("version"):
                global VERSION
                if not VERSION:
                    VERSION = asyncio.run(get_redis_version(NODES))
                if str(version) > VERSION:
                    pytest.skip(f"redis_version:{VERSION} < {version}")
