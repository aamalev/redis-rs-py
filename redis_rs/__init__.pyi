__all__ = [
    "create_client",
    "Client",
    "AsyncClient",
    "exceptions",
]

from typing import Dict, List, Optional

from redis_rs.client_async import AsyncClient

class exceptions:
    class PoolError(Exception): ...
    class RedisError(Exception): ...

class Client:
    def status(self) -> Dict: ...
    async def __aenter__(self) -> AsyncClient: ...
    async def __aexit__(self, *args, **kwargs): ...

def create_client(
    *args: str,
    max_size: Optional[int] = None,
    cluster: Optional[bool] = None,
    client_id: Optional[str] = None,
    features: Optional[List[str]] = None,
) -> Client: ...
