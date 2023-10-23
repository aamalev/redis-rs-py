from typing import Dict, List, Literal, Optional, Union, overload

class exceptions:
    class PoolError(Exception): ...
    class RedisError(Exception): ...

Encoding = Union[
    Literal["utf-8"],
    Literal["utf8"],
    Literal["int"],
    Literal["float"],
    Literal["info"],
]

Arg = Union[str, bytes, int, float]
Result = Union[bytes, str, int, float, dict, list]

class Client:
    def status(self) -> Dict: ...
    async def __aenter__(self) -> "AsyncClient": ...
    async def __aexit__(self, *args, **kwargs): ...

class AsyncClient:
    client_id: str
    def status(self) -> Dict: ...
    async def execute(self, *args: Arg, encoding: Optional[Encoding] = None) -> Result: ...
    async def fetch_bytes(self, *args: Arg) -> bytes: ...
    async def fetch_str(self, *args: Arg) -> str: ...
    async def fetch_int(self, *args: Arg) -> int: ...
    async def fetch_float(self, *args: Arg) -> float: ...
    async def fetch_dict(self, *args: Arg, encoding: Optional[Encoding] = None) -> dict: ...
    async def fetch_scores(self, *args: Arg) -> Dict[str, float]: ...
    async def set(self, key: str, value: Arg): ...
    async def get(self, key: str, *, encoding: Optional[Encoding] = None) -> Result: ...
    async def hset(self, key: str, field: str, value: Arg): ...
    async def hget(self, key: str, field: str, *, encoding: Optional[Encoding] = None) -> Result: ...
    async def hgetall(self, key: str, *, encoding: Optional[Encoding] = None) -> Dict: ...
    async def incr(self, key: str, delta: Union[None, int, float] = None) -> float: ...
    async def lpush(self, key: str, value: Arg): ...
    async def lpop(self, key: str, *, encoding: Optional[Encoding] = None) -> Result: ...
    async def lrange(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
        *,
        encoding: Optional[Encoding] = None,
    ) -> List[Result]: ...
    @overload
    async def xadd(
        self,
        stream: str,
        id: str,
        items: Dict[str, Arg],
        *,
        maxlen: Optional[int] = None,
        approx: bool = True,
    ) -> str: ...
    @overload
    async def xadd(
        self,
        stream: str,
        items: Dict[str, Arg],
        *,
        id: str = "*",
        maxlen: Optional[int] = None,
        approx: bool = True,
    ) -> str: ...
    @overload
    async def xadd(
        self,
        stream: str,
        *args: Arg,
        id: str = "*",
        maxlen: Optional[int] = None,
        approx: bool = True,
    ) -> str: ...
    @overload
    async def xread(
        self,
        streams: Dict[str, Union[str, Literal["$"], Literal[">"], Literal[0]]],
        *,
        block: Optional[int] = None,
        count: Optional[int] = None,
        noack: Optional[bool] = None,
        group: Optional[str] = None,
        encoding: Optional[Encoding] = None,
    ) -> Dict: ...
    @overload
    async def xread(
        self,
        *streams: str,
        id: Union[None, str, Literal["$"], Literal[">"], Literal[0]] = None,
        block: Optional[int] = None,
        count: Optional[int] = None,
        noack: Optional[bool] = None,
        group: Optional[str] = None,
        encoding: Optional[Encoding] = None,
    ) -> Dict: ...
    async def xack(
        self,
        key: str,
        group: str,
        *id: Union[str, Literal["$"], Literal[0]],
    ) -> Dict: ...

def create_client(
    *args: str,
    max_size: Optional[int] = None,
    cluster: Optional[bool] = None,
    client_id: Optional[str] = None,
) -> Client: ...
