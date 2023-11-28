from typing import Dict, List, Literal, Optional, Union, overload

from redis_rs.types import Arg, Encoding, Result

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
    async def exists(self, *keys: str) -> int: ...
    async def expire(self, key: str, seconds: int, option: Optional[str] = None) -> int: ...
    async def delete(self, *keys: str) -> int: ...
    async def keys(self, pattern: str, encoding: Optional[str] = None) -> int: ...
    async def eval(
        self, script: str, numkeys: int, *keys_and_args: Arg, encoding: Optional[Encoding] = None
    ) -> Result: ...
    async def set(self, key: str, value: Arg) -> Result: ...
    async def get(self, key: str, *, encoding: Optional[Encoding] = None) -> Result: ...
    @overload
    async def hset(self, key: str, field: str, value: Arg, *pairs) -> int: ...
    @overload
    async def hset(self, key: str, mapping: Dict[str, Arg]) -> int: ...
    async def hget(self, key: str, field: str, *, encoding: Optional[Encoding] = None) -> Result: ...
    async def hmget(self, key: str, *fields: str, encoding: Optional[Encoding] = None) -> Result: ...
    async def hgetall(self, key: str, *, encoding: Optional[Encoding] = None) -> Dict: ...
    async def hdel(self, key: str, *fields: str) -> int: ...
    async def hexists(self, key: str, field: str) -> bool: ...
    async def incr(self, key: str, delta: Union[None, int, float] = None) -> float: ...
    async def lpush(self, key: str, value: Arg) -> int: ...
    async def rpush(self, key: str, value: Arg) -> int: ...
    async def lpop(self, key: str, *, count=None, encoding: Optional[Encoding] = None) -> Result: ...
    async def lrem(self, key: str, count: int, element: Arg) -> Result: ...
    async def blpop(self, *keys: str, timeout, encoding: Optional[Encoding] = None) -> Result: ...
    async def lrange(
        self,
        key: str,
        start: int = 0,
        stop: int = -1,
        *,
        encoding: Optional[Encoding] = None,
    ) -> List[Result]: ...
    async def llen(self, key: str) -> int: ...
    async def pfadd(self, key: str, *elements: Arg) -> bool: ...
    async def pfcount(self, *keys: str) -> int: ...
    async def pfmerge(self, destkey: str, *sourcekeys: str) -> bool: ...
    @overload
    async def xadd(
        self,
        stream: str,
        id: str,
        items: Dict[str, Arg],
        *,
        mkstream: bool = True,
        maxlen: Optional[int] = None,
        minid: Optional[int] = None,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> str: ...
    @overload
    async def xadd(
        self,
        stream: str,
        items: Dict[str, Arg],
        *,
        id: str = "*",
        mkstream: bool = True,
        maxlen: Optional[int] = None,
        minid: Optional[int] = None,
        approx: bool = True,
        limit: Optional[int] = None,
    ) -> str: ...
    @overload
    async def xadd(
        self,
        stream: str,
        *args: Arg,
        id: str = "*",
        mkstream: bool = True,
        maxlen: Optional[int] = None,
        minid: Optional[int] = None,
        approx: bool = True,
        limit: Optional[int] = None,
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
    ) -> int: ...
    @overload
    async def zadd(
        self,
        key: str,
        score: float,
        value: str,
        *pairs: Arg,
    ) -> int: ...
    @overload
    async def zadd(
        self,
        key: str,
        *args: Arg,
        score: Optional[float] = None,
        incr: Optional[float] = None,
    ) -> int: ...
    @overload
    async def zrange(
        self,
        key: str,
        start: Union[int, str] = 0,
        stop: Union[int, str] = -1,
    ) -> List[str]: ...
    @overload
    async def zrange(
        self,
        key: str,
        start: Union[int, str] = 0,
        stop: Union[int, str] = -1,
        *,
        withscores: Literal[True],
    ) -> Dict[str, float]: ...
    async def zcard(self, key: str) -> int: ...
    async def zrem(self, key: str, *members: str) -> int: ...
