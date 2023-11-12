__all__ = [
    "create_client",
    "Client",
    "AsyncClient",
    "exceptions",
]

import socket
from typing import List, Optional
from uuid import uuid4

from .client_async import AsyncClient
from .redis_rs import Client, exceptions
from .redis_rs import create_client as _create_client


def create_client(
    *args: str,
    max_size: Optional[int] = None,
    cluster: Optional[bool] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    db: Optional[int] = None,
    client_id: Optional[str] = None,
    features: Optional[List[str]] = None,
) -> Client:
    if not client_id:
        client_id = f"{socket.gethostname()}-{uuid4()}"
    return _create_client(
        *args,
        max_size=max_size,
        cluster=cluster,
        username=username,
        password=password,
        db=db,
        client_id=client_id,
        features=features,
    )
