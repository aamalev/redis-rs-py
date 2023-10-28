__all__ = [
    "create_client",
    "Client",
    "AsyncClient",
    "exceptions",
]

import socket
from typing import Optional
from uuid import uuid4

from .redis_rs import Client, exceptions
from .redis_rs import create_client as _create_client

AsyncClient = Client


def create_client(
    *args: str,
    max_size: Optional[int] = None,
    cluster: Optional[bool] = None,
    client_id: Optional[str] = None,
) -> Client:
    if not client_id:
        client_id = f"{socket.gethostname()}-{uuid4()}"
    return _create_client(*args, max_size=max_size, cluster=cluster, client_id=client_id)
