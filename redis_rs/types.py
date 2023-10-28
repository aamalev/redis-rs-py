from typing import Literal, Union

Encoding = Union[
    Literal["utf-8"],
    Literal["utf8"],
    Literal["int"],
    Literal["float"],
    Literal["info"],
]
Arg = Union[str, bytes, int, float]
Result = Union[bytes, str, int, float, dict, list]
