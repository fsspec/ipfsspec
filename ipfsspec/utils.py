"""
Some utilities.
"""

from io import BytesIO
from typing import List, Union, BinaryIO

from multiformats import CID
from typing_extensions import TypeGuard

StreamLike = Union[BinaryIO, bytes]

def ensure_stream(stream_or_bytes: StreamLike) -> BinaryIO:
    if isinstance(stream_or_bytes, bytes):
        return BytesIO(stream_or_bytes)
    else:
        return stream_or_bytes


def is_cid_list(os: List[object]) -> TypeGuard[List[CID]]:
    return all(isinstance(o, CID) for o in os)
