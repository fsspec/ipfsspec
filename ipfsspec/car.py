"""
CAR handling functions.
"""

from typing import List, Optional, Tuple, Union, Iterator, BinaryIO
import dataclasses

import dag_cbor
from multiformats import CID, varint, multicodec, multihash

from .utils import is_cid_list, StreamLike, ensure_stream

DagPbCodec = multicodec.get("dag-pb")
Sha256Hash = multihash.get("sha2-256")

@dataclasses.dataclass
class CARBlockLocation:
    varint_size: int
    cid_size: int
    payload_size: int
    offset: int = 0

    @property
    def cid_offset(self) -> int:
        return self.offset + self.varint_size

    @property
    def payload_offset(self) -> int:
        return self.offset + self.varint_size + self.cid_size

    @property
    def size(self) -> int:
        return self.varint_size + self.cid_size + self.payload_size


def decode_car_header(stream: BinaryIO) -> Tuple[List[CID], int]:
    """
    Decodes a CAR header and returns the list of contained roots.
    """
    header_size, visize, _ = varint.decode_raw(stream)  # type: ignore [call-overload] # varint uses BufferedIOBase
    header = dag_cbor.decode(stream.read(header_size))
    if not isinstance(header, dict):
        raise ValueError("no valid CAR header found")
    if header["version"] != 1:
        raise ValueError("CAR is not version 1")
    roots = header["roots"]
    if not isinstance(roots, list):
        raise ValueError("CAR header doesn't contain roots")
    if not is_cid_list(roots):
        raise ValueError("CAR roots do not only contain CIDs")
    return roots, visize + header_size


def decode_raw_car_block(stream: BinaryIO) -> Optional[Tuple[CID, bytes, CARBlockLocation]]:
    try:
        block_size, visize, _ = varint.decode_raw(stream)  # type: ignore [call-overload] # varint uses BufferedIOBase
    except ValueError:
        # stream has likely been consumed entirely
        return None

    data = stream.read(block_size)
    # as the size of the CID is variable but not explicitly given in
    # the CAR format, we need to partially decode each CID to determine
    # its size and the location of the payload data
    if data[0] == 0x12 and data[1] == 0x20:
        # this is CIDv0
        cid_version = 0
        default_base = "base58btc"
        cid_codec: Union[int, multicodec.Multicodec] = DagPbCodec
        hash_codec: Union[int, multihash.Multihash] = Sha256Hash
        cid_digest = data[2:34]
        data = data[34:]
    else:
        # this is CIDv1(+)
        cid_version, _, data = varint.decode_raw(data)
        if cid_version != 1:
            raise ValueError(f"CIDv{cid_version} is currently not supported")
        default_base = "base32"
        cid_codec, _, data = multicodec.unwrap_raw(data)
        hash_codec, _, data = varint.decode_raw(data)
        digest_size, _, data = varint.decode_raw(data)
        cid_digest = data[:digest_size]
        data = data[digest_size:]
    cid = CID(default_base, cid_version, cid_codec, (hash_codec, cid_digest))

    if not cid.hashfun.digest(data) == cid.digest:
        raise ValueError(f"CAR is corrupted. Entry '{cid}' could not be verified")

    return cid, bytes(data), CARBlockLocation(visize, block_size - len(data), len(data))


def read_car(stream_or_bytes: StreamLike) -> Tuple[List[CID], Iterator[Tuple[CID, bytes, CARBlockLocation]]]:
    """
    Reads a CAR.

    Parameters
    ----------
    stream_or_bytes: StreamLike
        Stream to read CAR from

    Returns
    -------
    roots : List[CID]
        Roots as given by the CAR header
    blocks : Iterator[Tuple[cid, BytesLike, CARBlockLocation]]
        Iterator over all blocks contained in the CAR
    """
    stream = ensure_stream(stream_or_bytes)
    roots, header_size = decode_car_header(stream)
    def blocks() -> Iterator[Tuple[CID, bytes, CARBlockLocation]]:
        offset = header_size
        while (next_block := decode_raw_car_block(stream)) is not None:
            cid, data, sizes = next_block
            yield cid, data, dataclasses.replace(sizes, offset=offset)
            offset += sizes.size
    return roots, blocks()
