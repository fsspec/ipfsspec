"""
from UNIXFS spec (https://github.com/ipfs/specs/blob/master/UNIXFS.md):

message Data {
	enum DataType {
		Raw = 0;
		Directory = 1;
		File = 2;
		Metadata = 3;
		Symlink = 4;
		HAMTShard = 5;
	}

	required DataType Type = 1;
	optional bytes Data = 2;
	optional uint64 filesize = 3;
	repeated uint64 blocksizes = 4;
	optional uint64 hashType = 5;
	optional uint64 fanout = 6;
	optional uint32 mode = 7;
	optional UnixTime mtime = 8;
}

message Metadata {
	optional string MimeType = 1;
}

message UnixTime {
	required int64 Seconds = 1;
	optional fixed32 FractionalNanoseconds = 2;
}



from DAG-PB spec (https://ipld.io/specs/codecs/dag-pb/spec/):

message PBLink {
  // binary CID (with no multibase prefix) of the target object
  optional bytes Hash = 1;

  // UTF-8 string name
  optional string Name = 2;

  // cumulative size of target object
  optional uint64 Tsize = 3;
}

message PBNode {
  // refs to other objects
  repeated PBLink Links = 2;

  // opaque user data
  optional bytes Data = 1;
}
"""

from dataclasses import dataclass
from enum import IntEnum
from typing import List, Optional

from pure_protobuf.dataclasses_ import field, message  # type: ignore
from pure_protobuf.types import uint32, uint64, int64, fixed32  # type: ignore

class DataType(IntEnum):
    Raw = 0
    Directory = 1
    File = 2
    Metadata = 3
    Symlink = 4
    HAMTShard = 5

@message
@dataclass
class UnixTime:
    Seconds: int64 = field(1)
    FractionalNanoseconds: Optional[fixed32] = field(2)

@message
@dataclass
class Data:
    # pylint: disable=too-many-instance-attributes
    Type: DataType = field(1)
    Data: Optional[bytes] = field(2, default=None)
    filesize: Optional[uint64] = field(3, default=None)
    blocksizes: List[uint64] = field(4, default_factory=list, packed=False)
    hashType: Optional[uint64] = field(5, default=None)
    fanout: Optional[uint64] = field(6, default=None)
    mode: Optional[uint32] = field(7, default=None)
    mtime: Optional[UnixTime] = field(8, default=None)

@message
@dataclass
class Metadata:
    MimeType: Optional[str] = field(1, default=None)


@message
@dataclass
class PBLink:
    Hash: Optional[bytes] = field(1, default=None)
    Name: Optional[str] = field(2, default=None)
    Tsize: Optional[uint64] = field(3, default=None)

Data_ = Data
@message
@dataclass
class PBNode:
    Links: List[PBLink] = field(2, default_factory=list)
    Data: Optional[bytes] = field(1, default=None)
