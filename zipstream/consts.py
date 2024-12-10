import struct
from typing import NamedTuple

# zip constants
ZIP32_VERSION: int = 20
ZIP64_VERSION: int = 45
ZIP32_LIMIT: int = (1 << 31) - 1
UTF8_FLAG: int = 0x800   # utf-8 filename encoding flag

# zip compression methods
COMPRESSION_STORE: int = 0
COMPRESSION_DEFLATE: int = 8

# file header
LF_STRUCT: struct.Struct = struct.Struct(b"<4sHHHHHLLLHH")
class FileHeader(NamedTuple):
    signature: bytes
    version: int
    flags: int
    compression: int
    mod_time: int
    mod_date: int
    crc: int
    comp_size: int
    uncomp_size: int
    fname_len: int
    extra_len: int
LF_TUPLE = FileHeader
LF_MAGIC: bytes = b'\x50\x4b\x03\x04'

# extra fields
EXTRA_STRUCT: struct.Struct = struct.Struct(b"<HH")
class Extra(NamedTuple):
    signature: int
    size: int
EXTRA_TUPLE = Extra

EXTRA_64_STRUCT: struct.Struct = struct.Struct(b"<QQ")
class Extra64Local(NamedTuple):
    uncomp_size: int
    comp_size: int
EXTRA_64_TUPLE = Extra64Local

CD_EXTRA_64_STRUCT: struct.Struct = struct.Struct(b"<QQQ")
class Extra64Cdir(NamedTuple):
    uncomp_size: int
    comp_size: int
    offset: int
CD_EXTRA_64_TUPLE = Extra64Cdir

# file descriptor
DD_STRUCT: struct.Struct = struct.Struct(b"<LLL")
DD_STRUCT64: struct.Struct = struct.Struct(b"<LQQ")
class FileCrc(NamedTuple):
    crc: int
    comp_size: int
    uncomp_size: int
DD_TUPLE = FileCrc
DD_MAGIC: bytes = b'\x50\x4b\x07\x08'

# central directory file header
# Note: version and system fields represent "version made by"
CDLF_STRUCT: struct.Struct = struct.Struct(b"<4sBBHHHHHLLLHHHHHLL")
class CdFileHeader(NamedTuple):
    signature: bytes
    version: int    # low-order byte of 'version made by'
    system: int     # high-order byte (host OS)
    version_ndd: int
    flags: int
    compression: int
    mod_time: int
    mod_date: int
    crc: int
    comp_size: int
    uncomp_size: int
    fname_len: int
    extra_len: int
    fcomm_len: int
    disk_start: int
    attrs_int: int
    attrs_ext: int
    offset: int
CDLF_TUPLE = CdFileHeader
CDFH_MAGIC: bytes = b'\x50\x4b\x01\x02'

# end of central directory record
CD_END_STRUCT: struct.Struct = struct.Struct(b"<4sHHHHLLH")
class CdEnd(NamedTuple):
    signature: bytes
    disk_num: int
    disk_cdstart: int
    disk_entries: int
    total_entries: int
    cd_size: int
    cd_offset: int
    comment_len: int
CD_END_TUPLE = CdEnd
CD_END_MAGIC: bytes = b'\x50\x4b\x05\x06'

# zip64 end of central directory record
CD_END_STRUCT64: struct.Struct = struct.Struct(b"<4sQHHIIQQQQ")
class CdEnd64(NamedTuple):
    signature: bytes
    zip64_eocd_size: int
    version: int
    version_ndd: int
    disk_num: int
    disk_cdstart: int
    disk_entries: int
    total_entries: int
    cd_size: int
    cd_offset: int
CD_END_TUPLE64 = CdEnd64
CD_END_MAGIC64: bytes = b'\x50\x4b\x06\x06'

# zip64 end of central directory locator
CD_LOC64_STRUCT: struct.Struct = struct.Struct(b"<4sLQL")
class CdFileHeader64(NamedTuple):
    signature: bytes
    disk_cdstart: int
    offset: int
    disk_count: int
CD_LOC64_TUPLE = CdFileHeader64
CD_LOC64_MAGIC: bytes = b'\x50\x4b\x06\x07'