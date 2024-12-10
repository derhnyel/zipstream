#!/usr/bin/env python3
from __future__ import unicode_literals, absolute_import
import os
import zipfile
import zipstream
import zlib
import struct
import pytest

@pytest.fixture
def temp_files():
    files = []
    yield files
    for f in files:
        os.unlink(f)

def add_temp_file(files, length=None):
    import tempfile, random
    tf = tempfile.mkstemp(prefix="_zipstream_test_", suffix=".txt")[1]
    if length is None:
        length = random.randint(10, 50)
    temptxt = b"This is temporary file.\n"
    idx, pos = 0, 0
    with open(tf, "w") as f:
        while idx < length:
            idx += 1
            f.write(temptxt[pos:pos+1].decode())
            pos += 1
            if pos >= len(temptxt):
                pos = 0
    files.append(tf)
    return tf

def test_structs():
    assert zipfile.sizeEndCentDir == zipstream.consts.CD_END_STRUCT.size
    assert zipfile.sizeCentralDir == zipstream.consts.CDLF_STRUCT.size
    assert zipfile.sizeFileHeader == zipstream.consts.LF_STRUCT.size
    assert zipstream.consts.LF_MAGIC == zipfile.stringFileHeader
    assert zipstream.consts.CDFH_MAGIC == zipfile.stringCentralDir
    assert zipstream.consts.CD_END_MAGIC == zipfile.stringEndArchive

def test_empty_zip():
    with zipfile.ZipFile('/tmp/empty.zip', 'w'):
        pass
    zs = zipstream.ZipStream([])
    with open("/tmp/empty_out.zip", "wb") as fo:
        for f in zs.stream():
            fo.write(f)
    with open("/tmp/empty.zip", "rb") as f1, open("/tmp/empty_out.zip", "rb") as f2:
        bin1 = f1.read()
        bin2 = f2.read()
    assert bin1 == bin2

def test_one_file_add(temp_files):
    with open("/tmp/_tempik_1.txt", "w") as f:
        f.write("foo baz bar")
    with open("/tmp/_tempik_2.txt", "w") as f:
        f.write("baz trololo something")

    zs = zipstream.ZipStream([
        {"file": "/tmp/_tempik_1.txt"},
        {"file": "/tmp/_tempik_2.txt"}
    ])

    res = b""
    with open("/tmp/empty_out.zip", "wb") as f:
        for chunk in zs.stream():
            res += chunk

    assert res[:4] == zipfile.stringFileHeader
    assert res[4:6] == b"\x14\x00"  # version
    assert res[6:8] == b"\x08\x00"  # flags
    assert res[8:10] == b"\x00\x00"  # compression method
    assert res[14:18] == b"\x00\x00\x00\x00"  # crc is set to 0
    assert res[18:22] == b"\x00\x00\x00\x00"  # compressed size is 0
    assert res[22:26] == b"\x00\x00\x00\x00"  # uncompressed size is 0

    pos = res.find(zipstream.consts.LF_MAGIC, 10)

    dd = res[pos-16:pos]
    assert dd[:4] == zipstream.consts.DD_MAGIC

    crc = dd[4:8]
    crc2 = zlib.crc32(b"foo baz bar") & 0xffffffff
    crc2 = struct.pack(b"<L", crc2)
    assert crc == crc2
    assert dd[8:12] == b"\x0b\x00\x00\x00"
    assert dd[12:16] == b"\x0b\x00\x00\x00"

    endstruct = res[-zipstream.consts.CD_END_STRUCT.size:]
    assert endstruct[:4] == zipstream.consts.CD_END_MAGIC
    assert endstruct[8:10] == b"\x02\x00"  # two files in disc
    assert endstruct[10:12] == b"\x02\x00"  # two files total

    cdsize = struct.unpack("<L", endstruct[12:16])[0]
    cdpos = struct.unpack("<L", endstruct[16:20])[0]

    assert cdpos + cdsize == len(res) - zipstream.consts.CD_END_STRUCT.size
    assert res[cdpos:cdpos+4] == zipstream.consts.CDFH_MAGIC

def test_zip64(temp_files):
    large_file_path = add_temp_file(temp_files, length=zipstream.consts.ZIP32_LIMIT + 1)
    
    zs = zipstream.ZipStream([
        {"file": large_file_path}
    ], zip64=True)

    res = b""
    for chunk in zs.stream():
        res += chunk

    # Verify that the ZIP uses ZIP64 structures
    assert res.find(zipstream.consts.CD_END_MAGIC64) != -1  # ZIP64 EOCD record exists
    assert res.find(zipstream.consts.CD_LOC64_MAGIC) != -1   # ZIP64 EOCD locator exists

    # Additional checks to verify the correctness of ZIP64 extra fields and data descriptors

    # Verify ZIP64 extra fields in the local file header
    local_header_offset = 0
    lf_header = res[local_header_offset:local_header_offset + zipstream.consts.LF_STRUCT.size]
    assert lf_header[:4] == zipstream.consts.LF_MAGIC
    # Verify that uncompressed and compressed sizes are 0xFFFFFFFF
    assert lf_header[18:22] == b'\xFF\xFF\xFF\xFF'  # comp_size
    assert lf_header[22:26] == b'\xFF\xFF\xFF\xFF'  # uncomp_size
    # Verify presence of ZIP64 extra field
    assert b'\x01\x00' in res[26:60]  # ZIP64 extra field signature

    # Verify data descriptor has correct values
    dd_signature_index = res.find(zipstream.consts.DD_MAGIC)
    dd = res[dd_signature_index:dd_signature_index + zipstream.consts.DD_STRUCT64.size + 4]
    assert dd[:4] == zipstream.consts.DD_MAGIC
    # Validate CRC, compressed size, and uncompressed size in data descriptor
    # ...additional validation code...

    # Verify ZIP64 End of Central Directory Record and Locator
    eocd64_signature_index = res.find(zipstream.consts.CD_END_MAGIC64)
    assert eocd64_signature_index != -1
    eocd64 = res[eocd64_signature_index:eocd64_signature_index + zipstream.consts.CD_END_STRUCT64.size]
    assert eocd64[:4] == zipstream.consts.CD_END_MAGIC64

    eocd64_locator_index = res.find(zipstream.consts.CD_LOC64_MAGIC)
    assert eocd64_locator_index != -1
    eocd64_locator = res[eocd64_locator_index:eocd64_locator_index + zipstream.consts.CD_LOC64_STRUCT.size]
    assert eocd64_locator[:4] == zipstream.consts.CD_LOC64_MAGIC

    # Verify central directory entries have correct ZIP64 extra fields
    cd_start = res.find(zipstream.consts.CDFH_MAGIC)
    cd_entry = res[cd_start:cd_start + zipstream.consts.CDLF_STRUCT.size]
    assert cd_entry[:4] == zipstream.consts.CDFH_MAGIC
    # ...additional validation code...

if __name__ == '__main__':
    pytest.main()

