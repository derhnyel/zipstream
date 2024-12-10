import os
import time
import zlib
from typing import List, Dict, Generator, Union

from . import consts

class Processor:
    """
    Process file data and compress it if needed
    """

    def __init__(self, file_struct: Dict[str, Union[str, int, None]]) -> None:
        self.crc = 0
        self.o_size = self.c_size = 0
        if file_struct['cmethod'] is None:
            self.process = self._process_through
            self.tail = self._no_tail
        elif file_struct['cmethod'] == 'deflate':
            self.compr = zlib.compressobj(5, zlib.DEFLATED, -15)
            self.process = self._process_deflate
            self.tail = self._tail_deflate

    # no compression
    def _process_through(self, chunk: bytes) -> bytes:
        self.o_size += len(chunk)
        self.c_size = self.o_size
        self.crc = zlib.crc32(chunk, self.crc)
        return chunk

    def _no_tail(self) -> bytes:
        return b''

    # deflate compression
    def _process_deflate(self, chunk: bytes) -> bytes:
        self.o_size += len(chunk)
        self.crc = zlib.crc32(chunk, self.crc)
        chunk = self.compr.compress(chunk)
        self.c_size += len(chunk)
        return chunk

    def _tail_deflate(self) -> bytes:
        chunk = self.compr.flush(zlib.Z_FINISH)
        self.c_size += len(chunk)
        return chunk

    def state(self) -> tuple:
        """
        Return crc, original size and compressed size
        """
        return self.crc, self.o_size, self.c_size


class ZipBase:
    """
    Base class for ZIP file streaming
    """

    def __init__(self, files: List[Dict[str, Union[str, None]]] = [], chunksize: int = 1024, zip64: bool = False) -> None:
        self._source_of_files = files
        self.__files = []
        self.__zip64headers = False
        self.__version = consts.ZIP32_VERSION
        if zip64:
            self.zip64 = None
            self.zip64_required()
        else:
            self.zip64 = zip64
        self.chunksize = chunksize
        self.__use_ddmagic = True
        self.__cdir_size = self.__offset = 0

    def zip64_required(self) -> None:
        """
        Turn on zip64 mode for archive
        """
        if self.zip64:
            return
        if self.zip64 is False:
            # zip64 was explicitly disabled before
            raise Exception("Zip64 mode is required to stream large files")
        self.zip64 = True
        self.__zip64headers = True
        self.__version = consts.ZIP64_VERSION

    def _create_file_struct(self, data: Dict[str, Union[str, None]]) -> Dict[str, Union[str, int, bytes]]:
        dt = time.localtime()
        dosdate = ((dt[0] - 1980) << 9 | dt[1] << 5 | dt[2]) & 0xffff
        dostime = (dt[3] << 11 | dt[4] << 5 | (dt[5] // 2)) & 0xffff

        file_struct = {
            'mod_time': dostime,
            'mod_date': dosdate,
            'crc': 0,
            "offset": 0,  # will be determined at write time
            'flags': 0b00001000  # using data descriptor
        }

        if 'file' in data:
            stats = os.stat(data['file'])
            if stats.st_size > consts.ZIP32_LIMIT:
                self.zip64_required()
            file_struct['src'] = data['file']
            file_struct['stype'] = 'f'
        elif 'stream' in data:
            file_struct['src'] = data['stream']
            file_struct['stype'] = 's'
        else:
            raise Exception('No file or stream in sources')

        cmpr = data.get('compression', None)
        if cmpr not in (None, 'deflate'):
            raise Exception('Unknown compression method %r' % cmpr)
        file_struct['cmethod'] = cmpr
        file_struct['cmpr_id'] = {
            None: consts.COMPRESSION_STORE,
            'deflate': consts.COMPRESSION_DEFLATE
        }[cmpr]

        if 'name' not in data:
            data['name'] = os.path.basename(data['file'])
        try:
            file_struct['fname'] = data['name'].encode("ascii")
        except UnicodeError:
            file_struct['fname'] = data['name'].encode("utf-8")
            file_struct['flags'] |= consts.UTF8_FLAG

        return file_struct

    def _make_extra_field(self, signature: int, data: bytes) -> bytes:
        fields = {"signature": signature,
                  "size": len(data)}
        head = consts.EXTRA_TUPLE(**fields)
        head = consts.EXTRA_STRUCT.pack(*head)
        return head + data

    def make_zip64_cdir_extra(self, fsize: int, compsize: int, offset: int) -> bytes:
        fields = {
            "uncomp_size": fsize,
            "comp_size": compsize,
            "offset": offset
        }
        data = consts.CD_EXTRA_64_TUPLE(**fields)
        data = consts.CD_EXTRA_64_STRUCT.pack(*data)
        return self._make_extra_field(0x0001, data)

    def make_zip64_local_extra(self, fsize: int, compsize: int) -> bytes:
        fields = {
            "uncomp_size": fsize,
            "comp_size": compsize
        }
        data = consts.EXTRA_64_TUPLE(**fields)
        data = consts.EXTRA_64_STRUCT.pack(*data)
        return self._make_extra_field(0x0001, data)

    def _make_local_file_header(self, file_struct: Dict[str, Union[str, int, bytes]]) -> bytes:
        fields = {
            "signature": consts.LF_MAGIC,
            "version": self.__version,
            "flags": file_struct['flags'],
            "compression": file_struct['cmpr_id'],
            "mod_time": file_struct['mod_time'],
            "mod_date": file_struct['mod_date'],
            "crc": 0,
            "uncomp_size": 0,
            "comp_size": 0,
            "fname_len": len(file_struct['fname']),
            "extra_len": 0
        }
        if self.__zip64headers:
            fields['uncomp_size'] = 0xffffffff
            fields['comp_size'] = 0xffffffff
            z64extra = self.make_zip64_local_extra(0, 0)
            fields['extra_len'] = len(z64extra)
        head = consts.LF_TUPLE(**fields)
        head = consts.LF_STRUCT.pack(*head)
        head += file_struct['fname']
        if self.__zip64headers:
            head += z64extra
        return head

    def _make_data_descriptor(self, file_struct: Dict[str, Union[str, int, bytes]], crc: int, org_size: int, compr_size: int) -> bytes:
        file_struct['crc'] = crc & 0xffffffff
        file_struct['size'] = org_size
        file_struct['csize'] = compr_size
        fields = {
            "uncomp_size": file_struct['size'],
            "comp_size": file_struct['csize'],
            "crc": file_struct['crc']
        }
        descriptor = consts.DD_TUPLE(**fields)

        if self.__zip64headers:
            descriptor = consts.DD_STRUCT64.pack(*descriptor)
        else:
            descriptor = consts.DD_STRUCT.pack(*descriptor)

        if self.__use_ddmagic:
            descriptor = consts.DD_MAGIC + descriptor

        return descriptor

    def _make_cdir_file_header(self, file_struct: Dict[str, Union[str, int, bytes]]) -> bytes:
        """
        Create central directory file header
        According to the ZIP spec and the structures:
        version (low byte) and system (high byte) form "version made by".
        """
        fields = {
            "signature": consts.CDFH_MAGIC,
            "version": self.__version,
            "system": 0x03,  # Unix
            "version_ndd": self.__version,
            "flags": file_struct['flags'],
            "compression": file_struct['cmpr_id'],
            "mod_time": file_struct['mod_time'],
            "mod_date": file_struct['mod_date'],
            "crc": file_struct['crc'],
            "comp_size": file_struct['csize'],
            "uncomp_size": file_struct['size'],
            "fname_len": len(file_struct['fname']),
            "extra_len": 0,
            "fcomm_len": 0,
            "disk_start": 0,
            "attrs_int": 0,
            "attrs_ext": 0,
            "offset": file_struct['offset']
        }

        if self.__zip64headers:
            # use ZIP64 placeholders
            fields['uncomp_size'] = 0xffffffff
            fields['comp_size'] = 0xffffffff
            fields['offset'] = 0xffffffff
            z64extra = self.make_zip64_cdir_extra(
                file_struct['size'],
                file_struct['size'],
                file_struct['offset']
            )
            fields['extra_len'] = len(z64extra)

        cdfh = consts.CDLF_TUPLE(**fields)
        cdfh = consts.CDLF_STRUCT.pack(*cdfh)
        cdfh += file_struct['fname']
        if self.__zip64headers:
            cdfh += z64extra
        return cdfh

    def _make_cdend(self) -> bytes:
        fields = {
            "signature": consts.CD_END_MAGIC,
            "disk_num": 0,
            "disk_cdstart": 0,
            "disk_entries": len(self.__files),
            "total_entries": len(self.__files),
            "cd_size": self.__cdir_size,
            "cd_offset": self._offset_get(),
            "comment_len": 0
        }
        if self.__zip64headers:
            fields['disk_entries'] = 0xffff
            fields['total_entries'] = 0xffff
            fields['cd_size'] = 0xffffffff
            fields['cd_offset'] = 0xffffffff
        cdend = consts.CD_END_TUPLE(**fields)
        cdend = consts.CD_END_STRUCT.pack(*cdend)
        return cdend

    def make_cdend64(self) -> bytes:
        """
        make zip64 end of central directory record
        """
        fields = {
            "signature": consts.CD_END_MAGIC64,
            "zip64_eocd_size": consts.CD_END_STRUCT64.size - 12,
            "version": self.__version,
            "version_ndd": self.__version,
            "disk_num": 0,
            "disk_cdstart": 0,
            "disk_entries": len(self.__files),
            "total_entries": len(self.__files),
            "cd_size": self.__cdir_size,
            "cd_offset": self.__offset
        }
        cdend = consts.CD_END_TUPLE64(**fields)
        cdend = consts.CD_END_STRUCT64.pack(*cdend)
        return cdend

    def make_eocd64_locator(self, offset: int) -> bytes:
        fields = {
            "signature": consts.CD_LOC64_MAGIC,
            "disk_cdstart": 0,
            "offset": offset,
            "disk_count": 1
        }
        cdend = consts.CD_LOC64_TUPLE(**fields)
        cdend = consts.CD_LOC64_STRUCT.pack(*cdend)
        return cdend

    def _make_end_structures(self) -> Generator[bytes, None, None]:
        # Write all central directory entries
        for file_struct in self.__files:
            chunk = self._make_cdir_file_header(file_struct)
            self.__cdir_size += len(chunk)
            yield chunk
        # If using ZIP64
        if self.__zip64headers:
            # Calculate EOCD64 position
            eocd64position = self.__offset + self.__cdir_size
            # Write ZIP64 EOCD
            yield self.make_cdend64()
            # Write ZIP64 EOCD locator
            yield self.make_eocd64_locator(eocd64position)
        # Write EOCD
        yield self._make_cdend()

    def _offset_add(self, value: int) -> None:
        self.__offset += value

    def _offset_get(self) -> int:
        return self.__offset

    def _add_file_to_cdir(self, file_struct: Dict[str, Union[str, int, bytes]]) -> None:
        self.__files.append(file_struct)

    def _cleanup(self) -> None:
        self.__files = []
        self.__cdir_size = self.__offset = 0

    def total_size(self) -> int:
        total_size = 0
        for file in self._source_of_files:
            if 'file' in file:
                total_size += os.path.getsize(file['file'])
            elif 'stream' in file:
                raise Exception("Cannot determine size of stream sources")
        return total_size