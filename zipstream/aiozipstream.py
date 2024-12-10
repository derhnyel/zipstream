#
# ZIP File streaming
# based on official ZIP File Format Specification version 6.3.4
# https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT

from typing import Any, AsyncGenerator, Dict, Union
from .base import ZipBase, Processor, MissingSourceError, UnsupportedSourceTypeError
try:
    import aiofiles
    aio_available = True
except ImportError:
    aio_available = False


__all__ = ("AioZipStream",)


class AioZipStream(ZipBase):
    """
    Asynchronous version of ZipStream.

    Args:
        files: List of files to be added to the archive.
        chunksize: Size of chunks to read from files (default 1024 bytes).
        zip64: Use ZIP64 format for large files (default False).

    Raises:
        MissingSourceError: If 'aiofiles' is required but not installed.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(AioZipStream, self).__init__(*args, **kwargs)

    def _create_file_struct(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Creates a file structure with metadata and data source.

        Args:
            data: File data dictionary with 'file' or 'src' key and optional 'name' key.

        Returns:
            A dictionary representing the file structure.

        Raises:
            MissingSourceError: If 'aiofiles' is required but not installed.
        """
        if 'file' in data and not aio_available:
            raise MissingSourceError("The 'aiofiles' module is required to stream files asynchronously.")
        return super(AioZipStream, self)._create_file_struct(data)

    async def data_generator(self, src: Union[str, AsyncGenerator[bytes, None]], src_type: str) -> AsyncGenerator[bytes, None]:
        """
        Asynchronous generator for file data.

        Args:
            src: Source of data.
            src_type: Type of source ('s' for generator, 'f' for file).

        Yields:
            Chunks of file data (bytes).

        Raises:
            UnsupportedSourceTypeError: If an unknown source type is provided.
        """
        if src_type == 's':
            async for chunk in src:
                yield chunk
        elif src_type == 'f':
            async with aiofiles.open(src, "rb") as fh:
                while True:
                    part = await fh.read(self.chunksize)
                    if not part:
                        break
                    yield part
        else:
            raise UnsupportedSourceTypeError(f"Unknown source type: {src_type}")

    async def _stream_single_file(self, file_struct: Dict[str, Any]) -> AsyncGenerator[bytes, None]:
        """
        Asynchronously streams a single file with headers and data descriptors.

        Args:
            file_struct: File structure dictionary with file data and metadata.

        Yields:
            Chunks of file data (bytes).
        """
        yield self._make_local_file_header(file_struct)
        pcs = Processor(file_struct)
        async for chunk in self.data_generator(file_struct['src'], file_struct['stype']):
            processed_chunk = pcs.process(chunk)
            yield processed_chunk
        tail_chunk = pcs.tail()
        if tail_chunk:
            yield tail_chunk
        yield self._make_data_descriptor(file_struct, *pcs.state())

    async def stream(self) -> AsyncGenerator[bytes, None]:
        """
        Streams the complete archive asynchronously.

        Yields:
            Chunks of the archive (bytes).
        """
        for source in self._source_of_files:
            file_struct = self._create_file_struct(source)
            file_struct['offset'] = self._offset_get()
            self._add_file_to_cdir(file_struct)
            async for chunk in self._stream_single_file(file_struct):
                self._offset_add(len(chunk))
                yield chunk
        for chunk in self._make_end_structures():
            yield chunk
        self._cleanup()
