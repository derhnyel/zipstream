from typing import Dict, Generator, Union
from .base import ZipBase, Processor, UnsupportedSourceTypeError

class ZipStream(ZipBase):
    """
    Main class for streaming ZIP archives.

    Args:
        files: List of files to be added to the archive (each file is a dictionary with 'file' or 'src' key).
        chunksize: Size of chunks to read from files (default 8KB).
        zip64: Use ZIP64 format for large files (default False).
    """

    def data_generator(
        self,
        src: Union[str, Generator[bytes, None, None]],
        src_type: str,
    ) -> Generator[bytes, None, None]:
        """
        Generator for file data.

        Args:
            src: Source of data.
            src_type: Type of source ('s' for generator, 'f' for file).

        Yields:
            Chunks of file data (bytes).

        Raises:
            UnsupportedSourceTypeError: If an unknown source type is provided.
        """
        if src_type == 's':
            yield from src
        elif src_type == 'f':
            with open(src, "rb") as fh:
                for part in iter(lambda: fh.read(self.chunksize), b''):
                    yield part
        else:
            raise UnsupportedSourceTypeError(f"Unknown source type '{src_type}' provided.")

    def _stream_single_file(
        self, file_struct: Dict[str, Union[str, int, bytes]]
    ) -> Generator[bytes, None, None]:
        """
        Streams a single file with headers and data descriptors.

        Args:
            file_struct: File structure dictionary with file data and metadata.

        Yields:
            Chunks of file data (bytes).
        """
        yield self._make_local_file_header(file_struct)
        pcs = Processor(file_struct)
        for chunk in self.data_generator(file_struct['src'], file_struct['stype']):
            yield pcs.process(chunk)
        tail_chunk = pcs.tail()
        if tail_chunk:
            yield tail_chunk
        yield self._make_data_descriptor(file_struct, *pcs.state())

    def stream(self) -> Generator[bytes, None, None]:
        """
        Streams the complete archive.

        Yields:
            Chunks of the archive (bytes).
        """
        for source in self._source_of_files:
            file_struct = self._create_file_struct(source)
            file_struct['offset'] = self._offset_get()
            self._add_file_to_cdir(file_struct)
            for chunk in self._stream_single_file(file_struct):
                self._offset_add(len(chunk))
                yield chunk
        for chunk in self._make_end_structures():
            yield chunk
        self._cleanup()