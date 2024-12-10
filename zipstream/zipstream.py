from typing import Dict, Generator, Union
from .base import ZipBase, Processor

class ZipStream(ZipBase):

    def data_generator(self, src: Union[str, Generator[bytes, None, None]], src_type: str) -> Generator[bytes, None, None]:
        if src_type == 's':
            for chunk in src:
                yield chunk
        elif src_type == 'f':
            with open(src, "rb") as fh:
                while True:
                    part = fh.read(self.chunksize)
                    if not part:
                        break
                    yield part
        else:
            raise ValueError(f"Unknown source type: {src_type}")

    def _stream_single_file(self, file_struct: Dict[str, Union[str, int, bytes]]) -> Generator[bytes, None, None]:
        """
        Stream single zip file with header and descriptor at the end.
        """
        yield self._make_local_file_header(file_struct)
        pcs = Processor(file_struct)
        for chunk in self.data_generator(file_struct['src'], file_struct['stype']):
            chunk = pcs.process(chunk)
            if chunk:
                yield chunk
        chunk = pcs.tail()
        if chunk:
            yield chunk
        yield self._make_data_descriptor(file_struct, *pcs.state())

    def stream(self) -> Generator[bytes, None, None]:
        """
        Stream complete archive.
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