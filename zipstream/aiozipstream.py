#
# ZIP File streaming
# based on official ZIP File Format Specification version 6.3.4
# https://pkware.cachefly.net/webdocs/casestudies/APPNOTE.TXT
#
import asyncio
from typing import Any, AsyncGenerator, Dict, Union
from .base import ZipBase, Processor
from concurrent import futures
try:
    import aiofiles
    aio_available = True
except ImportError:
    aio_available = False


__all__ = ("AioZipStream",)


class AioZipStream(ZipBase):
    """
    Asynchronous version of ZipStream
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super(AioZipStream, self).__init__(*args, **kwargs)
        self.__tpex: futures.ThreadPoolExecutor

    def __get_executor(self) -> futures.ThreadPoolExecutor:
        try:
            return self.__tpex
        except AttributeError:
            self.__tpex = futures.ThreadPoolExecutor(max_workers=1)
            return self.__tpex

    async def _execute_aio_task(self, task: Any, *args: Any, **kwargs: Any) -> Any:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(self.__get_executor(), task, *args, **kwargs)

    def _create_file_struct(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if 'file' in data and not aio_available:
            raise Exception("aiofiles module is required to stream files asynchronously")
        return super(AioZipStream, self)._create_file_struct(data)

    async def data_generator(self, src: Union[str, AsyncGenerator[bytes, None]], src_type: str) -> AsyncGenerator[bytes, None]:
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
            raise ValueError(f"Unknown source type: {src_type}")

    async def _stream_single_file(self, file_struct: Dict[str, Any]) -> AsyncGenerator[bytes, None]:
        """
        Stream single zip file with header and descriptor at the end.
        """
        yield self._make_local_file_header(file_struct)
        pcs = Processor(file_struct)
        async for chunk in self.data_generator(file_struct['src'], file_struct['stype']):
            yield await self._execute_aio_task(pcs.process, chunk)
        chunk = await self._execute_aio_task(pcs.tail)
        if chunk:
            yield chunk
        yield self._make_data_descriptor(file_struct, *pcs.state())

    async def stream(self) -> AsyncGenerator[bytes, None]:
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
