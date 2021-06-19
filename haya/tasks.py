import typing
from mmap import mmap
from typing import Union


class FileInfo(typing.NamedTuple):
    file_id: int
    download_path: str
    s3_uri: str
    file_name: str
    bucket: str
    key: str


class GetChunkTask(typing.NamedTuple):
    file_id: int
    start_bytes: int
    range_parameter: str


class WriteChunkTask(typing.NamedTuple):
    data: Union[mmap, bytes]
    start_bytes: int
    file_id: int
    chunk_len: int
    direct: bool
