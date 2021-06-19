import logging
import mmap
import multiprocessing
import os
from typing import Dict

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from haya.bounded_thread_pool_executor import BoundedThreadPoolExecutor
from haya.tasks import FileInfo, GetChunkTask, WriteChunkTask
from haya.utils import (
    IO_CHUNK_SIZE,
    KB,
    MAX_GET_CHUNK_EXECUTOR_QUEUE,
    MAX_GET_CHUNK_WORKERS,
    MAX_WRITE_EXECUTOR_QUEUE,
    MAX_WRITE_WORKERS,
    NoMoreWriteTasksException,
)

logger = logging.getLogger()


class DownloadProcess(multiprocessing.Process):
    def __init__(self, get_object_queue: multiprocessing.Queue, file_info_dict: dict):
        super().__init__(daemon=True)
        self.s3_client = boto3.session.Session().client(
            "s3", config=Config(signature_version=UNSIGNED)
        )
        self.get_object_queue = get_object_queue
        self.file_info_dict: Dict[int, FileInfo] = file_info_dict
        self.write_io_executor = BoundedThreadPoolExecutor(
            max_workers=MAX_WRITE_WORKERS, max_queue_size=MAX_WRITE_EXECUTOR_QUEUE
        )
        self.get_object_executor = BoundedThreadPoolExecutor(
            max_workers=MAX_GET_CHUNK_WORKERS,
            max_queue_size=MAX_GET_CHUNK_EXECUTOR_QUEUE,
        )

    def write_chunk(self, write_chunk_task: WriteChunkTask):
        fd = None
        try:
            flags = os.O_RDWR
            if write_chunk_task.direct:
                flags |= os.O_DIRECT
            fd = os.open(
                self.file_info_dict[write_chunk_task.file_id].download_path, flags=flags
            )

            os.lseek(fd, write_chunk_task.start_bytes, os.SEEK_SET)
            os.write(fd, write_chunk_task.data)
        except Exception as e:
            logger.exception(f"Fail -> {write_chunk_task.chunk_len}", exc_info=e)
        finally:
            if fd:
                os.close(fd)
            write_chunk_task.data.close()

    def get_chunk(self, get_chunk_task: GetChunkTask):
        try:
            current_index = get_chunk_task.start_bytes
            file_info: FileInfo = self.file_info_dict[get_chunk_task.file_id]
            response = self.s3_client.get_object(
                Bucket=file_info.bucket,
                Key=file_info.key,
                Range=get_chunk_task.range_parameter,
            )

            for chunk in response["Body"].iter_chunks(IO_CHUNK_SIZE):
                chunk_len = len(chunk)
                if chunk_len % (4 * KB) == 0:
                    direct = True
                    m = mmap.mmap(-1, IO_CHUNK_SIZE)
                    m.write(chunk)
                    chunk_data = m
                else:
                    direct = False
                    chunk_data = chunk

                write_task = WriteChunkTask(
                    data=chunk_data,
                    start_bytes=current_index,
                    file_id=file_info.file_id,
                    chunk_len=len(chunk),
                    direct=direct,
                )
                self.write_io_executor.submit(self.write_chunk, write_task)
                current_index += IO_CHUNK_SIZE
        except Exception as e:
            logger.exception(e)

    def run(self) -> None:
        failed = False
        try:
            while True:
                get_chunk_task: GetChunkTask = self.get_object_queue.get()
                if not get_chunk_task:
                    raise NoMoreWriteTasksException()
                self.get_object_executor.submit(self.get_chunk, get_chunk_task)
        except NoMoreWriteTasksException:
            failed = False
            logger.info("Downloader finished")
        except Exception as e:
            logger.exception(e)
            failed = True
        finally:
            self.get_object_executor.shutdown(wait=(not failed))
            self.write_io_executor.shutdown(wait=(not failed))
