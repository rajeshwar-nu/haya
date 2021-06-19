import logging
import multiprocessing
import os.path
from os.path import abspath
from urllib.parse import urlparse

import boto3
from botocore import UNSIGNED
from botocore.config import Config

from haya.download_process import DownloadProcess
from haya.tasks import FileInfo, GetChunkTask
from haya.utils import (
    GET_OBJECT_CHUNK_SIZE,
    GET_OBJECT_QUE_SIZE,
    N_DOWNLOAD_PROCESSES,
    calculate_num_parts,
    calculate_range_parameter,
)

logger = logging.getLogger()

sync_manager = multiprocessing.Manager()
get_objects_queue = multiprocessing.Queue(maxsize=GET_OBJECT_QUE_SIZE)
file_info_dict = sync_manager.dict()

s3_uri = "s3://commoncrawl/crawl-data/CC-MAIN-2021-25/segments/1623487582767.0/warc/CC-MAIN-20210612103920-20210612133920-00000.warc.gz"
# s3_uri = "s3://tcga-2-open/00072b66-9638-4107-8343-eb46d90bc782/jhu-usc.edu_KIRP.HumanMethylation450.15.lvl-2.TCGA-5P-A9KA-01A-11D-A42K-05.txt"
parsed_url = urlparse(s3_uri)
bucket_name = parsed_url.netloc
key = parsed_url.path[1:]
file_name = key.split("/")[-1]

s3_client = boto3.session.Session(profile_name="rajeshwar2369").client(
    "s3", config=Config(signature_version=UNSIGNED)
)
response = s3_client.head_object(
    Bucket=bucket_name,
    Key=key,
)
logger.info(response)
file_size = response["ContentLength"]

part_size = GET_OBJECT_CHUNK_SIZE

num_parts = calculate_num_parts(file_size, part_size)

os.makedirs(".out", exist_ok=True)
download_path = abspath(f".out/{file_name}")
if os.path.isfile(download_path):
    os.remove(download_path)
with open(download_path, "wb") as out:
    out.truncate(file_size)

file_info_dict[0] = FileInfo(
    0,
    download_path=download_path,
    s3_uri=s3_uri,
    bucket=bucket_name,
    key=key,
    file_name=file_name,
)
procs = []
for _ in range(N_DOWNLOAD_PROCESSES):
    p = DownloadProcess(
        get_object_queue=get_objects_queue, file_info_dict=file_info_dict
    )
    p.start()
    procs.append(p)

for i in range(num_parts):
    range_parameter = calculate_range_parameter(part_size, i, num_parts)
    get_object_task = GetChunkTask(
        file_id=0, start_bytes=i * part_size, range_parameter=range_parameter
    )
    get_objects_queue.put(get_object_task)

for _ in range(N_DOWNLOAD_PROCESSES):
    get_objects_queue.put(None)

for p in procs:
    p.join()

logger.info("finsih")
