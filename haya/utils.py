import math


def calculate_range_parameter(part_size, part_index, num_parts, total_size=None):
    """Calculate the range parameter for multipart downloads/copies

    :type part_size: int
    :param part_size: The size of the part

    :type part_index: int
    :param part_index: The index for which this parts starts. This index starts
        at zero

    :type num_parts: int
    :param num_parts: The total number of parts in the transfer

    :returns: The value to use for Range parameter on downloads or
        the CopySourceRange parameter for copies
    """
    # Used to calculate the Range parameter
    start_range = part_index * part_size
    if part_index == num_parts - 1:
        end_range = ""
        if total_size is not None:
            end_range = str(total_size - 1)
    else:
        end_range = start_range + part_size - 1
    range_param = "bytes=%s-%s" % (start_range, end_range)
    return range_param


def calculate_num_parts(size, part_size):
    return int(math.ceil(size / float(part_size)))


class NoMoreWriteTasksException(Exception):
    pass


KB = 1024
MB = int(KB * KB)
GB = int(MB * KB)
N_DOWNLOAD_PROCESSES = 8
GET_OBJECT_QUE_SIZE = 10000
GET_OBJECT_CHUNK_SIZE = 4 * MB
MAX_GET_CHUNK_EXECUTOR_QUEUE = 100
MAX_GET_CHUNK_WORKERS = 8
MAX_WRITE_EXECUTOR_QUEUE = 100
MAX_WRITE_WORKERS = 4
IO_CHUNK_SIZE = 256 * KB
