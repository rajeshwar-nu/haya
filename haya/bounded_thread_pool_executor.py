import threading
from concurrent.futures.thread import ThreadPoolExecutor


class BoundedThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, max_queue_size, max_workers, *args, **kwargs):
        super(BoundedThreadPoolExecutor, self).__init__(max_workers=max_workers)
        self.semaphore = threading.BoundedSemaphore(value=max_queue_size + max_workers)

    def submit(self, fn, *args, **kwargs):
        self.semaphore.acquire()
        future = super(BoundedThreadPoolExecutor, self).submit(fn, *args, **kwargs)
        future.add_done_callback(lambda x: self.semaphore.release())
        return future
