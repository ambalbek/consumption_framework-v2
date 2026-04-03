import logging
from queue import Empty, Queue
from threading import Lock, Thread
from typing import Callable, Dict

from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

logger = logging.getLogger(__name__)


def progress_bar_logging_redirect(func):
    def wrapped(*args, **kwargs):
        with logging_redirect_tqdm():
            return func(*args, **kwargs)

    return wrapped


# Based on https://stackoverflow.com/a/77207002/4642859
class ProgressQueue(Queue):
    def __init__(self, maxsize: int = 0):
        super().__init__(maxsize=maxsize)

        self.processed_tasks = 0
        self.total_tasks = 0

        # The progress bar that we will dynamically update
        self.pbar = tqdm(
            total=self.total_tasks,
            desc="Tasks",
            dynamic_ncols=True,
            disable=None,
            delay=3600,
        )

        # Used for thread-safe progress bar updates
        self.lock = Lock()

    def task_done(self):
        super().task_done()

        self.lock.acquire()
        self.pbar.update()
        self.lock.release()

    def _put(self, item):
        super()._put(item)

        self.lock.acquire()
        self.total_tasks += 1
        self.processed_tasks = self.pbar.n
        self.pbar.reset(self.total_tasks)
        self.pbar.update(self.processed_tasks)
        self.pbar.refresh()
        self.lock.release()

    def join(self):
        super().join()
        self.pbar.close()


class MultithreadingEngine:
    def __init__(
        self,
        workers: int = 10,
    ):
        self.workers = workers

        # Queue of tasks to perform
        # It contains tuples of:
        # - the function to be executed
        # - the parameters to be passed to the function
        self.tasks_queue = ProgressQueue()

        # Flag to stop the workers
        self.running = True

    @progress_bar_logging_redirect
    def submit_task(self, function: Callable, params: Dict):
        """
        Submit a task to the queue.
        """
        self.tasks_queue.put((function, params))

    @progress_bar_logging_redirect
    def __enter__(self):
        logger.info(f"Starting {self.workers} workers to process tasks")

        def _task_wrapper():
            """
            Function run by every worker to process tasks.
            """

            logger.debug("Worker started and waiting for tasks")

            while self.running:
                try:
                    function, params = self.tasks_queue.get_nowait()
                except Empty:
                    continue

                logger.debug(f"Worker processing task: {function.__name__}")

                try:
                    function(**params)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
                finally:
                    self.tasks_queue.task_done()

            logger.info("Worker stopped")

        self.threads = [Thread(target=_task_wrapper) for _ in range(self.workers)]
        [thread.start() for thread in self.threads]

        return self

    @progress_bar_logging_redirect
    def __exit__(self, type, value, traceback):
        """
        Stop the workers.
        """

        while not self.tasks_queue.empty():
            pass

        logger.info("All tasks have been processed, stopping workers")
        self.running = False
        [thread.join() for thread in self.threads]
