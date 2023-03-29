import logging
import os
from queue import Queue
from typing import Any

from .job import Job

logger = logging.getLogger(__name__)


class FileJob(Job):
    def __init__(
        self, actions: list[tuple[str, Any]], queue: Queue, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.actions = actions
        self.queue = queue

    def save_state(self):
        super().save_state()
        self.state["job_type"] = "file_job"

    def target(self):
        for filemode, filename in self.actions:
            try:
                if filemode in ("w", "a"):
                    with open(file=filename, mode=filemode) as file:
                        if not self.queue.empty():
                            file.write(self.queue.get())
                elif filemode == "r":
                    if not os.path.exists(filename):
                        raise RuntimeError(
                            f"Cannot read file {filename}: the file is missing"
                        )
                    with open(file=filename, mode=filemode) as file:
                        self.queue.put(file.read())
                else:
                    logger.warning(f"Filemode `%s` is not supported", filemode)
            except RuntimeError as error:
                logger.error(error)
            yield
