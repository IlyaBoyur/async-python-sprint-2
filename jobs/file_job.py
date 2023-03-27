import logging
import os
from queue import Queue
from typing import Any, List, Tuple

from .job import Job

logger = logging.getLogger(__name__)


class FileJob(Job):
    def __init__(
        self,
        actions: List[Tuple[str, str, Any]],
        queue: Queue,
        *args,
        **kwargs,
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
                            f"Невозможно прочитать файл {filename}: файл отсутствует"
                        )
                    with open(file=filename, mode=filemode) as file:
                        self.queue.put(file.read())
                else:
                    logger.warning("Режим не поддерживается")
            except RuntimeError as error:
                logger.error(error)
            yield
