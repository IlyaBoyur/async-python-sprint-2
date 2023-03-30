import logging
import os
from dataclasses import dataclass
from queue import Queue
from typing import Any, ClassVar

from .constants import JobType
from .job import Job, JobMomento

logger = logging.getLogger(__name__)


@dataclass
class FileJobMomento(JobMomento):
    TYPE: ClassVar[JobType] = JobType.FILE
    actions: list[tuple[str, Any]]
    queue: Queue


class FileJob(Job):
    def __init__(
        self, actions: list[tuple[str, Any]], queue: Queue, *args, **kwargs
    ):
        self.actions = actions
        self.queue = queue
        super().__init__(*args, **kwargs)

    def create_momento(self, defaults: dict[str, Any]):
        return FileJobMomento(
            **defaults, actions=self.actions, queue=self.queue
        )

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
