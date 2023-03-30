import logging
from dataclasses import dataclass
from queue import Queue
from typing import Any, ClassVar

import requests

from .constants import JobType
from .job import Job, JobMomento

logger = logging.getLogger(__name__)


@dataclass
class WebJobMomento(JobMomento):
    TYPE: ClassVar[JobType] = JobType.WEB
    urls: list[str]
    queue: Queue


class WebJob(Job):
    def __init__(
        self, urls: list[str] = None, queue: Queue = None, *args, **kwargs
    ):
        self.urls = urls or []
        self.queue = queue
        super().__init__(*args, **kwargs)

    def create_momento(self, defaults: dict[str, Any]):
        return WebJobMomento(**defaults, urls=self.urls, queue=self.queue)

    def target(self):
        try:
            for url in self.urls:
                response = requests.get(url)
                response.raise_for_status()
                logger.info(
                    f"Status: %s. Content: %s",
                    response.status_code,
                    response.content[:100],
                )
                if self.queue is not None:
                    self.queue.put(response.content)
                yield response
        except requests.exceptions.HTTPError as error:
            logger.error(error)
            self.retry()
