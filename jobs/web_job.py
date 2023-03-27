import logging
from queue import Queue
from typing import List

import requests

from .job import Job

logger = logging.getLogger(__name__)


class WebJob(Job):
    def __init__(
        self, urls: List[str] = None, queue: Queue = None, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.urls = urls or []
        self.queue = queue

    def save_state(self):
        super().save_state()
        self.state["job_type"] = "web_job"

    def target(self):
        try:
            for url in self.urls:
                response = requests.get(url)
                response.raise_for_status()
                logger.info(
                    f"{response.status_code}: {response.content[:100]}"
                )
                if self.queue is not None:
                    self.queue.put(response.content)
                yield response
        except requests.exceptions.HTTPError as error:
            logger.error(error)
            self.retry()
