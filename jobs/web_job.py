import requests
from .job import Job
from typing import List
import logging
from queue import Queue


URLS_DEFAULT = [
    "https://google.com/",
    "https://ya.ru/",
    "https://www.rambler.ru/",
    "https://www.yahoo.com/",
    "https://www.bing.com/",
]


logger = logging.getLogger(__name__)


class WebJob(Job):
    def __init__(self, urls:List[str]=URLS_DEFAULT, queue: Queue=None, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.urls = urls
        self.queue = queue

    def target(self):
        try:
            for url in self.urls:
                response = requests.get(url)
                response.raise_for_status()
                logger.info(f"{response.status_code}: {response.content[:100]}")
                if self.queue is not None:
                    self.queue.put(response.content)
                yield response
        except requests.exceptions.HTTPError as error:
            logger.error(error)
            self.retry()
