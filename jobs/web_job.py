import requests
from .job import Job
from typing import List
import logging


URLS_DEFAULT = [
    "https://google.com/",
    "https://ya.ru/",
    "https://www.rambler.ru/",
    "https://www.yahoo.com/",
    "https://www.bing.com/",
]


logger = logger.getLogger(__name__)


class WebJob(Job):
    def __init__(self, urls:List[str]=URLS_DEFAULT):
        super().__init__()
        self.urls = urls

    def target(self):
        super().target()
        try:
            for url in self.urls:
                response = requests.get(url)
                response.raise_for_status()
                logger.info(f"{response.status_code}: {response.content[:100]}")
                yield response
        except requests.exceptions.HTTPError as error:
            logger.error(error)
            self.retry()
