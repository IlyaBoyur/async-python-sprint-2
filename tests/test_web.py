import pytest
from jobs import WebJob, URLS_DEFAULT
from scheduler import Scheduler
from queue import Queue


class TestWebJob:
    def test_default(self):
        job = WebJob(max_working_time=3)
        scheduler = Scheduler(pool_size=3)
        scheduler.run()
        scheduler.schedule(job)
        scheduler.join()

    def test_logging(self):
        import logging

        logging.basicConfig(
            format="[%(levelname)s] - %(asctime)s - %(message)s",
            level=logging.DEBUG,
            datefmt="%H:%M:%S",
        )
        scheduler = Scheduler(pool_size=3)
        scheduler.run()
        job = WebJob(max_working_time=3)
        scheduler.schedule(job)
        scheduler.join()


    def test_queue(self):
        import json

        CITIES = {
            "MOSCOW": "https://code.s3.yandex.net/async-module/moscow-response.json",
            "PARIS": "https://code.s3.yandex.net/async-module/paris-response.json",
            "LONDON": "https://code.s3.yandex.net/async-module/london-response.json",
        }
        test_queue = Queue()
        job = WebJob(urls=CITIES.values(), queue=test_queue)
        scheduler = Scheduler(pool_size=3)
        scheduler.run()
        scheduler.schedule(job)
        scheduler.join()

        size = test_queue.qsize()
        assert size == len(CITIES)
        result = []
        result.extend(json.loads(test_queue.get())  for i in range(len(CITIES)))
            
        for response in result:
            assert "info" in response
            assert "geo_object" in response
            assert "fact" in response
            assert "yesterday" in response
            assert "forecasts" in response