import json
from queue import Queue

from jobs import JobType, WebJob
from scheduler import Scheduler

URLS_DEFAULT = [
    "https://google.com/",
    "https://ya.ru/",
    "https://www.rambler.ru/",
    "https://www.yahoo.com/",
    "https://www.bing.com/",
]


class TestWebJob:
    def test_default(self):
        job = WebJob(max_working_time=3, urls=URLS_DEFAULT)
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
        job = WebJob(max_working_time=3, urls=URLS_DEFAULT)
        scheduler.schedule(job)
        scheduler.join()

    def test_queue(self):
        import json

        CITIES = [
            "https://code.s3.yandex.net/async-module/moscow-response.json",
            "https://code.s3.yandex.net/async-module/paris-response.json",
            "https://code.s3.yandex.net/async-module/london-response.json",
        ]
        test_queue = Queue()
        job = WebJob(urls=CITIES, queue=test_queue)
        scheduler = Scheduler(pool_size=3)
        scheduler.run()
        scheduler.schedule(job)
        scheduler.join()

        size = test_queue.qsize()
        assert size == len(CITIES)
        responses = [json.loads(test_queue.get()) for i in range(len(CITIES))]
        for response in responses:
            assert "info" in response
            assert "geo_object" in response
            assert "fact" in response
            assert "yesterday" in response
            assert "forecasts" in response

    def test_stop(self):
        job = WebJob(max_working_time=3, urls=URLS_DEFAULT)
        scheduler = Scheduler(pool_size=3)
        scheduler.run()
        scheduler.schedule(job)
        scheduler.stop()

        with open("scheduler.lock", "r") as file:
            data = json.load(file)

        assert len(data["active"]) == 1
        for job in data["active"]:
            assert job["type"] == JobType.WEB
            body = job["task_body"]
            assert "start_at" in body
            assert "max_working_time" in body
            assert "tries" in body
            assert "dependencies" in body
            assert "urls" in body
            assert "queue" in body
