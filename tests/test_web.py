import pytest
from jobs.job import WebJob
from scheduler import Scheduler


class TestWebJob:
    def test_default(self):
        job = WebJob()
        scheduler = Scheduler(3)
        scheduler.run()
        scheduler.schedule(job)
        scheduler.join()
