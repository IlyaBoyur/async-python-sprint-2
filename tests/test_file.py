import pytest
from jobs import FileJob
from scheduler import Scheduler
from queue import Queue
import json
import os


class TestFileJob:
    ACTIONS = [
        ["w", "file.txt"],
        ["r", "non_exist.txt"],
        ["r", "file.txt"],
    ]

    @pytest.fixture
    def clear_files(self):
        yield
        files = [file for _, file in self.ACTIONS]
        [os.unlink(path) for path in files if os.path.exists(path)]

    def test_write(self, clear, clear_files):
        queue = Queue(maxsize=1)
        queue.put("test_write")

        scheduler = Scheduler()
        scheduler.run()
        job = FileJob(max_working_time=3, actions=[["w", "file.txt"]], queue=queue)
        scheduler.schedule(job)
        scheduler.join()
        
        with open("file.txt", "r") as file:
            assert file.read() == "test_write"


    def test_read(self, clear, clear_files):
        with open("file.txt", "w") as file:
            file.write("test_read")
        queue = Queue(maxsize=1)

        scheduler = Scheduler()
        scheduler.run()
        job = FileJob(max_working_time=3, actions=[["r", "file.txt"]], queue=queue)
        scheduler.schedule(job)
        scheduler.join()
        
        assert queue.get() == "test_read"

