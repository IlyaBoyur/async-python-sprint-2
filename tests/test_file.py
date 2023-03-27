import pytest
from jobs import FileJob
from scheduler import Scheduler
from queue import Queue
import json
import os


class TestFileJob:
    buffer = [None]
    ACTIONS = [
        ["w", "file.txt", "file_data"],
        ["r", "non_exist.txt", buffer],
        ["r", "file.txt", buffer],
    ]

    @pytest.fixture
    def clear_files(self):
        yield
        files = [file for _, file, _ in self.ACTIONS]
        [os.unlink(path) for path in files if os.path.exists(path)]

    def test_logging(self, clear, clear_files):
        import logging

        logging.basicConfig(
            format="[%(levelname)s] - %(asctime)s - %(message)s",
            level=logging.DEBUG,
            datefmt="%H:%M:%S",
        )
        scheduler = Scheduler()
        scheduler.run()
        job = FileJob(max_working_time=3, actions=self.ACTIONS)
        scheduler.schedule(job)
        scheduler.join()

        assert self.buffer[0] == "file_data"