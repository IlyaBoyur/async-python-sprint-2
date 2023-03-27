import pytest
from jobs import Job, FileJob
from scheduler import Scheduler
import queue
import json
import os
from typing import Callable, Any


class SourceJob(Job):
    def __init__(self, queue: queue.Queue):
        super().__init__()
        self.queue = queue

    def target(self):
        for step in range(10):
            self.queue.put(step)
            yield


class ProcessorJob(Job):
    def __init__(self, queue_in: queue.Queue, queue_out: queue.Queue, func: Callable[[Any],Any]):
        super().__init__()
        self.queue_in = queue_in
        self.queue_out = queue_out
        self.func = func

    def target(self):
        while True:
            try:
                item = self.queue_in.get_nowait()
            except queue.Empty:
                break
            else:
                item = self.func(item)
                self.queue_out.put(item)
                yield


class TargetJob(Job):
    def __init__(self, queue: queue.Queue, outfile: str):
        super().__init__()
        self.queue = queue
        self.outfile = outfile

    def target(self):
        while True:
            try:
                item = self.queue.get_nowait()
            except queue.Empty:
                break
            else:
                with open(self.outfile, "a") as file:
                    file.write(f"{item}\n")
                yield


class TestConveyorJob:
    TEST_FILE = "test_file.txt"

    @pytest.fixture
    def clear_files(self):
        yield
        if os.path.exists(self.TEST_FILE):
            os.unlink(self.TEST_FILE)

    def test_conveyor(self, clear_files):
        queue_in = queue.Queue()
        queue_out = queue.Queue()

        scheduler = Scheduler()
        scheduler.run()
        for job in [
            SourceJob(queue=queue_in),
            ProcessorJob(queue_in=queue_in, queue_out=queue_out, func=lambda x: x ** 2),
            TargetJob(queue=queue_out, outfile=self.TEST_FILE)
        ]:
            scheduler.schedule(job)
        scheduler.join()

        with open(self.TEST_FILE) as file:
            assert sum(int(value.strip()) for value in file.readlines()) == sum(x ** 2 for x in range(10))