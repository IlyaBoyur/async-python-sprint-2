import os
import queue
from typing import Any, Callable

import pytest

from jobs import Job
from scheduler import Scheduler


class PassableTargetJob(Job):
    def __init__(
        self, target: Callable = None, args: list[Any] = None, **kwargs
    ):
        self.new_target = target
        self.args = args
        super().__init__(**kwargs)
        self.queue = queue

    def target(self):
        return self.new_target(*self.args)


def source(queue_out):
    for step in range(10):
        queue_out.put(step)
        yield


def processor(queue_in, queue_out, func):
    while True:
        try:
            item = queue_in.get_nowait()
        except queue.Empty:
            break
        else:
            item = func(item)
            queue_out.put(item)
            yield


def target(queue_in, outfile):
    while True:
        try:
            item = queue_in.get_nowait()
        except queue.Empty:
            break
        else:
            with open(outfile, "a") as file:
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
            PassableTargetJob(target=source, args=(queue_in,)),
            PassableTargetJob(
                target=processor, args=(queue_in, queue_out, lambda x: x**2)
            ),
            PassableTargetJob(target=target, args=(queue_out, self.TEST_FILE)),
        ]:
            scheduler.schedule(job)
        scheduler.join()

        with open(self.TEST_FILE) as file:
            assert sum(
                int(value.strip()) for value in file.readlines()
            ) == sum(x**2 for x in range(10))
