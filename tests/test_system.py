import pytest
from jobs import SystemJob, SystemAction
from scheduler import Scheduler
from queue import Queue
import json
import pathlib


class TestSystemJob:
    ACTIONS = [
        [SystemAction.CREATE, "file.txt"],
        [SystemAction.CREATE, "folder/file"],
        [SystemAction.CREATE, "folder/folder/file"],
        [SystemAction.DELETE, "folder/folder/file"],
        [SystemAction.CREATE_DIR, "folder/folder/folder"],
        [SystemAction.MOVE, "file.txt", "new_file.txt"],
    ]

    @pytest.fixture
    def clear_system(self):
        yield
        root = pathlib.Path().cwd()
        files = [
            root / "file.txt",
            root / "new_file.txt",
            root / "folder/file"
        ]
        [path.unlink() for path in files if path.exists()]
        folders = [
            root / "folder/folder/folder",
            root / "folder/folder",
            root / "folder",
        ]
        [path.rmdir() for path in folders if path.exists()]

    def test_logging(self, clear, clear_system):
        import logging

        logging.basicConfig(
            format="[%(levelname)s] - %(asctime)s - %(message)s",
            level=logging.DEBUG,
            datefmt="%H:%M:%S",
        )
        scheduler = Scheduler()
        scheduler.run()
        job = SystemJob(max_working_time=3, actions=self.ACTIONS)
        scheduler.schedule(job)
        scheduler.join()
