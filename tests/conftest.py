import os

import pytest

from scheduler import Scheduler


@pytest.fixture
def clear():
    Scheduler._instances.clear()
    lockfile = "scheduler.lock"
    with open(lockfile, "w"):
        pass
    yield
    if os.path.exists(lockfile):
        os.unlink(lockfile)
