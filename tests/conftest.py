import pytest
from scheduler import Scheduler
import os

@pytest.fixture
def clear():
    Scheduler._instances.clear()
    lockfile = "scheduler.lock"
    with open(lockfile, "w") as file:
        pass
    yield
    if os.path.exists(lockfile):
        os.unlink(lockfile) 
