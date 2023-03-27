import pytest
from scheduler import Scheduler


@pytest.fixture
def clear():
    Scheduler._instances.clear()
    with open("scheduler.lock", "w") as file:
        pass
