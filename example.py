import json

from jobs import InfiniteJob
from scheduler import Scheduler


if __name__ == "__main__":
    active = 5
    waiting = 4
    jobs_active = [InfiniteJob() for _ in range(active)]
    jobs_waiting = [InfiniteJob() for _ in range(waiting)]
    scheduler = Scheduler(pool_size=active)
    scheduler.run()
    [scheduler.schedule(job) for job in [*jobs_active, *jobs_waiting]]
    scheduler.stop()

    with open("scheduler.lock", "r") as file:
        data = json.load(file)

    assert len(data["active"]) == len(jobs_active)
    assert len(data["waiting"]) == len(jobs_waiting)
