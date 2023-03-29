import json

import pytest

from jobs import InfiniteJob
from scheduler import Scheduler


class TestSchedulerCommon:
    @pytest.fixture
    def prepare_lock(self):
        active_jobs = [InfiniteJob() for _ in range(4)]
        scheduler = Scheduler()
        [scheduler.schedule(job) for job in active_jobs]
        scheduler.stop()
        return scheduler, active_jobs

    def test_task_size(self, clear):
        jobs = [InfiniteJob() for _ in range(11)]
        scheduler = Scheduler()
        [scheduler.schedule(job) for job in jobs]

        assert len(scheduler.tasks_active) == 10
        assert len(scheduler.tasks_wait) == 1

    def test_stop(self, clear):
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

    def test_restart(self, clear, prepare_lock):
        scheduler, active_jobs = prepare_lock
        active_jobs_count = len(active_jobs)

        scheduler.restart()
        scheduler.pause()

        assert len(scheduler.tasks_active) == active_jobs_count
        assert len(scheduler.tasks_wait) == 0

    def test_singleton(self, clear):
        scheduler = Scheduler()
        scheduler_other = Scheduler()

        assert id(scheduler) == id(scheduler_other)

    def test_max_working_time(self, clear):
        jobs = [InfiniteJob(max_working_time=1) for _ in range(2)]
        scheduler = Scheduler(pool_size=len(jobs))
        scheduler.run()
        [scheduler.schedule(job) for job in jobs]

        scheduler.join()
        assert len(scheduler.tasks_active) == 0
