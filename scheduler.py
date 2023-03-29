import json
import logging
import time
from threading import Lock, Thread

from jobs import JOB_TYPES, Job

logger = logging.Logger(__name__)


ITER_SECS = 0.5


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super().__call__(*args, **kwargs)
        return cls._instances[cls]


class Scheduler(metaclass=SingletonMeta):
    def __init__(
        self, *, pool_size: int = 10, lockfile: str = "scheduler.lock"
    ):
        self.tasks_active = []
        self.pool_size = pool_size
        self.tasks_wait = []
        self.lockfile = lockfile
        self.lock = Lock()
        self.event_loop_started = False

    def schedule(self, task: Job):
        """Schedules task with its dependencies"""
        self.__stop_event_loop()
        if len(self.tasks_active) + len(task.dependencies) < self.pool_size:
            self.tasks_active.extend(task.dependencies)
            self.tasks_active.append(task)
        else:
            self.tasks_wait.extend(task.dependencies)
            self.tasks_wait.append(task)
        self.__start_event_loop()

    def run(self):
        self.__start_event_loop()

    def restart(self):
        """Restarts scheduler

        Reads saved task states
        Stops event loop
        Restores waiting, active tasks
        Starts event loop
        """
        with open(self.lockfile, "r") as file:
            data = json.load(file)
        active = data.get("active", [])
        waiting = data.get("waiting", [])
        self.__stop_event_loop()
        for state in [*waiting, *active[self.pool_size :]]:
            job_type = state.get("job_type")
            job_klass = JOB_TYPES.get(job_type, Job)
            self.tasks_wait.append(job_klass(**state))
        for state in active[: self.pool_size]:
            job_type = state.get("job_type")
            job_klass = JOB_TYPES.get(job_type, Job)
            self.tasks_active.append(job_klass(**state))
        self.__start_event_loop()

    def pause(self):
        self.__stop_event_loop()

    def stop(self):
        """Stops scheduler

        Stops event loop and tasks
        Saves waiting, active tasks states
        Dumps tasks states to filesystem
        Clears task queues
        """
        self.__stop_event_loop()
        for task in self.tasks_active:
            task.stop()
        waiting = [task.state for task in self.tasks_wait]
        active = [task.state for task in self.tasks_active]
        with open(self.lockfile, "w") as file:
            json.dump({"active": active, "waiting": waiting}, file)
        self.tasks_wait = []
        self.tasks_active = []

    def join(self):
        while True:
            with self.lock:
                if len(self.tasks_active) + len(self.tasks_wait) == 0:
                    break
            time.sleep(ITER_SECS)

    def __start_event_loop(self):
        if not self.event_loop_started:
            thread = Thread(target=self.__event_loop, daemon=True)
            thread.start()
            self.event_loop_started = True
        if self.lock.locked():
            self.lock.release()

    def __stop_event_loop(self):
        if not self.lock.locked():
            self.lock.acquire()

    def __event_loop(self):
        """Scheduler Event Loop

        Loops infinitely, assuming it is called as a daemon
        Runs one iteration at a time
        If job is done - remove job and add job from wait list
        Extends cursor pointing to the next task
        """
        logger.info(f"event loop: started")
        current = 0
        while True:
            with self.lock:
                if len(self.tasks_active) == 0:
                    self.lock.release()
                    time.sleep(ITER_SECS)
                    self.lock.acquire()
                    continue
                job = self.tasks_active[current]
                if not job.is_finished:
                    logger.info(f"event loop: job %s iteration started", job)
                    self.__process_job(job)
                    logger.info(f"event loop: job %s iteration finished", job)
                else:
                    logger.info(f"event loop: job %s finished", job)
                    self.tasks_active.pop(current)
                    if (
                        len(self.tasks_active) < self.pool_size
                        and len(self.tasks_wait) > 0
                    ):
                        self.tasks_active.append(self.tasks_wait.pop())
                active = len(self.tasks_active)
                current = (current + 1) % active if active > 0 else 0

    @staticmethod
    def __process_job(job: Job):
        job.run()
