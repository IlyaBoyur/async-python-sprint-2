import pickle
import json
import logging
import time
from threading import Lock, current_thread, Thread
from jobs import Job


logger = logging.Logger(__name__)


ITER_SECS = 0.5


class SingletonMeta(type):
    _instances = {}
    
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Scheduler(metaclass=SingletonMeta):
    def __init__(self, *, pool_size:int=10, lockfile:str="scheduler.lock"):
        self.tasks_active = []
        self.pool_size = pool_size
        self.tasks_wait = []
        self.lockfile = lockfile
        self.lock = Lock()
        self.event_loop_started = False


    def schedule(self, task: Job):
        self._stop_event_loop()
        if len(self.tasks_active) + len(task.dependencies) < self.pool_size:
            self.tasks_active.extend(task.dependencies)
            self.tasks_active.append(task)
        else:
            self.tasks_wait.extend(task.dependencies)
            self.tasks_wait.append(task)
        self._start_event_loop()

    def run(self):
        self._start_event_loop()

    def restart(self):
        # read task states
        with open(self.lockfile, "r") as file:
            # waiting, active = pickle.load(file)
            data = json.load(file)
        active = data.get("active", [])
        waiting =  data.get("waiting", [])
        # stop event loop
        self._stop_event_loop()
        # restore waiting tasks
        self.tasks_wait = [Job(**state) for state in waiting]
        # restore active tasks
        self.tasks_active = [Job(**state) for state in active[:self.pool_size]]
        # start event loop
        self._start_event_loop()

    def pause(self):
        self._stop_event_loop()

    def stop(self):
        # stop event loop
        self._stop_event_loop()
        # stop tasks
        for task in self.tasks_active:
            task.stop()
        # save waiting tasks state
        waiting = [task.state for task in self.tasks_wait]
        # save tasks state
        active = [task.state for task in self.tasks_active]
        # write task states
        data = {"active": active, "waiting": waiting}
        with open(self.lockfile, "w") as file:
            # pickle.dump(waiting, file)
            # pickle.dump(active, file)
            json.dump(data, file)

    def _start_event_loop(self):
        if not self.event_loop_started:
            thread = Thread(target=self._event_loop, daemon=True)
            thread.start()
            self.event_loop_started = True
        if self.lock.locked():
            self.lock.release()

    def _stop_event_loop(self):
        if not self.lock.locked():
            self.lock.acquire()

    def _event_loop(self):
        logger.info(f"event loop: started")
        current = 0
        while True:
            with self.lock:
                if len(self.tasks_active) == 0:
                    self.lock.release()
                    time.sleep(ITER_SECS)
                    self.lock.acquire()
                    continue
                # 1) run iteration in a job
                job = self.tasks_active[current]
                if not job.is_finished:
                    logger.info(f"event loop: iteration for job {job} started")
                    self._process_job(job)
                    logger.info(f"event loop: iteration for job {job} finished")
                else:
                # 2) if job is done - remove job and add job from wait list
                    logger.info(f"event loop: job {job} finished")
                    self.tasks_active.pop(current)
                    if len(self.tasks_active) < self.pool_size and len(self.tasks_wait) > 0:
                        self.tasks_active.append(self.tasks_wait.pop())
                # extend cursor
                active = len(self.tasks_active)
                scurrent = (current + 1) % active if active > 0 else 0


    @staticmethod
    def _process_job(job: Job):
        job.run()

    def join(self):
        while True:
            with self.lock:
                if len(self.tasks_active) + len(self.tasks_wait) == 0:
                    break
            time.sleep(ITER_SECS)

