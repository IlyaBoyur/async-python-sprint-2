import pickle
import logging
import time
from threading import Lock, current_thread, Thread
from job import Job


logger = logging.Logger(__name__)


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
        if len(tasks_active) + len(task.dependencies) < self.pool_size:
            self.tasks_active.append(task)
            self.tasks_active.extend(task.dependencies)
        else:
            self.tasks_wait.append(task)
        self._start_event_loop()

    def run(self):
        self._start_event_loop()

    def restart(self):
        # read task states
        with open(self.lockfile, "rb") as file:
            waiting, active = pickle.load(file)
        # stop event loop
        self._stop_event_loop()
        # restore waiting tasks
        self.tasks_wait = [Job(**state) for state in waiting]
        # restore active tasks
        self.tasks_active = [Job(**state) for state in active[:self.pool_size]]
        # start event loop
        self._start_event_loop()

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
        with open(self.lockfile, "wb") as file:
            pickle.dump(waiting, file)
            pickle.dump(active, file)

    def _start_event_loop():
        if not self.event_loop_started:
            thread = Thread(target=self._event_loop, daemon=True)
            thread.start()
            self.event_loop_started = True
        self.lock.release()

    def _stop_event_loop():
        if not self.lock.locked():
            self.lock.acquire()

    def _event_loop(self):
        logger.info(f"event loop: started")
        current = 0
        while True:
            with self.lock:
                if len(self.tasks_active) == 0:
                    self.lock.release()
                    time.sleep(0.5)
                    self.lock.acquire()
                    continue
                # 1) run iteration in a job
                    logger.info(f"event loop: iteration {current} started")
                if not job.is_finished:
                    self._process_job(self.pool[current])
                    logger.info(f"event loop: iteration {current} finished")
                else:
                # 2) if job is done - remove job and add job from wait list
                    logger.info(f"event loop: job {current} finished")
                    self.tasks_active.pop(current)
                    if len(self.tasks_wait) > 0:
                        self.tasks_active.append(self.tasks_wait.pop())
                # extend cursor
                current = (current + 1) % self.pool_size


    @staticmethod
    def _process_job(job: Job):
        job.run()
        
