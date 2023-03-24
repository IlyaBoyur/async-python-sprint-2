from datetime import datetime, timedelta
import pytz
from typing import List, Optional
import logging
import time


logger = logging.getLogger(__name__)


class JobSoftReset(RuntimeError):
    pass


class JobNotReady(RuntimeError):
    pass


class Job:
    def __init__(self,
                 *,
                 start_at: Optional[datetime]=None,
                 max_working_time: int=-1,
                 tries: int=0,
                 dependencies: List[Job]=None,
                 **kwargs):
        
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies
        self.kwargs = kwargs
        # Prepare to run
        self.tries_left = self.tries
        self.soft_reset()

    @staticmethod
    def now():
        return pytz.timezone("Europe/Moscow").localize(datetime.now())

    @staticmethod
    def check_start_ready(func):
        def inner(self, *args, **kwargs):
            if self.now() < self.time_start or not all(job.is_finished for job in self.dependencies):
                raise JobNotReady()
            func(*args, **kwargs)
        return inner

    @staticmethod
    def check_timeout(func):
        def inner(self, *args, **kwargs):
            if (
                self.max_working_time > 0 
                and (self.time_start + timedelta(seconds=self.time_since_start) > self.time_timeout)
            ):
                logger.info("Превышено допустимое время выполнения")
                if self.tries_left > 0:
                    self.tries_left -= 1
                    raise JobSoftReset()
                else:
                    raise StopIteration()
            func(*args, **kwargs)
        return inner

    @staticmethod
    def timeit(func):
        def inner(self, *args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            timed = time.time() - start
            logger.debug(f"Функция {func.__name__} выполнилась за {timed} секунд")
            self.time_since_start += timed
            return result
        return inner

    @timeit
    def run(self):
        try:
            if not self.is_finished:
                self.coro.send(None)
        except JobSoftReset():
            self.soft_reset()
        except StopIteration():
            self.is_finished = True
        finally:
            self.save_state()

    @check_start_ready
    @check_timeout
    def target(self):
        raise NotImplementedError("Метод Job.target() должен выполнять логику задачи")

    def stop(self):
        self.save_state()
        self.is_finished = True

    def save_state(self):
        self.state = dict(
            start_at=self.start_at ,
            max_working_time=self.max_working_time ,
            tries=self.tries,
            dependencies=self.dependencies,
            **self.kwargs,
        )
    
    def soft_reset(self):
        self.coro = self.target()
        self.time_start = self.start_at or self.now()
        self.time_since_start = 0
        self.time_timeout = self.time_start + timedelta(seconds=self.max_working_time)
        self.state = None
        self.is_finished = False


