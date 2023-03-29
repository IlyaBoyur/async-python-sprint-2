import logging
import time
from datetime import datetime, timedelta
from functools import wraps
from typing import Self

import pytz

logger = logging.getLogger(__name__)


class JobSoftReset(RuntimeError):
    """Job is reset because of internal error"""

    pass


class JobNotReady(RuntimeError):
    """Job is scheduled but it is not ready to start"""

    pass


class Job:
    """Job prototype"""

    def __init__(
        self,
        *,
        start_at: datetime | None = None,
        max_working_time: int = -1,
        tries: int = 0,
        dependencies: list[Self] = None,
        **kwargs,
    ):
        self.start_at = start_at
        self.max_working_time = max_working_time
        self.tries = tries
        self.dependencies = dependencies or []
        self.kwargs = kwargs
        # Prepare to run
        self.tries_left = self.tries
        self.soft_reset()

    @staticmethod
    def now():
        return pytz.timezone("Europe/Moscow").localize(datetime.now())

    @staticmethod
    def check_start_ready(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            if self.now() < self.time_start or not all(
                job.is_finished for job in self.dependencies
            ):
                raise JobNotReady()
            func(self, *args, **kwargs)

        return inner

    @staticmethod
    def check_timeout(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            if self.max_working_time > 0 and (
                self.time_start + timedelta(seconds=self.time_since_start)
                > self.time_timeout
            ):
                logger.info("Execution time exceeded")
                self.retry()
            func(self, *args, **kwargs)

        return inner

    @staticmethod
    def timeit(func):
        @wraps(func)
        def inner(self, *args, **kwargs):
            start = time.time()
            result = func(self, *args, **kwargs)
            timed = time.time() - start
            logger.debug(
                f"Function {self.__class__}.{func.__name__}"
                f" completed in {timed} seconds"
            )
            self.time_since_start += timed
            return result

        return inner

    def run(self):
        try:
            if not self.is_finished:
                self._iter_job()
        except JobSoftReset:
            self.soft_reset()
        except StopIteration:
            self.is_finished = True
        finally:
            self.save_state()

    @timeit
    @check_start_ready
    @check_timeout
    def _iter_job(self):
        self.coro.send(None)

    def target(self):
        raise NotImplementedError(
            f"Method {self.__class__}.target()"
            f" should be implemented as a generator function"
        )

    def stop(self):
        self.save_state()
        self.is_finished = True

    def save_state(self):
        self.state = dict(
            start_at=self.start_at,
            max_working_time=self.max_working_time,
            tries=self.tries,
            dependencies=self.dependencies or None,
            **self.kwargs,
        )

    def soft_reset(self):
        self.coro = self.target()
        self.time_start = self.start_at or self.now()
        self.time_since_start = 0
        self.time_timeout = self.time_start + timedelta(
            seconds=self.max_working_time
        )
        self.state = None
        self.is_finished = False

    def retry(self):
        if self.tries_left > 0:
            self.tries_left -= 1
            logger.info(
                f"{self.__class__}: restart. Tries left: {self.tries_left}"
            )
            raise JobSoftReset()
        raise StopIteration()


class EmptyJob(Job):
    """Empty Job which does nothing"""

    def save_state(self):
        super().save_state()
        self.state["job_type"] = "empty_job"

    def target(self):
        yield


class InfiniteJob(Job):
    """Empty Job which iterates infinitely"""

    def save_state(self):
        super().save_state()
        self.state["job_type"] = "infinite_job"

    def target(self):
        while True:
            yield
