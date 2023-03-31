import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, ClassVar, Self

import pytz

from .constants import JobType

logger = logging.getLogger(__name__)


class JobSoftReset(RuntimeError):
    """Job is reset because of internal error"""

    pass


class JobNotReady(RuntimeError):
    """Job is scheduled but it is not ready to start"""

    pass


@dataclass
class JobMomento:
    TYPE: ClassVar[JobType]
    start_at: datetime | None
    max_working_time: int
    tries: int
    dependencies: list[Self]


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

    @classmethod
    def from_momento(cls, momento: JobMomento):
        instance = cls(**momento.__dict__)
        return instance

    def create_momento(self, defaults: dict[str, Any]):
        """Controls job momento creation"""
        return JobMomento(**defaults)

    def serialize(self):
        return {"type": self._state.TYPE, "task_body": self._state.__dict__}

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
                "Function %s.%s completed in %.3f seconds",
                self.__class__.__name__,
                func.__name__,
                timed,
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
        except Exception as error:
            logger.exception(error)
        finally:
            self._save_state()

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
        self._save_state()
        self.is_finished = True

    def _save_state(self):
        """Prepare state type and freeze job state"""
        defaults = dict(
            start_at=self.start_at,
            max_working_time=self.max_working_time,
            tries=self.tries,
            dependencies=self.dependencies or None,
        )
        self._state = self.create_momento(defaults)
        logger.info(f"self._state: {self._state}")

    def soft_reset(self):
        self.coro = self.target()
        self.time_start = self.start_at or self.now()
        self.time_since_start = 0
        self.time_timeout = self.time_start + timedelta(
            seconds=self.max_working_time
        )
        self._save_state()
        self.is_finished = False

    def retry(self):
        if self.tries_left > 0:
            self.tries_left -= 1
            logger.info(
                "%s: restart. Tries left: %d",
                self.__class__.__name__,
                self.tries_left,
            )
            raise JobSoftReset()
        raise StopIteration()


@dataclass
class EmptyJobMomento(JobMomento):
    TYPE: ClassVar[JobType] = JobType.EMPTY


class EmptyJob(Job):
    """Empty Job which does nothing"""

    def create_momento(self, defaults: dict[str, Any]):
        return EmptyJobMomento(**defaults)

    def target(self):
        yield


@dataclass
class InfiniteJobMomento(JobMomento):
    TYPE: ClassVar[JobType] = JobType.INFINITE


class InfiniteJob(Job):
    """Empty Job which iterates infinitely"""

    def create_momento(self, defaults: dict[str, Any]):
        return InfiniteJobMomento(**defaults)

    def target(self):
        while True:
            yield
