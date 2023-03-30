import logging
import pathlib
from dataclasses import dataclass
from typing import Any, ClassVar

from .constants import JobType
from .job import Job, JobMomento


class SystemAction:
    CREATE = 1
    DELETE = 2
    MOVE = 3
    CREATE_DIR = 4


logger = logging.getLogger(__name__)


@dataclass
class SystemJobMomento(JobMomento):
    TYPE: ClassVar[JobType] = JobType.SYSTEM
    actions: list[list[Any]]


class SystemJob(Job):
    def __init__(self, actions: list[list[Any]], *args, **kwargs):
        self.actions = actions
        super().__init__(*args, **kwargs)

    def create_momento(self, defaults: dict[str, Any]):
        return SystemJobMomento(**defaults, actions=self.actions)

    def target(self):
        try:
            for action, *paths in self.actions:
                source, *target = paths
                if action == SystemAction.CREATE_DIR:
                    source = pathlib.Path(source)
                    source.mkdir(parents=True, exist_ok=True)
                elif action == SystemAction.CREATE:
                    *path, file = source.split("/")
                    path = pathlib.Path("/".join(path))
                    path.mkdir(parents=True, exist_ok=True)
                    path = path / file
                    path.touch()
                elif action == SystemAction.DELETE:
                    source = pathlib.Path(source)
                    source.unlink(missing_ok=True)
                elif action == SystemAction.MOVE:
                    if not target:
                        raise RuntimeError("No target path provided")
                    target, *_ = target
                    source = pathlib.Path(source)
                    source.rename(target)
                else:
                    pass
                yield
        except RuntimeError as error:
            logger.error(error)
