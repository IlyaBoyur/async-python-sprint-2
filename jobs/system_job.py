import logging
import pathlib
from typing import Any

from .job import Job


class SystemAction:
    CREATE = 1
    DELETE = 2
    MOVE = 3
    CREATE_DIR = 4


logger = logging.getLogger(__name__)


class SystemJob(Job):
    def __init__(self, actions: list[list[Any]], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actions = actions

    def save_state(self):
        super().save_state()
        self.state["job_type"] = "system_job"

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
