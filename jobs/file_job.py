from .job import Job
from typing import List, Tuple, Any
import os
import logging


logger = logging.getLogger(__name__)


class FileJob(Job):
    def __init__(self, actions: List[Tuple[str, str, Any]], *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.actions = actions

    def target(self):
        for filemode, file, target in self.actions:
            try:
                if filemode == "w":
                    with open(file=file, mode=filemode) as file:
                        file.write(target)
                elif filemode == "r":
                    if not os.path.exists(file):
                        raise RuntimeError(f"Невозможно прочитать файл {file}: файл отсутствует")
                    with open(file=file, mode=filemode) as file:
                        target[0] = file.read()
                else:
                    logger.warning("Режим не поддерживается")
            except RuntimeError as error:
                logger.error(error)
            yield
            
