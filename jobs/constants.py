from enum import Enum


class JobType(str, Enum):
    EMPTY = "empty"
    FILE = "file"
    INFINITE = "infinite"
    SYSTEM = "system"
    WEB = "web"
