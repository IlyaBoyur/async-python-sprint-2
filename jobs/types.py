from . import EmptyJob, FileJob, InfiniteJob, SystemJob, WebJob
from .constants import JobType

JOB_TYPES = {
    JobType.EMPTY: EmptyJob,
    JobType.FILE: FileJob,
    JobType.INFINITE: InfiniteJob,
    JobType.SYSTEM: SystemJob,
    JobType.WEB: WebJob,
}
