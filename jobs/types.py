from . import EmptyJob, FileJob, InfiniteJob, SystemJob, WebJob

JOB_TYPES = {
    "file_job": FileJob,
    "system_job": SystemJob,
    "web_job": WebJob,
    "infinite_job": InfiniteJob,
    "empty_job": EmptyJob,
}
