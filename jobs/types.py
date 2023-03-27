from . import FileJob, SystemJob, WebJob, EmptyJob, InfiniteJob

JOB_TYPES = {
    "file_job": FileJob,
    "system_job": SystemJob,
    "web_job": WebJob,
    "infinite_job": InfiniteJob,
    "empty_job": EmptyJob,
}