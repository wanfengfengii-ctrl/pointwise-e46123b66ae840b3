from lake.dispatch.assignments import Assignment, read_assignments
from lake.dispatch.jobs import ResendJob
from lake.release.partitions import FrozenRowKey
from lake.storage import Conflict


class SendIneligible(Conflict):
    pass


def check_send_eligibility(db, job: ResendJob, key: FrozenRowKey, *, delivered: bool) -> Assignment:
    assignment = read_assignments(db, job.target, (key,)).get(key)
    if assignment is None or assignment.job_id != job.job_id:
        raise SendIneligible(f'job {job.job_id} does not own line {key.line}')
    if delivered:
        raise SendIneligible(f'line {key.line} already has a receipt')
    if assignment.active_attempt is not None:
        raise SendIneligible(f'line {key.line} is held by attempt {assignment.active_attempt}')
    return assignment
