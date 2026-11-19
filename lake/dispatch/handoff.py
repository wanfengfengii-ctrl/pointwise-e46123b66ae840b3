from dataclasses import dataclass

from lake.dispatch.assignments import move_rows
from lake.dispatch.jobs import JobNotFound, ResendJob, load_job, register_job
from lake.dispatch.ranges import InvalidJobRange, require_owned_range, resolve_job_range
from lake.release.partitions import FrozenRowKey
from lake.storage import BadInput, LakeError, identifier, write_transaction


class HandoffRejected(LakeError):
    pass


@dataclass(frozen=True)
class HandoffResult:
    from_job: str
    to_job: ResendJob
    rows: tuple[FrozenRowKey, ...]


def handoff(center, from_job, to_job, bounds):
    try:
        identifier(from_job, 'from_job')
        identifier(to_job, 'to_job')
    except BadInput as exc:
        raise HandoffRejected(str(exc)) from exc
    if from_job == to_job:
        raise HandoffRejected('source and destination jobs must differ')
    with write_transaction(center):
        source = load_job(center, from_job)
        try:
            keys = tuple(row.key for row in resolve_job_range(center, source.release_id, bounds))
            require_owned_range(center, source.target, keys, source.job_id)
        except InvalidJobRange as exc:
            raise HandoffRejected(str(exc)) from exc
        try:
            destination = load_job(center, to_job)
        except JobNotFound:
            destination = ResendJob(to_job, source.target, source.release_id)
            register_job(center, destination)
        if (source.target, source.release_id) != (destination.target, destination.release_id):
            raise HandoffRejected('jobs must share the same target and release')
        move_rows(center, source.target, keys, source.job_id, destination.job_id)
    return HandoffResult(source.job_id, destination, keys)
