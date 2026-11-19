from lake.dispatch.assignments import read_assignments
from lake.release.history import ReleaseNotFound, load_frozen_index
from lake.release.partitions import UnknownFrozenRange, select_frozen_rows
from lake.storage import BadInput


class InvalidJobRange(BadInput):
    pass


def resolve_job_range(db, release_id, bounds):
    try:
        return select_frozen_rows(load_frozen_index(db, release_id), bounds)
    except (ReleaseNotFound, UnknownFrozenRange) as exc:
        raise InvalidJobRange(str(exc)) from exc


def require_unassigned(db, target, keys):
    assigned = read_assignments(db, target, keys)
    if assigned:
        lines = ', '.join(str(key.line) for key in assigned)
        raise InvalidJobRange(f'rows already have jobs for {target}: {lines}')


def require_owned_range(db, target, keys, job_id):
    assigned = read_assignments(db, target, keys)
    unauthorized = [key.line for key in keys if key not in assigned or assigned[key].job_id != job_id]
    if unauthorized:
        lines = ', '.join(str(line) for line in unauthorized)
        raise InvalidJobRange(f'job {job_id} does not own lines {lines}')
