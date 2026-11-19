from datetime import date

from lake.intake.rows import BatchRevisionKey
from lake.storage import BadInput, identifier


class InvalidBatch(BadInput):
    pass


def validate_revision_key(station, day, revision):
    try:
        station = identifier(station, 'station')
        revision = identifier(revision, 'revision')
    except BadInput as exc:
        raise InvalidBatch(str(exc)) from exc
    try:
        parsed_day = date.fromisoformat(day)
    except (ValueError, TypeError) as exc:
        raise InvalidBatch('day must be a calendar date in YYYY-MM-DD format') from exc
    if parsed_day.isoformat() != day:
        raise InvalidBatch('day must use YYYY-MM-DD format')
    return BatchRevisionKey(station, day, revision)


def validate_batch_input(station, day, revision, values):
    key = validate_revision_key(station, day, revision)
    if not isinstance(values, (list, tuple)):
        raise InvalidBatch('values must be an ordered list or tuple')
    snapshot = tuple(values)
    for position, value in enumerate(snapshot, start=1):
        if value is not None and type(value) is not int:
            raise InvalidBatch(f'row {position} must be an integer or null')
    return key, snapshot
