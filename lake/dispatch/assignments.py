from dataclasses import dataclass

from lake.release.partitions import FrozenRowKey
from lake.storage import Conflict


@dataclass(frozen=True)
class Assignment:
    target: str
    key: FrozenRowKey
    job_id: str
    active_attempt: str | None


def initialize_assignments(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS assignments (
            target TEXT NOT NULL,
            release_id INTEGER NOT NULL,
            line INTEGER NOT NULL,
            job_id TEXT NOT NULL,
            active_attempt TEXT UNIQUE REFERENCES attempts (attempt_id),
            PRIMARY KEY (target, release_id, line),
            FOREIGN KEY (release_id, line) REFERENCES frozen_rows,
            FOREIGN KEY (job_id, target, release_id) REFERENCES jobs (job_id, target, release_id)
        );
        CREATE INDEX IF NOT EXISTS assignments_by_job ON assignments (job_id, line);
    ''')


def claim_rows(db, target, keys, job_id):
    db.executemany('''
        INSERT INTO assignments (target, release_id, line, job_id) VALUES (?, ?, ?, ?)
    ''', ((target, key.release_id, key.line, job_id) for key in keys))


def read_assignments(db, target, keys):
    assignments = {}
    for key in keys:
        record = db.execute('''
            SELECT job_id, active_attempt FROM assignments
            WHERE target = ? AND release_id = ? AND line = ?
        ''', (target, key.release_id, key.line)).fetchone()
        if record is not None:
            assignments[key] = Assignment(target, key, *record)
    return assignments


def assignments_for_job(db, job_id):
    records = db.execute('''
        SELECT target, release_id, line, active_attempt FROM assignments
        WHERE job_id = ? ORDER BY line
    ''', (job_id,))
    return tuple(
        Assignment(target, FrozenRowKey(release_id, line), job_id, active_attempt)
        for target, release_id, line, active_attempt in records
    )


def move_rows(db, target, keys, from_job, to_job):
    changed = db.executemany('''
        UPDATE assignments SET job_id = ?
        WHERE target = ? AND release_id = ? AND line = ? AND job_id = ?
    ''', ((to_job, target, key.release_id, key.line, from_job) for key in keys))
    if changed.rowcount != len(keys):
        raise Conflict('handoff ownership no longer matches the requested rows')


def reserve_attempt(db, assignment, attempt_id):
    result = db.execute('''
        UPDATE assignments SET active_attempt = ?
        WHERE target = ? AND release_id = ? AND line = ? AND job_id = ? AND active_attempt IS NULL
    ''', (attempt_id, assignment.target, assignment.key.release_id, assignment.key.line, assignment.job_id))
    if result.rowcount != 1:
        raise Conflict('failed to reserve the sending slot')


def release_attempt(db, target, key, attempt_id):
    result = db.execute('''
        UPDATE assignments SET active_attempt = NULL
        WHERE target = ? AND release_id = ? AND line = ? AND active_attempt = ?
    ''', (target, key.release_id, key.line, attempt_id))
    if result.rowcount != 1:
        raise Conflict('the original attempt does not hold this sending slot')
