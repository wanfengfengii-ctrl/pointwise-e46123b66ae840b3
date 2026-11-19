from dataclasses import dataclass

from lake.dispatch.assignments import claim_rows
from lake.dispatch.ranges import require_unassigned, resolve_job_range
from lake.storage import Conflict, NotFound, identifier, write_transaction


class JobConflict(Conflict):
    pass


class JobNotFound(NotFound):
    pass


@dataclass(frozen=True)
class ResendJob:
    job_id: str
    target: str
    release_id: int


def initialize_jobs(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY NOT NULL,
            target TEXT NOT NULL,
            release_id INTEGER NOT NULL REFERENCES releases,
            UNIQUE (job_id, target, release_id)
        );
    ''')


def register_job(db, job):
    result = db.execute('''
        INSERT INTO jobs VALUES (?, ?, ?) ON CONFLICT (job_id) DO NOTHING
    ''', (job.job_id, job.target, job.release_id))
    if result.rowcount != 1:
        raise JobConflict(f'job already exists: {job.job_id}')


def load_job(db, job_id):
    identifier(job_id, 'job_id')
    record = db.execute('SELECT job_id, target, release_id FROM jobs WHERE job_id = ?', (job_id,)).fetchone()
    if record is None:
        raise JobNotFound(f'job not found: {job_id}')
    return ResendJob(*record)


def open_job(center, job_id, target, release_id, bounds):
    job = ResendJob(identifier(job_id, 'job_id'), identifier(target, 'target'), release_id)
    with write_transaction(center):
        keys = tuple(row.key for row in resolve_job_range(center, release_id, bounds))
        require_unassigned(center, job.target, keys)
        register_job(center, job)
        claim_rows(center, job.target, keys, job.job_id)
    return job
