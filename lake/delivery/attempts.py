from dataclasses import dataclass
import sqlite3

from lake.delivery.outcomes import AttemptStatus, ReceiverOutcome, parse_status, resolve_outcome
from lake.delivery.receipts import Receipt, find_receipt, read_receipts, save_receipt
from lake.delivery.receiver import submit_attempt
from lake.dispatch.assignments import assignments_for_job, release_attempt, reserve_attempt
from lake.dispatch.eligibility import check_send_eligibility
from lake.dispatch.jobs import load_job
from lake.release.history import load_frozen_index
from lake.release.partitions import FrozenRowKey, select_frozen_rows
from lake.storage import BadInput, Conflict, NotFound, identifier, write_transaction


@dataclass(frozen=True)
class Attempt:
    attempt_id: str
    job_id: str
    target: str
    key: FrozenRowKey
    status: AttemptStatus


@dataclass(frozen=True)
class RowDelivery:
    key: FrozenRowKey
    status: str
    attempt_id: str | None
    receipt: Receipt | None


class AttemptNotFound(NotFound):
    pass


def initialize_attempts(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS attempts (
            attempt_id TEXT PRIMARY KEY NOT NULL,
            job_id TEXT NOT NULL,
            target TEXT NOT NULL,
            release_id INTEGER NOT NULL,
            line INTEGER NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'received', 'not_received')),
            FOREIGN KEY (job_id, target, release_id) REFERENCES jobs (job_id, target, release_id),
            FOREIGN KEY (release_id, line) REFERENCES frozen_rows
        );
    ''')


def load_attempt(db, attempt_id):
    identifier(attempt_id, 'attempt_id')
    record = db.execute('''
        SELECT job_id, target, release_id, line, status FROM attempts WHERE attempt_id = ?
    ''', (attempt_id,)).fetchone()
    if record is None:
        raise AttemptNotFound(f'attempt not found: {attempt_id}')
    job_id, target, release_id, line, status = record
    return Attempt(attempt_id, job_id, target, FrozenRowKey(release_id, line), AttemptStatus(status))


def attempt_outcome(db, attempt):
    receipt = None
    if attempt.status == AttemptStatus.RECEIVED:
        receipt = find_receipt(db, attempt.target, attempt.key)
        if receipt is None or receipt.attempt_id != attempt.attempt_id:
            raise Conflict(f'attempt {attempt.attempt_id} is missing its receipt')
    return ReceiverOutcome(attempt.status, receipt)


def record_attempt_outcome(db, attempt_id, observed):
    with write_transaction(db):
        attempt = load_attempt(db, attempt_id)
        receipt = observed.receipt
        if receipt is not None and (receipt.target, receipt.key, receipt.attempt_id) != (
            attempt.target, attempt.key, attempt.attempt_id,
        ):
            raise Conflict('receipt does not belong to the original attempt')
        current = attempt_outcome(db, attempt)
        outcome = resolve_outcome(current, observed)
        if outcome.status == current.status:
            return outcome
        if outcome.receipt is not None:
            save_receipt(db, outcome.receipt)
        release_attempt(db, attempt.target, attempt.key, attempt.attempt_id)
        db.execute('UPDATE attempts SET status = ? WHERE attempt_id = ?', (outcome.status.value, attempt_id))
        return outcome


def send(center, receiver, job_id, line, attempt_id, *, event=AttemptStatus.RECEIVED, hide_reply=False):
    identifier(attempt_id, 'attempt_id')
    event = parse_status(event)
    if type(hide_reply) is not bool:
        raise BadInput('hide_reply must be a boolean')
    with write_transaction(center):
        job = load_job(center, job_id)
        row, = select_frozen_rows(load_frozen_index(center, job.release_id), (line, line))
        try:
            original = load_attempt(center, attempt_id)
        except AttemptNotFound:
            original = None
        if original is not None:
            if (original.job_id, original.target, original.key) != (job.job_id, job.target, row.key):
                raise Conflict(f'attempt {attempt_id} already refers to another send')
            return attempt_outcome(center, original)
        assignment = check_send_eligibility(
            center, job, row.key, delivered=find_receipt(center, job.target, row.key) is not None,
        )
        center.execute('''
            INSERT INTO attempts VALUES (?, ?, ?, ?, ?, 'pending')
        ''', (attempt_id, job.job_id, job.target, row.key.release_id, row.key.line))
        reserve_attempt(center, assignment, attempt_id)
    try:
        observed = submit_attempt(receiver, job.target, attempt_id, row, event=event, hide_reply=hide_reply)
    except sqlite3.OperationalError:
        observed = ReceiverOutcome(AttemptStatus.PENDING, lookup_available=False)
    return record_attempt_outcome(center, attempt_id, observed)


def read_job_status(center, job_id):
    job = load_job(center, job_id)
    assignments = assignments_for_job(center, job.job_id)
    receipts = read_receipts(center, job.target, (assignment.key for assignment in assignments))
    rows = []
    for assignment in assignments:
        receipt = receipts.get(assignment.key)
        if receipt is not None:
            rows.append(RowDelivery(assignment.key, 'received', receipt.attempt_id, receipt))
        elif assignment.active_attempt is not None:
            rows.append(RowDelivery(assignment.key, 'pending', assignment.active_attempt, None))
        else:
            rows.append(RowDelivery(assignment.key, 'ready', None, None))
    return tuple(rows)
