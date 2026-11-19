from dataclasses import dataclass
import json

from lake.delivery.outcomes import AttemptStatus, OutcomeConflict, ReceiverOutcome, parse_status
from lake.delivery.receipts import Receipt
from lake.release.partitions import FrozenRow, frozen_row_fields, frozen_row_from_fields
from lake.storage import BadInput, Conflict, NotFound, identifier, write_transaction


@dataclass(frozen=True)
class ReceivedRow:
    row: FrozenRow
    receipt: Receipt


def initialize_receiver(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS receiver_attempts (
            target TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            status TEXT NOT NULL CHECK (status IN ('pending', 'received', 'not_received')),
            lookup_enabled INTEGER NOT NULL DEFAULT 1 CHECK (lookup_enabled IN (0, 1)),
            PRIMARY KEY (target, attempt_id)
        );
        CREATE TABLE IF NOT EXISTS arrivals (
            receipt_id INTEGER PRIMARY KEY,
            target TEXT NOT NULL,
            attempt_id TEXT NOT NULL,
            UNIQUE (target, attempt_id),
            FOREIGN KEY (target, attempt_id) REFERENCES receiver_attempts
        );
        CREATE INDEX IF NOT EXISTS arrivals_by_target ON arrivals (target, receipt_id);
    ''')


def _attempt_record(db, target, attempt_id):
    return db.execute('''
        SELECT payload, status, lookup_enabled FROM receiver_attempts
        WHERE target = ? AND attempt_id = ?
    ''', (target, attempt_id)).fetchone()


def _known_outcome(db, target, attempt_id, row, status):
    status = AttemptStatus(status)
    if status != AttemptStatus.RECEIVED:
        return ReceiverOutcome(status)
    receipt_id, = db.execute('''
        SELECT receipt_id FROM arrivals WHERE target = ? AND attempt_id = ?
    ''', (target, attempt_id)).fetchone()
    return ReceiverOutcome(status, Receipt(receipt_id, target, attempt_id, row.key))


def _register_or_match(db, target, attempt_id, row):
    record = _attempt_record(db, target, attempt_id)
    payload = json.dumps(frozen_row_fields(row), ensure_ascii=False)
    if record is None:
        db.execute('''
            INSERT INTO receiver_attempts (target, attempt_id, payload, status) VALUES (?, ?, ?, 'pending')
        ''', (target, attempt_id, payload))
        return ReceiverOutcome(AttemptStatus.PENDING), True
    old_payload, status, _ = record
    if old_payload != payload:
        raise Conflict(f'attempt {attempt_id} already refers to another receiver payload')
    return _known_outcome(db, target, attempt_id, row, status), False


def _settle(db, target, attempt_id, row, current, status):
    if current.status == status or status == AttemptStatus.PENDING:
        return current
    if current.status != AttemptStatus.PENDING:
        raise OutcomeConflict(f'attempt {attempt_id} already ended as {current.status}')
    receipt = None
    if status == AttemptStatus.RECEIVED:
        receipt_id = db.execute('INSERT INTO arrivals (target, attempt_id) VALUES (?, ?)', (
            target, attempt_id,
        )).lastrowid
        receipt = Receipt(receipt_id, target, attempt_id, row.key)
    db.execute('''
        UPDATE receiver_attempts SET status = ? WHERE target = ? AND attempt_id = ?
    ''', (status.value, target, attempt_id))
    return ReceiverOutcome(status, receipt)


def submit_attempt(receiver, target, attempt_id, row, *, event=AttemptStatus.RECEIVED, hide_reply=False):
    identifier(target, 'target')
    identifier(attempt_id, 'attempt_id')
    status = parse_status(event)
    if type(hide_reply) is not bool:
        raise BadInput('hide_reply must be a boolean')
    with write_transaction(receiver):
        outcome, fresh = _register_or_match(receiver, target, attempt_id, row)
        if fresh:
            outcome = _settle(receiver, target, attempt_id, row, outcome, status)
    return ReceiverOutcome(AttemptStatus.PENDING) if hide_reply else outcome


def record_receiver_outcome(receiver, target, attempt_id, row, status):
    identifier(target, 'target')
    identifier(attempt_id, 'attempt_id')
    status = parse_status(status)
    if status == AttemptStatus.PENDING:
        raise BadInput('receiver event must declare a terminal outcome')
    with write_transaction(receiver):
        current, fresh = _register_or_match(receiver, target, attempt_id, row)
        if fresh and status == AttemptStatus.RECEIVED:
            raise NotFound(f'attempt {attempt_id} was never submitted to {target}')
        return _settle(receiver, target, attempt_id, row, current, status)


def query_attempt(receiver, target, attempt_id):
    identifier(target, 'target')
    identifier(attempt_id, 'attempt_id')
    record = _attempt_record(receiver, target, attempt_id)
    if record is None:
        return ReceiverOutcome(AttemptStatus.PENDING)
    payload, status, lookup_enabled = record
    if not lookup_enabled:
        return ReceiverOutcome(AttemptStatus.PENDING, lookup_available=False)
    row = frozen_row_from_fields(*json.loads(payload))
    return _known_outcome(receiver, target, attempt_id, row, status)


def set_lookup_available(receiver, target, attempt_id, available):
    identifier(target, 'target')
    identifier(attempt_id, 'attempt_id')
    if type(available) is not bool:
        raise BadInput('available must be a boolean')
    with write_transaction(receiver):
        result = receiver.execute('''
            UPDATE receiver_attempts SET lookup_enabled = ? WHERE target = ? AND attempt_id = ?
        ''', (int(available), target, attempt_id))
        if result.rowcount != 1:
            raise NotFound(f'attempt {attempt_id} does not exist at {target}')
    return available


def read_receiver_log(receiver, target):
    identifier(target, 'target')
    records = receiver.execute('''
        SELECT arrivals.receipt_id, arrivals.attempt_id, receiver_attempts.payload
        FROM arrivals JOIN receiver_attempts USING (target, attempt_id)
        WHERE arrivals.target = ? ORDER BY arrivals.receipt_id
    ''', (target,))
    received = []
    for receipt_id, attempt_id, payload in records:
        row = frozen_row_from_fields(*json.loads(payload))
        received.append(ReceivedRow(row, Receipt(receipt_id, target, attempt_id, row.key)))
    return tuple(received)
