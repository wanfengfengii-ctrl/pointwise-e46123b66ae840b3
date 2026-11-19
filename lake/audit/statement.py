from dataclasses import dataclass

from lake.audit.continuation import (
    ContinuationRejected, ContinuationRequest, build_plan, confirm_continuation,
    find_center_continuation, find_center_parent_continuation, find_consumed_parent,
    find_receiver_confirmation, load_parent_statement, require_partition_coverage,
    require_same_identity, sign_continuation,
)
from lake.audit.coverage import CoveredRow, require_coverage
from lake.audit.totals import statement_totals
from lake.delivery.receiver import read_receiver_log
from lake.release.balances import ReadingBalance
from lake.release.history import load_frozen_index, read_release
from lake.release.partitions import frozen_row_fields, select_frozen_rows
from lake.storage import (
    Conflict, encode_reading, identifier, positive_number, write_transaction,
)


@dataclass(frozen=True)
class Statement:
    statement_id: int
    target: str
    release_id: int
    rows: tuple[CoveredRow, ...]
    balance: ReadingBalance


def initialize_statements(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS statements (
            statement_id INTEGER PRIMARY KEY,
            target TEXT NOT NULL,
            release_id INTEGER NOT NULL REFERENCES releases,
            row_count INTEGER NOT NULL,
            missing_count INTEGER NOT NULL,
            total TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS statement_rows (
            statement_id INTEGER NOT NULL REFERENCES statements,
            ordinal INTEGER NOT NULL,
            release_id INTEGER NOT NULL,
            line INTEGER NOT NULL,
            partition TEXT NOT NULL,
            station TEXT NOT NULL,
            day TEXT NOT NULL,
            revision TEXT NOT NULL,
            position INTEGER NOT NULL,
            value TEXT,
            attempt_id TEXT NOT NULL,
            receipt_id INTEGER NOT NULL,
            PRIMARY KEY (statement_id, ordinal),
            UNIQUE (statement_id, line)
        );
    ''')


def issue_statement(center, target, release_id, bounds):
    identifier(target, 'target')
    with write_transaction(center):
        frozen = select_frozen_rows(load_frozen_index(center, release_id), bounds)
        covered = require_coverage(center, target, frozen)
        balance = statement_totals(covered)
        statement_id = center.execute('''
            INSERT INTO statements (target, release_id, row_count, missing_count, total)
            VALUES (?, ?, ?, ?, ?)
        ''', (target, release_id, balance.row_count, balance.missing_count, encode_reading(balance.total))).lastrowid
        center.executemany('''
            INSERT INTO statement_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            (statement_id, ordinal, *frozen_row_fields(entry.row), entry.receipt.attempt_id, entry.receipt.receipt_id)
            for ordinal, entry in enumerate(covered, start=1)
        ))
    return Statement(statement_id, target, release_id, covered, balance)


def issue_continuation(center, receiver, target, request_id, parent_statement_id,
                       child_release_id, partition):
    """Sign a continuation of one signed parent statement into another release."""
    identifier(target, 'target')
    identifier(request_id, 'request_id')
    identifier(partition, 'partition')
    positive_number(parent_statement_id, 'parent_statement_id')
    positive_number(child_release_id, 'child_release_id')
    identity = (target, parent_statement_id, child_release_id, partition)

    existing = find_center_continuation(center, request_id)
    if existing is not None:
        continuation_id, found_target, found_parent, found_child, found_partition = existing
        require_same_identity(
            (found_target, found_parent, found_child, found_partition), identity,
            f'request {request_id!r}',
        )
        return {'status': 'issued', 'continuation_id': continuation_id}

    confirmation = find_receiver_confirmation(receiver, request_id)
    if confirmation is not None:
        require_same_identity(
            (confirmation.target, confirmation.parent_statement_id,
             confirmation.child_release_id, confirmation.partition),
            identity, f'request {request_id!r}',
        )

    parent = load_parent_statement(center, parent_statement_id)
    if parent.target != target:
        raise ContinuationRejected(
            f'parent statement {parent_statement_id} belongs to {parent.target!r}, not {target!r}'
        )
    if parent.release_id == child_release_id:
        raise ContinuationRejected('the child release must be a different release than the parent')
    parent_rows = read_release(center, parent.release_id, partition).rows
    parent_entries = require_partition_coverage(parent, partition, parent_rows)
    request = ContinuationRequest(
        request_id, target, parent_statement_id, parent.release_id, child_release_id, partition,
    )

    if confirmation is None:
        consumed = find_consumed_parent(receiver, target, parent_statement_id)
        if consumed is None:
            consumed = find_center_parent_continuation(center, target, parent_statement_id)
        if consumed is not None:
            raise Conflict(
                f'parent statement {parent_statement_id} is already continued by request {consumed!r}'
            )
        child_rows = read_release(center, child_release_id, partition).rows
        arrivals = read_receiver_log(receiver, target)
        rows, retired_rows, missing = build_plan(parent_entries, child_rows, arrivals)
        if missing:
            return {'status': 'pending', 'missing_lines': list(missing)}
        confirmation = confirm_continuation(
            receiver, request, rows, retired_rows, statement_totals(rows),
        )

    continuation_id = sign_continuation(center, request, confirmation)
    return {'status': 'issued', 'continuation_id': continuation_id}
