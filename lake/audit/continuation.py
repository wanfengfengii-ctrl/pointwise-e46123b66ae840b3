"""Signed continuation of a frozen partition into another release.

A continuation binds one already signed V1 parent statement to the same-named
partition of another frozen release.  Rows whose source batch, in-batch
position and reading are unchanged keep the parent receipt; every other child
row needs an actual receipt held by the target for that child frozen row.

The target keeps its own confirmation in its database and consumes one of the
parent statement's continuation slots in the same local commit.  The center
only signs a continuation from such a confirmation.
"""
from dataclasses import asdict, dataclass
import json

from lake.delivery.receipts import Receipt
from lake.release.balances import ReadingBalance, summarize_readings
from lake.release.partitions import FrozenRow, FrozenRowKey, frozen_row_from_fields
from lake.storage import Conflict, LakeError, NotFound, positive_number, write_transaction


class ContinuationRejected(LakeError):
    """A request refers to a statement or partition it may not continue."""


class ParentStatementNotFound(NotFound):
    pass


class ContinuationNotFound(NotFound):
    pass


@dataclass(frozen=True)
class ContinuationRequest:
    request_id: str
    target: str
    parent_statement_id: int
    parent_release_id: int
    child_release_id: int
    partition: str


@dataclass(frozen=True)
class ContinuationRow:
    row: FrozenRow
    receipt: Receipt
    kind: str


@dataclass(frozen=True)
class RetiredRow:
    row: FrozenRow
    receipt: Receipt


@dataclass(frozen=True)
class Confirmation:
    confirmation_id: str
    target: str
    request_id: str
    parent_statement_id: int
    parent_release_id: int
    child_release_id: int
    partition: str
    rows: tuple[ContinuationRow, ...]
    retired_rows: tuple[RetiredRow, ...]
    balance: ReadingBalance


@dataclass(frozen=True)
class SignedParent:
    statement_id: int
    target: str
    release_id: int
    entries: tuple[tuple[FrozenRow, Receipt], ...]


def identity(request):
    return (request.target, request.parent_statement_id, request.child_release_id, request.partition)


def require_same_identity(actual, expected, label):
    if tuple(actual) != tuple(expected):
        raise Conflict(f'{label} is already bound to different continuation arguments')


def confirmation_dict(confirmation):
    return json.loads(json.dumps(asdict(confirmation), ensure_ascii=False))


def same_origin(parent_row, child_row):
    return (
        parent_row.source.batch == child_row.source.batch
        and parent_row.source.position == child_row.source.position
        and parent_row.value == child_row.value
    )


def index_arrivals(arrivals):
    index = {}
    for entry in arrivals:
        index.setdefault(entry.row.key, []).append(entry)
    return index


def build_plan(parent_entries, child_rows, arrivals):
    """Return confirmation rows, retired parent rows and missing child lines.

    Parent entries are the signed statement rows in their original order;
    child rows are the child partition rows in child frozen order.  Reuse is by
    unchanged origin, so a shifted line number does not prevent it.  New rows
    only accept actual arrivals of that exact child frozen row for the target.
    """
    parent_entries = tuple(parent_entries)
    child_rows = tuple(child_rows)
    arrivals = index_arrivals(arrivals)
    used = set()
    rows = []
    missing = []
    for child_row in child_rows:
        matched = None
        for position, (parent_row, _) in enumerate(parent_entries):
            if position not in used and same_origin(parent_row, child_row):
                matched = position
                break
        if matched is not None:
            used.add(matched)
            rows.append(ContinuationRow(child_row, parent_entries[matched][1], 'reused'))
            continue
        candidates = [
            entry for entry in arrivals.get(child_row.key, ()) if entry.row == child_row
        ]
        if candidates:
            chosen = min(candidates, key=lambda entry: entry.receipt.receipt_id)
            rows.append(ContinuationRow(child_row, chosen.receipt, 'received'))
        else:
            missing.append(child_row.key.line)
    if missing:
        return (), (), tuple(missing)
    retired = tuple(
        RetiredRow(entry[0], entry[1])
        for position, entry in enumerate(parent_entries)
        if position not in used
    )
    balance = summarize_readings(row.row.value for row in rows)
    return tuple(rows), retired, ()


def load_parent_statement(center, statement_id):
    positive_number(statement_id, 'parent_statement_id')
    header = center.execute(
        'SELECT target, release_id FROM statements WHERE statement_id = ?', (statement_id,),
    ).fetchone()
    if header is None:
        raise ParentStatementNotFound(f'statement not found: {statement_id}')
    target, release_id = header
    records = center.execute('''
        SELECT release_id, line, partition, station, day, revision, position, value,
               attempt_id, receipt_id
        FROM statement_rows WHERE statement_id = ? ORDER BY ordinal
    ''', (statement_id,))
    entries = []
    for *fields, attempt_id, receipt_id in records:
        row = frozen_row_from_fields(*fields)
        entries.append((row, Receipt(receipt_id, target, attempt_id, row.key)))
    return SignedParent(statement_id, target, release_id, tuple(entries))


def require_partition_coverage(parent, partition, parent_partition_rows):
    """Require the parent statement to be exactly the full same-named partition."""
    selected = [(row, receipt) for row, receipt in parent.entries if row.partition == partition]
    if len(selected) != len(parent.entries) or [row for row, _ in selected] != list(
        parent_partition_rows
    ):
        raise ContinuationRejected(
            f'parent statement {parent.statement_id} does not cover partition {partition!r} exactly'
        )
    return tuple(selected)


def initialize_center_continuations(center):
    center.executescript('''
        CREATE TABLE IF NOT EXISTS continuations (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            continuation_id TEXT UNIQUE,
            request_id TEXT NOT NULL UNIQUE,
            target TEXT NOT NULL,
            parent_statement_id INTEGER NOT NULL,
            parent_release_id INTEGER NOT NULL,
            child_release_id INTEGER NOT NULL,
            partition TEXT NOT NULL,
            confirmation_id TEXT NOT NULL,
            payload TEXT NOT NULL,
            UNIQUE (target, parent_statement_id)
        );
        CREATE INDEX IF NOT EXISTS continuations_by_parent
            ON continuations (target, parent_statement_id);
    ''')


def initialize_receiver_continuations(receiver):
    receiver.executescript('''
        CREATE TABLE IF NOT EXISTS continuation_confirmations (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            confirmation_id TEXT UNIQUE,
            request_id TEXT NOT NULL UNIQUE,
            target TEXT NOT NULL,
            parent_statement_id INTEGER NOT NULL,
            parent_release_id INTEGER NOT NULL,
            child_release_id INTEGER NOT NULL,
            partition TEXT NOT NULL,
            payload TEXT NOT NULL,
            UNIQUE (target, parent_statement_id)
        );
    ''')


def find_center_continuation(center, request_id):
    return center.execute('''
        SELECT continuation_id, target, parent_statement_id, child_release_id, partition
        FROM continuations WHERE request_id = ?
    ''', (request_id,)).fetchone()


def find_receiver_confirmation(receiver, request_id):
    record = receiver.execute('''
        SELECT confirmation_id, target, parent_statement_id, parent_release_id,
               child_release_id, partition, payload
        FROM continuation_confirmations WHERE request_id = ?
    ''', (request_id,)).fetchone()
    if record is None:
        return None
    confirmation_id, target, parent_statement_id, parent_release_id, child_release_id, partition, payload = record
    stored = json.loads(payload)
    return Confirmation(
        confirmation_id=confirmation_id,
        target=target,
        request_id=request_id,
        parent_statement_id=parent_statement_id,
        parent_release_id=parent_release_id,
        child_release_id=child_release_id,
        partition=partition,
        rows=tuple(
            ContinuationRow(
                _row_from_payload(item['row']),
                _receipt_from_payload(item['receipt']),
                item['kind'],
            )
            for item in stored['rows']
        ),
        retired_rows=tuple(
            RetiredRow(
                _row_from_payload(item['row']),
                _receipt_from_payload(item['receipt']),
            )
            for item in stored['retired_rows']
        ),
        balance=ReadingBalance(**stored['balance']),
    )


def _row_from_payload(payload):
    fields = (
        payload['key']['release_id'], payload['key']['line'], payload['partition'],
        payload['source']['batch']['station'], payload['source']['batch']['day'],
        payload['source']['batch']['revision'], payload['source']['position'],
        None if payload['value'] is None else format(payload['value'], 'x'),
    )
    return frozen_row_from_fields(*fields)


def _receipt_from_payload(payload):
    return Receipt(
        payload['receipt_id'], payload['target'], payload['attempt_id'],
        FrozenRowKey(payload['key']['release_id'], payload['key']['line']),
    )


def find_consumed_parent(receiver, target, parent_statement_id):
    record = receiver.execute('''
        SELECT request_id FROM continuation_confirmations
        WHERE target = ? AND parent_statement_id = ?
    ''', (target, parent_statement_id)).fetchone()
    return None if record is None else record[0]


def find_center_parent_continuation(center, target, parent_statement_id):
    record = center.execute('''
        SELECT request_id FROM continuations
        WHERE target = ? AND parent_statement_id = ?
    ''', (target, parent_statement_id)).fetchone()
    return None if record is None else record[0]


def sign_continuation(center, request, confirmation):
    """Persist the center continuation for an issued target confirmation."""
    with write_transaction(center):
        existing = find_center_continuation(center, request.request_id)
        if existing is not None:
            continuation_id, target, parent_statement_id, child_release_id, partition = existing
            require_same_identity(
                (target, parent_statement_id, child_release_id, partition),
                identity(request),
                f'request {request.request_id!r}',
            )
            return continuation_id
        consumed = find_center_parent_continuation(
            center, request.target, request.parent_statement_id,
        )
        if consumed is not None:
            raise Conflict(
                f'parent statement {request.parent_statement_id} is already continued '
                f'by request {consumed!r}'
            )
        seq = center.execute('''
            INSERT INTO continuations (
                continuation_id, request_id, target, parent_statement_id,
                parent_release_id, child_release_id, partition, confirmation_id, payload
            ) VALUES (NULL, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            request.request_id, request.target, request.parent_statement_id,
            request.parent_release_id, request.child_release_id, request.partition,
            confirmation.confirmation_id,
            json.dumps(asdict(confirmation), ensure_ascii=False),
        )).lastrowid
        continuation_id = f'continuation-{seq}'
        center.execute(
            'UPDATE continuations SET continuation_id = ? WHERE seq = ?',
            (continuation_id, seq),
        )
    return continuation_id


def confirm_continuation(receiver, request, rows, retired_rows, balance):
    """Commit one target-local confirmation and consume the parent statement."""
    with write_transaction(receiver):
        existing = find_receiver_confirmation(receiver, request.request_id)
        if existing is not None:
            require_same_identity(
                (existing.target, existing.parent_statement_id,
                 existing.child_release_id, existing.partition),
                identity(request),
                f'request {request.request_id!r}',
            )
            return existing
        consumed = find_consumed_parent(receiver, request.target, request.parent_statement_id)
        if consumed is not None:
            raise Conflict(
                f'parent statement {request.parent_statement_id} is already continued '
                f'by request {consumed!r}'
            )
        seq = receiver.execute('''
            INSERT INTO continuation_confirmations (
                confirmation_id, request_id, target, parent_statement_id,
                parent_release_id, child_release_id, partition, payload
            ) VALUES (NULL, ?, ?, ?, ?, ?, ?, '{}')
        ''', (
            request.request_id, request.target, request.parent_statement_id,
            request.parent_release_id, request.child_release_id, request.partition,
        )).lastrowid
        confirmation = Confirmation(
            f'confirmation-{seq}', request.target, request.request_id,
            request.parent_statement_id, request.parent_release_id,
            request.child_release_id, request.partition,
            tuple(rows), tuple(retired_rows), balance,
        )
        receiver.execute('''
            UPDATE continuation_confirmations SET confirmation_id = ?, payload = ?
            WHERE seq = ?
        ''', (confirmation.confirmation_id, json.dumps(asdict(confirmation), ensure_ascii=False), seq))
    return confirmation


def read_active_continuation(receiver, target, parent_statement_id):
    record = receiver.execute('''
        SELECT request_id FROM continuation_confirmations
        WHERE target = ? AND parent_statement_id = ?
    ''', (target, parent_statement_id)).fetchone()
    if record is None:
        return {'status': 'uncontinued'}
    confirmation = find_receiver_confirmation(receiver, record[0])
    return {'status': 'confirmed', 'confirmation': confirmation_dict(confirmation)}


def read_signed_continuation(center, continuation_id):
    record = center.execute(
        'SELECT payload FROM continuations WHERE continuation_id = ?', (continuation_id,),
    ).fetchone()
    if record is None:
        raise ContinuationNotFound(f'continuation not found: {continuation_id}')
    return {'continuation_id': continuation_id, 'confirmation': json.loads(record[0])}
