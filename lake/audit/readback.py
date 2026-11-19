from lake.audit.continuation import ContinuationNotFound as ContinuationNotFound
from lake.audit.continuation import read_signed_continuation
from lake.audit.coverage import CoveredRow
from lake.audit.statement import Statement
from lake.delivery.receipts import Receipt
from lake.release.balances import ReadingBalance
from lake.release.partitions import frozen_row_from_fields
from lake.storage import NotFound, decode_reading, identifier, positive_number


class StatementNotFound(NotFound):
    pass


def read_continuation(center, continuation_id):
    identifier(continuation_id, 'continuation_id')
    return read_signed_continuation(center, continuation_id)


def read_statement(center, statement_id):
    positive_number(statement_id, 'statement_id')
    header = center.execute('''
        SELECT target, release_id, row_count, missing_count, total FROM statements WHERE statement_id = ?
    ''', (statement_id,)).fetchone()
    if header is None:
        raise StatementNotFound(f'statement not found: {statement_id}')
    target, release_id, row_count, missing_count, total = header
    records = center.execute('''
        SELECT release_id, line, partition, station, day, revision, position, value, attempt_id, receipt_id
        FROM statement_rows WHERE statement_id = ? ORDER BY ordinal
    ''', (statement_id,))
    rows = []
    for *fields, attempt_id, receipt_id in records:
        row = frozen_row_from_fields(*fields)
        rows.append(CoveredRow(row, Receipt(receipt_id, target, attempt_id, row.key)))
    return Statement(statement_id, target, release_id, tuple(rows), ReadingBalance(
        row_count, missing_count, decode_reading(total),
    ))
