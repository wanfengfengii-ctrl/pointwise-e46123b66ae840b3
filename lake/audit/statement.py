from dataclasses import dataclass

from lake.audit.coverage import CoveredRow, require_coverage
from lake.audit.totals import statement_totals
from lake.release.balances import ReadingBalance
from lake.release.history import load_frozen_index
from lake.release.partitions import frozen_row_fields, select_frozen_rows
from lake.storage import encode_reading, identifier, write_transaction


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
