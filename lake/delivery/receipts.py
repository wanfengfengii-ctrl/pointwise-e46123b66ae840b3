from dataclasses import dataclass

from lake.release.partitions import FrozenRowKey
from lake.storage import Conflict


@dataclass(frozen=True)
class Receipt:
    receipt_id: int
    target: str
    attempt_id: str
    key: FrozenRowKey


def initialize_receipts(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS receipts (
            target TEXT NOT NULL,
            release_id INTEGER NOT NULL,
            line INTEGER NOT NULL,
            attempt_id TEXT NOT NULL UNIQUE REFERENCES attempts (attempt_id),
            receipt_id INTEGER NOT NULL UNIQUE,
            PRIMARY KEY (target, release_id, line),
            FOREIGN KEY (release_id, line) REFERENCES frozen_rows
        );
    ''')


def save_receipt(db, receipt):
    db.execute('''
        INSERT INTO receipts (target, release_id, line, attempt_id, receipt_id) VALUES (?, ?, ?, ?, ?)
        ON CONFLICT (target, release_id, line) DO NOTHING
    ''', (receipt.target, receipt.key.release_id, receipt.key.line, receipt.attempt_id, receipt.receipt_id))
    if find_receipt(db, receipt.target, receipt.key) != receipt:
        raise Conflict(f'line {receipt.key.line} has a different receipt for {receipt.target}')


def find_receipt(db, target, key):
    record = db.execute('''
        SELECT receipt_id, attempt_id FROM receipts
        WHERE target = ? AND release_id = ? AND line = ?
    ''', (target, key.release_id, key.line)).fetchone()
    if record is None:
        return None
    receipt_id, attempt_id = record
    return Receipt(receipt_id, target, attempt_id, key)


def read_receipts(db, target, keys):
    receipts = {}
    for key in keys:
        receipt = find_receipt(db, target, key)
        if receipt is not None:
            receipts[key] = receipt
    return receipts
