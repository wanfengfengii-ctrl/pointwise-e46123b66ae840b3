from dataclasses import dataclass

from lake.release.balances import ReadingBalance
from lake.release.partitions import FrozenRow, frozen_row_fields, frozen_row_from_fields
from lake.storage import NotFound, decode_reading, encode_reading, identifier, positive_number


class ReleaseNotFound(NotFound):
    pass


@dataclass(frozen=True)
class FrozenPartition:
    release_id: int
    partition: str
    rows: tuple[FrozenRow, ...]
    balance: ReadingBalance


def initialize_releases(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS releases (
            release_id INTEGER PRIMARY KEY
        );
        CREATE TABLE IF NOT EXISTS partitions (
            release_id INTEGER NOT NULL REFERENCES releases,
            name TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            row_count INTEGER NOT NULL,
            missing_count INTEGER NOT NULL,
            total TEXT NOT NULL,
            PRIMARY KEY (release_id, name),
            UNIQUE (release_id, ordinal)
        );
        CREATE TABLE IF NOT EXISTS frozen_rows (
            release_id INTEGER NOT NULL,
            line INTEGER NOT NULL CHECK (line > 0),
            partition TEXT NOT NULL,
            station TEXT NOT NULL,
            day TEXT NOT NULL,
            revision TEXT NOT NULL,
            position INTEGER NOT NULL,
            value TEXT,
            PRIMARY KEY (release_id, line),
            FOREIGN KEY (release_id, partition) REFERENCES partitions
        );
        CREATE INDEX IF NOT EXISTS partition_lines ON frozen_rows (release_id, partition, line);
    ''')


def allocate_release_number(db):
    return db.execute('INSERT INTO releases DEFAULT VALUES').lastrowid


def save_release(db, release_id, partitions):
    for ordinal, partition in enumerate(partitions, start=1):
        balance = partition.balance
        db.execute('INSERT INTO partitions VALUES (?, ?, ?, ?, ?, ?)', (
            release_id, partition.partition, ordinal,
            balance.row_count, balance.missing_count, encode_reading(balance.total),
        ))
        db.executemany(
            'INSERT INTO frozen_rows VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
            (frozen_row_fields(row) for row in partition.rows),
        )


def load_frozen_index(db, release_id):
    positive_number(release_id, 'release_id')
    if db.execute('SELECT 1 FROM releases WHERE release_id = ?', (release_id,)).fetchone() is None:
        raise ReleaseNotFound(f'release {release_id} does not exist')
    records = db.execute('''
        SELECT release_id, line, partition, station, day, revision, position, value
        FROM frozen_rows WHERE release_id = ? ORDER BY line
    ''', (release_id,))
    return tuple(frozen_row_from_fields(*record) for record in records)


def read_release(center, release_id, partition):
    positive_number(release_id, 'release_id')
    identifier(partition, 'partition')
    balance = center.execute('''
        SELECT row_count, missing_count, total FROM partitions
        WHERE release_id = ? AND name = ?
    ''', (release_id, partition)).fetchone()
    if balance is None:
        raise ReleaseNotFound(f'partition {partition!r} does not exist in release {release_id}')
    records = center.execute('''
        SELECT release_id, line, partition, station, day, revision, position, value
        FROM frozen_rows WHERE release_id = ? AND partition = ? ORDER BY line
    ''', (release_id, partition))
    row_count, missing_count, total = balance
    return FrozenPartition(
        release_id, partition, tuple(frozen_row_from_fields(*record) for record in records),
        ReadingBalance(row_count, missing_count, decode_reading(total)),
    )
