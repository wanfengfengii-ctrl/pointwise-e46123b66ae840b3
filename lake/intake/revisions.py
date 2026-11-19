from lake.intake.rows import ObservationRow, SourceRowRef
from lake.storage import Conflict, decode_reading, encode_reading


class RevisionConflict(Conflict):
    pass


def initialize_revisions(db):
    db.executescript('''
        CREATE TABLE IF NOT EXISTS revisions (
            station TEXT NOT NULL,
            day TEXT NOT NULL,
            revision TEXT NOT NULL,
            PRIMARY KEY (station, day, revision)
        );
        CREATE TABLE IF NOT EXISTS observations (
            station TEXT NOT NULL,
            day TEXT NOT NULL,
            revision TEXT NOT NULL,
            position INTEGER NOT NULL CHECK (position > 0),
            value TEXT,
            PRIMARY KEY (station, day, revision, position),
            FOREIGN KEY (station, day, revision) REFERENCES revisions
        );
    ''')


def save_revision(db, key, rows):
    identity = (key.station, key.day, key.revision)
    inserted = db.execute('''
        INSERT INTO revisions VALUES (?, ?, ?)
        ON CONFLICT (station, day, revision) DO NOTHING
    ''', identity)
    if inserted.rowcount != 1:
        raise RevisionConflict(f'batch revision already exists: {key}')
    db.executemany(
        'INSERT INTO observations VALUES (?, ?, ?, ?, ?)',
        ((*identity, row.source.position, encode_reading(row.value)) for row in rows),
    )


def load_revision(db, key):
    identity = (key.station, key.day, key.revision)
    exists = db.execute('''
        SELECT 1 FROM revisions WHERE station = ? AND day = ? AND revision = ?
    ''', identity).fetchone()
    if exists is None:
        return None
    records = db.execute('''
        SELECT position, value FROM observations
        WHERE station = ? AND day = ? AND revision = ? ORDER BY position
    ''', identity)
    return tuple(
        ObservationRow(SourceRowRef(key, position), decode_reading(value))
        for position, value in records
    )
