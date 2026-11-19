from contextlib import contextmanager
import sqlite3


class LakeError(Exception):
    """A rejected business request."""


class BadInput(LakeError):
    pass


class Conflict(LakeError):
    pass


class NotFound(LakeError):
    pass


def open_database(path):
    db = sqlite3.connect(path, isolation_level=None, timeout=5)
    try:
        db.execute('PRAGMA foreign_keys = ON')
    except BaseException:
        db.close()
        raise
    return db


@contextmanager
def write_transaction(db):
    db.execute('BEGIN IMMEDIATE')
    try:
        yield
        db.commit()
    except BaseException:
        db.rollback()
        raise


def identifier(value, label):
    if not isinstance(value, str) or not value or value != value.strip() or '\x00' in value:
        raise BadInput(f'{label} must be a nonempty identifier without surrounding whitespace or NUL')
    return value


def positive_number(value, label):
    if type(value) is not int or not 1 <= value <= (1 << 63) - 1:
        raise BadInput(f'{label} must be a positive SQLite record number')
    return value


def encode_reading(value):
    return None if value is None else format(value, 'x')


def decode_reading(value):
    return None if value is None else int(value, 16)
