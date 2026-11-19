from dataclasses import dataclass

from lake.intake.rows import BatchRevisionKey, SourceRowRef
from lake.storage import BadInput, decode_reading, encode_reading


@dataclass(frozen=True)
class FrozenRowKey:
    release_id: int
    line: int


@dataclass(frozen=True)
class FrozenRow:
    key: FrozenRowKey
    partition: str
    source: SourceRowRef
    value: int | None


class UnknownFrozenRange(BadInput):
    pass


def build_partitions(release_id, resolved_sources):
    partitions = []
    next_line = 1
    for name, source_rows in resolved_sources:
        rows = tuple(
            FrozenRow(FrozenRowKey(release_id, line), name, row.source, row.value)
            for line, row in enumerate(source_rows, start=next_line)
        )
        partitions.append((name, rows))
        next_line += len(rows)
    return tuple(partitions)


def select_frozen_rows(index, bounds):
    if not isinstance(bounds, (list, tuple)) or len(bounds) != 2:
        raise UnknownFrozenRange('range must contain inclusive first and last line numbers')
    first, last = bounds
    if type(first) is not int or type(last) is not int or not 1 <= first <= last <= len(index):
        raise UnknownFrozenRange(f'range is outside frozen lines 1 through {len(index)}')
    return tuple(index[first - 1:last])


def frozen_row_fields(row):
    source = row.source
    return (
        row.key.release_id, row.key.line, row.partition,
        source.batch.station, source.batch.day, source.batch.revision, source.position,
        encode_reading(row.value),
    )


def frozen_row_from_fields(release_id, line, partition, station, day, revision, position, value):
    return FrozenRow(
        FrozenRowKey(release_id, line), partition,
        SourceRowRef(BatchRevisionKey(station, day, revision), position),
        decode_reading(value),
    )
