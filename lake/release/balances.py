from dataclasses import dataclass

from lake.release.partitions import FrozenRow


@dataclass(frozen=True)
class ReadingBalance:
    row_count: int
    missing_count: int
    total: int


def summarize_readings(values):
    row_count = 0
    missing_count = 0
    total = 0
    for value in values:
        row_count += 1
        if value is None:
            missing_count += 1
        else:
            total += value
    return ReadingBalance(row_count, missing_count, total)


def summarize_partition(rows: tuple[FrozenRow, ...]):
    return summarize_readings(row.value for row in rows)
