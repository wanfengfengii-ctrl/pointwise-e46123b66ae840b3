from dataclasses import dataclass

from lake.delivery.receipts import Receipt, read_receipts
from lake.release.partitions import FrozenRow
from lake.storage import Conflict


class IncompleteCoverage(Conflict):
    def __init__(self, target, missing_lines):
        self.target = target
        self.missing_lines = tuple(missing_lines)
        lines = ', '.join(str(line) for line in self.missing_lines)
        super().__init__(f'missing receipts for {target} on lines {lines}')


@dataclass(frozen=True)
class CoveredRow:
    row: FrozenRow
    receipt: Receipt


def require_coverage(db, target, rows):
    rows = tuple(rows)
    receipts = read_receipts(db, target, (row.key for row in rows))
    missing_lines = tuple(row.key.line for row in rows if row.key not in receipts)
    if missing_lines:
        raise IncompleteCoverage(target, missing_lines)
    return tuple(CoveredRow(row, receipts[row.key]) for row in rows)
