from collections.abc import Iterable

from lake.audit.coverage import CoveredRow
from lake.release.balances import ReadingBalance, summarize_readings


def statement_totals(entries: Iterable[CoveredRow]) -> ReadingBalance:
    return summarize_readings(entry.row.value for entry in entries)
