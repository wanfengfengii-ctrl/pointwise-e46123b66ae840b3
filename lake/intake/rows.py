from dataclasses import dataclass


@dataclass(frozen=True)
class BatchRevisionKey:
    station: str
    day: str
    revision: str


@dataclass(frozen=True)
class SourceRowRef:
    batch: BatchRevisionKey
    position: int


@dataclass(frozen=True)
class ObservationRow:
    source: SourceRowRef
    value: int | None


def make_rows(key, values):
    return tuple(
        ObservationRow(SourceRowRef(key, position), value)
        for position, value in enumerate(values, start=1)
    )
