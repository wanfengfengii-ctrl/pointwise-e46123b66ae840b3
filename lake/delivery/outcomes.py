from dataclasses import dataclass
from enum import StrEnum

from lake.delivery.receipts import Receipt
from lake.storage import BadInput, Conflict


class AttemptStatus(StrEnum):
    PENDING = 'pending'
    RECEIVED = 'received'
    NOT_RECEIVED = 'not_received'


class OutcomeConflict(Conflict):
    pass


@dataclass(frozen=True)
class ReceiverOutcome:
    status: AttemptStatus
    receipt: Receipt | None = None
    lookup_available: bool = True

    def __post_init__(self):
        if not isinstance(self.status, AttemptStatus):
            raise BadInput('invalid attempt status')
        if (self.status == AttemptStatus.RECEIVED) != (self.receipt is not None):
            raise BadInput('a received outcome must carry its original receipt')
        if not self.lookup_available and self.status != AttemptStatus.PENDING:
            raise BadInput('an unavailable lookup cannot establish a terminal outcome')


def parse_status(status):
    try:
        return AttemptStatus(status)
    except (ValueError, TypeError) as exc:
        raise BadInput('status must be pending, received or not_received') from exc


def resolve_outcome(current, observed):
    if current.status == AttemptStatus.PENDING:
        return observed
    if observed.status == AttemptStatus.PENDING or observed == current:
        return current
    raise OutcomeConflict(f'terminal outcome {current.status} conflicts with {observed.status}')
