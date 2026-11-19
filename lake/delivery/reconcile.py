import sqlite3

from lake.delivery.attempts import AttemptNotFound as AttemptNotFound
from lake.delivery.attempts import attempt_outcome, load_attempt, record_attempt_outcome
from lake.delivery.outcomes import AttemptStatus, ReceiverOutcome
from lake.delivery.receiver import query_attempt


def reconcile(center, receiver, attempt_id):
    attempt = load_attempt(center, attempt_id)
    if attempt.status != AttemptStatus.PENDING:
        return attempt_outcome(center, attempt)
    try:
        observed = query_attempt(receiver, attempt.target, attempt.attempt_id)
    except sqlite3.OperationalError:
        observed = ReceiverOutcome(AttemptStatus.PENDING, lookup_available=False)
    return record_attempt_outcome(center, attempt.attempt_id, observed)
