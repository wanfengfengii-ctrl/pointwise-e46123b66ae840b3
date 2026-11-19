# -*- coding: utf-8 -*-
"""Fixture and runnable V1 comparison for the correction in CONTINUATIONS.md."""
from contextlib import contextmanager
from dataclasses import dataclass
import json
import sqlite3
from tempfile import TemporaryDirectory

from lake.audit.coverage import IncompleteCoverage
from lake.audit.readback import read_statement
from lake.audit.statement import Statement, issue_statement
from lake.audit.workday import initialize_workspace
from lake.delivery.attempts import send
from lake.delivery.outcomes import AttemptStatus
from lake.delivery.receiver import read_receiver_log, submit_attempt
from lake.delivery.reconcile import reconcile
from lake.dispatch.jobs import open_job
from lake.intake.batches import ingest
from lake.release.freeze import freeze
from lake.release.history import FrozenPartition


@dataclass(frozen=True)
class Example:
    target: str
    parent: Statement
    child: FrozenPartition
    competing_child: FrozenPartition

    def request(self, request_id="correction-1"):
        return {
            "target": self.target,
            "request_id": request_id,
            "parent_statement_id": self.parent.statement_id,
            "child_release_id": self.child.release_id,
            "partition": self.child.partition,
        }


def prepare_example(center, receiver):
    day, target, partition = "2026-04-17", "archive", "east"
    for station, value in (("A", 5), ("B", 11), ("C", 11)):
        ingest(center, station, day, "r1", [value])

    def publish(stations):
        release = freeze(center, [(partition, [(name, day, "r1") for name in stations])])
        return release.partitions[0]

    parent = publish(("A", "B"))
    child = publish(("C", "A"))
    competing_child = publish(("B", "A"))
    open_job(center, "parent-job", target, parent.release_id, (1, 2))
    send(center, receiver, "parent-job", 1, "parent-A")
    send(center, receiver, "parent-job", 2, "parent-B")
    statement = issue_statement(center, target, parent.release_id, (1, 2))
    return Example(target, statement, child, competing_child)


def deliver_c(center, receiver, example, *, target=None, hide_reply=False):
    if target is None:
        target = example.target
    job_id = f"{target}-child-job"
    open_job(center, job_id, target, example.child.release_id, (1, 2))
    return send(center, receiver, job_id, 1, f"{target}-C", hide_reply=hide_reply)


@contextmanager
def deny_next_commit(db):
    """Deny one COMMIT on the supplied test connection and allow rollback."""
    denied = False

    def authorize(action, operation, *_):
        nonlocal denied
        if action == sqlite3.SQLITE_TRANSACTION and operation == "COMMIT" and not denied:
            denied = True
            return sqlite3.SQLITE_DENY
        return sqlite3.SQLITE_OK

    def was_denied():
        return denied

    db.set_authorizer(authorize)
    try:
        yield was_denied
    finally:
        db.set_authorizer(None)


def equal(label, actual, expected):
    if actual != expected:
        raise AssertionError(f"{label}: expected {expected!r}, got {actual!r}")


def require_denied_commit(db, action):
    with deny_next_commit(db) as was_denied:
        try:
            action()
        except sqlite3.DatabaseError:
            equal("commit denial reached", was_denied(), True)
        else:
            raise AssertionError("expected SQLite to reject COMMIT")
    equal("transaction rolled back", db.in_transaction, False)


def compare_v1(workspace):
    with workspace.connect() as (center, receiver):
        example = prepare_example(center, receiver)
        initial = read_receiver_log(receiver, example.target)
        equal("initial receipts", [entry.receipt.attempt_id for entry in initial], ["parent-A", "parent-B"])
        require_denied_commit(receiver, lambda: submit_attempt(
            receiver, example.target, "denied-arrival", example.child.rows[0],
        ))
        equal("failed receiver commit", read_receiver_log(receiver, example.target), initial)
        outcome = deliver_c(center, receiver, example, hide_reply=True)
        equal("hidden C reply", outcome.status, AttemptStatus.PENDING)
        after_c = read_receiver_log(receiver, example.target)
        equal("C arrived", [entry.receipt.attempt_id for entry in after_c], ["parent-A", "parent-B", "archive-C"])

    with workspace.connect() as (center, receiver):
        equal("reconciled C", reconcile(center, receiver, "archive-C").receipt, after_c[-1].receipt)
        try:
            issue_statement(center, example.target, example.child.release_id, (1, 2))
        except IncompleteCoverage as exc:
            missing = list(exc.missing_lines)
            equal("V1 missing lines", missing, [2])
        else:
            raise AssertionError("V1 unexpectedly used a receipt from the parent release")
        send(center, receiver, "archive-child-job", 2, "child-A")
        count_before = center.execute("SELECT COUNT(*) FROM statements").fetchone()[0]
        require_denied_commit(center, lambda: issue_statement(center, example.target, example.child.release_id, (1, 2)))
        equal("failed statement commit", center.execute("SELECT COUNT(*) FROM statements").fetchone()[0], count_before)
        child = issue_statement(center, example.target, example.child.release_id, (1, 2))
        equal("original statement", read_statement(center, example.parent.statement_id), example.parent)
        totals = [example.parent.balance.total, child.balance.total]
        equal("V1 totals", totals, [16, 16])
        attempts = [entry.receipt.attempt_id for entry in read_receiver_log(receiver, example.target)]
        equal("V1 actual arrivals", attempts, ["parent-A", "parent-B", "archive-C", "child-A"])
        return {"v1_missing_child_lines": missing, "v1_totals": totals, "v1_arrivals": attempts}


def main():
    with TemporaryDirectory(prefix="linting-continuation-") as path:
        print(json.dumps(compare_v1(initialize_workspace(path)), sort_keys=True))


if __name__ == "__main__":
    main()
