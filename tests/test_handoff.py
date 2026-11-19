from lake.audit.coverage import IncompleteCoverage
from lake.audit.readback import read_statement
from lake.audit.statement import issue_statement
from lake.delivery.attempts import load_attempt, read_job_status
from lake.delivery.outcomes import AttemptStatus, OutcomeConflict
from lake.delivery.receiver import (
    query_attempt, read_receiver_log, record_receiver_outcome, set_lookup_available, submit_attempt,
)
from lake.delivery.reconcile import AttemptNotFound, reconcile
from lake.dispatch.eligibility import SendIneligible
from lake.dispatch.handoff import HandoffRejected, handoff
from lake.dispatch.jobs import JobNotFound, load_job, open_job
from lake.dispatch.ranges import InvalidJobRange
from lake.release.balances import ReadingBalance
from lake.storage import Conflict
from tests.support import WorkspaceCase


class HandoffTests(WorkspaceCase):
    def open_day_job(self, values=(5, 11), bounds=(1, 2)):
        release = self.release(values)
        open_job(self.center, 'A', self.target, release.release_id, bounds)
        return release

    def test_ordinary_handoff(self):
        release = self.open_day_job()
        result = handoff(self.center, 'A', 'B', (1, 2))
        self.assertEqual([row.line for row in result.rows], [1, 2])
        for line in (1, 2):
            with self.assertRaises(SendIneligible):
                self.send('A', line, f'old-{line}')
            self.assertEqual(self.send('B', line, f'new-{line}').status, AttemptStatus.RECEIVED)
        arrivals = read_receiver_log(self.receiver, self.target)
        self.assertEqual([(item.row.key.line, item.receipt.attempt_id) for item in arrivals], [(1, 'new-1'), (2, 'new-2')])
        self.assertEqual(issue_statement(self.center, self.target, release.release_id, (1, 2)).balance, ReadingBalance(2, 0, 16))

    def test_partial_handoff(self):
        self.open_day_job((5, 11, 13), (1, 3))
        handoff(self.center, 'A', 'B', (1, 2))
        self.assertEqual([item.key.line for item in read_job_status(self.center, 'A')], [3])
        self.assertEqual([item.key.line for item in read_job_status(self.center, 'B')], [1, 2])
        with self.assertRaises(SendIneligible):
            self.send('B', 3, 'unauthorized')
        self.assertEqual(self.send('A', 3, 'a-3').status, AttemptStatus.RECEIVED)
        self.assertEqual(self.send('B', 2, 'b-2').status, AttemptStatus.RECEIVED)

    def test_received_handoff(self):
        release = self.open_day_job()
        original = self.send('A', 1, 'a-1').receipt
        handoff(self.center, 'A', 'B', (1, 2))
        self.assertEqual(read_job_status(self.center, 'B')[0].receipt, original)
        with self.assertRaises(SendIneligible):
            self.send('B', 1, 'b-1')
        second = self.send('B', 2, 'b-2').receipt
        statement = issue_statement(self.center, self.target, release.release_id, (1, 2))
        self.assertEqual(tuple(item.receipt for item in statement.rows), (original, second))
        self.assertEqual(len(read_receiver_log(self.receiver, self.target)), 2)

    def test_job_conflict(self):
        release = self.open_day_job(bounds=(1, 1))
        with self.assertRaises(InvalidJobRange):
            open_job(self.center, 'B', self.target, release.release_id, (1, 2))
        with self.assertRaises(JobNotFound):
            load_job(self.center, 'B')
        self.assertEqual(self.send('A', 1, 'a-1').status, AttemptStatus.RECEIVED)
        open_job(self.center, 'B', self.target, release.release_id, (2, 2))
        self.assertEqual(self.send('B', 2, 'b-2').status, AttemptStatus.RECEIVED)

    def test_unauthorized_range_is_atomic(self):
        self.open_day_job(bounds=(1, 1))
        with self.assertRaises(HandoffRejected):
            handoff(self.center, 'A', 'B', (1, 2))
        with self.assertRaises(JobNotFound):
            self.send('B', 1, 'b-1')
        with self.assertRaises(SendIneligible):
            self.send('A', 2, 'a-2')
        only = self.send('A', 1, 'a-1').receipt
        self.assertEqual([entry.receipt for entry in read_receiver_log(self.receiver, self.target)], [only])

    def test_cross_boundary(self):
        first = self.open_day_job()
        second = self.release([13], revision='r2')
        open_job(self.center, 'X', 'other', first.release_id, (1, 1))
        open_job(self.center, 'Y', self.target, second.release_id, (1, 1))
        for destination in ('X', 'Y'):
            with self.subTest(destination=destination):
                with self.assertRaises(HandoffRejected):
                    handoff(self.center, 'A', destination, (1, 2))
        self.assertEqual([item.key.line for item in read_job_status(self.center, 'A')], [1, 2])
        self.assertEqual(self.send('A', 1, 'a-1').status, AttemptStatus.RECEIVED)

    def test_pending_transfer(self):
        release = self.open_day_job()
        outcome = self.send('A', 1, 'a-1', hide_reply=True)
        self.assertEqual(outcome.status, AttemptStatus.PENDING)
        original, = read_receiver_log(self.receiver, self.target)
        self.assertEqual(original.row.value, 5)
        self.assertEqual(original.receipt.attempt_id, 'a-1')
        handoff(self.center, 'A', 'B', (1, 2))
        self.assertEqual([(row.key.line, row.status, row.attempt_id) for row in read_job_status(self.center, 'B')], [
            (1, 'pending', 'a-1'), (2, 'ready', None),
        ])
        with self.assertRaises(SendIneligible):
            self.send('B', 1, 'b-1')
        self.assertEqual(read_receiver_log(self.receiver, self.target), (original,))
        second = self.send('B', 2, 'b-2').receipt
        with self.assertRaises(IncompleteCoverage):
            issue_statement(self.center, self.target, release.release_id, (1, 2))
        self.reopen()
        settled = reconcile(self.center, self.receiver, 'a-1')
        self.assertEqual(settled.receipt, original.receipt)
        statement = issue_statement(self.center, self.target, release.release_id, (1, 2))
        self.assertEqual([item.row.value for item in statement.rows], [5, 11])
        self.assertEqual([item.row.source.position for item in statement.rows], [1, 2])
        self.assertEqual([item.row.source.batch.station for item in statement.rows], ['river', 'river'])
        self.assertEqual([item.receipt for item in statement.rows], [original.receipt, second])
        self.assertEqual(statement.balance, ReadingBalance(2, 0, 16))
        self.assertEqual(reconcile(self.center, self.receiver, 'a-1'), settled)
        self.assertEqual(read_statement(self.center, statement.statement_id), statement)
        self.assertEqual([entry.receipt for entry in read_receiver_log(self.receiver, self.target)], [original.receipt, second])

    def test_lookup_unavailable(self):
        self.open_day_job()
        self.send('A', 1, 'a-1', hide_reply=True)
        set_lookup_available(self.receiver, self.target, 'a-1', False)
        handoff(self.center, 'A', 'B', (1, 2))
        self.reopen()
        outcome = reconcile(self.center, self.receiver, 'a-1')
        self.assertEqual(outcome.status, AttemptStatus.PENDING)
        self.assertFalse(outcome.lookup_available)
        with self.assertRaises(SendIneligible):
            self.send('B', 1, 'b-1')
        self.assertEqual(self.send('B', 2, 'b-2').status, AttemptStatus.RECEIVED)
        set_lookup_available(self.receiver, self.target, 'a-1', True)
        self.assertEqual(reconcile(self.center, self.receiver, 'a-1').status, AttemptStatus.RECEIVED)
        self.assertEqual(len(read_receiver_log(self.receiver, self.target)), 2)

    def test_receiver_replay(self):
        release = self.open_day_job()
        sent = self.send('A', 1, 'a-1')
        self.assertEqual(self.send('A', 1, 'a-1'), sent)
        row = release.partitions[0].rows[0]
        replay = submit_attempt(self.receiver, self.target, 'a-1', row)
        self.assertEqual(replay, sent)
        with self.assertRaises(Conflict):
            submit_attempt(self.receiver, self.target, 'a-1', release.partitions[0].rows[1])
        self.reopen()
        self.assertEqual(query_attempt(self.receiver, self.target, 'a-1'), sent)
        arrivals = read_receiver_log(self.receiver, self.target)
        self.assertEqual([entry.receipt for entry in arrivals], [sent.receipt])
        self.assertEqual(arrivals[0].row, row)

    def test_retry_owner(self):
        release = self.open_day_job()
        row = release.partitions[0].rows[0]
        self.assertEqual(self.send('A', 1, 'a-1', event='pending').status, AttemptStatus.PENDING)
        handoff(self.center, 'A', 'B', (1, 2))
        self.assertEqual(reconcile(self.center, self.receiver, 'a-1').status, AttemptStatus.PENDING)
        with self.assertRaises(SendIneligible):
            self.send('B', 1, 'too-early')
        second = self.send('B', 2, 'b-2').receipt
        record_receiver_outcome(self.receiver, self.target, 'a-1', row, 'not_received')
        with self.assertRaises(SendIneligible):
            self.send('B', 1, 'before-reconcile')
        self.reopen()
        self.assertEqual(reconcile(self.center, self.receiver, 'a-1').status, AttemptStatus.NOT_RECEIVED)
        self.assertEqual(reconcile(self.center, self.receiver, 'a-1').status, AttemptStatus.NOT_RECEIVED)
        with self.assertRaises(SendIneligible):
            self.send('A', 1, 'old-owner')
        first = self.send('B', 1, 'b-1').receipt
        self.assertEqual(submit_attempt(self.receiver, self.target, 'a-1', row).status, AttemptStatus.NOT_RECEIVED)
        with self.assertRaises(OutcomeConflict):
            record_receiver_outcome(self.receiver, self.target, 'a-1', row, 'received')
        statement = issue_statement(self.center, self.target, release.release_id, (1, 2))
        self.assertEqual([entry.receipt for entry in statement.rows], [first, second])
        self.assertEqual(statement.balance, ReadingBalance(2, 0, 16))
        self.assertEqual([entry.receipt for entry in read_receiver_log(self.receiver, self.target)], [second, first])

    def test_unknown_attempt(self):
        self.open_day_job()
        self.send('A', 1, 'a-1', event='pending')
        with self.assertRaises(AttemptNotFound):
            reconcile(self.center, self.receiver, 'unknown')
        self.assertEqual(load_attempt(self.center, 'a-1').status, AttemptStatus.PENDING)
        self.assertEqual(read_receiver_log(self.receiver, self.target), ())

    def test_repeated_handoff_keeps_occupancy(self):
        release = self.open_day_job()
        self.send('A', 1, 'a-1', event='pending')
        handoff(self.center, 'A', 'B', (1, 2))
        handoff(self.center, 'B', 'C', (1, 1))
        with self.assertRaises(SendIneligible):
            self.send('C', 1, 'c-early')
        row = release.partitions[0].rows[0]
        actual = record_receiver_outcome(self.receiver, self.target, 'a-1', row, 'received')
        self.assertEqual(reconcile(self.center, self.receiver, 'a-1'), actual)
        self.assertEqual(read_job_status(self.center, 'C')[0].receipt, actual.receipt)
        self.assertEqual(self.send('B', 2, 'b-2').status, AttemptStatus.RECEIVED)

    def test_range_outside_frozen_release(self):
        release = self.release([5, 11])
        with self.assertRaises(InvalidJobRange):
            open_job(self.center, 'A', self.target, release.release_id, (1, 3))
        with self.assertRaises(JobNotFound):
            load_job(self.center, 'A')
        job = open_job(self.center, 'A', self.target, release.release_id, (1, 2))
        self.assertEqual(job.release_id, release.release_id)
