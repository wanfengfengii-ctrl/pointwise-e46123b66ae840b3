from lake.audit.coverage import IncompleteCoverage
from lake.audit.readback import StatementNotFound, read_statement
from lake.audit.statement import issue_statement
from lake.delivery.attempts import read_job_status
from lake.delivery.outcomes import AttemptStatus
from lake.delivery.receiver import query_attempt, read_receiver_log, submit_attempt
from lake.delivery.reconcile import reconcile
from lake.dispatch.eligibility import SendIneligible
from lake.dispatch.handoff import handoff
from lake.dispatch.jobs import open_job
from lake.intake.batches import ingest
from lake.release.balances import ReadingBalance
from tests.support import WorkspaceCase


class StatementTests(WorkspaceCase):
    def test_incomplete_statement(self):
        release = self.release([5, 11])
        open_job(self.center, 'A', self.target, release.release_id, (1, 2))
        first = self.send('A', 1, 'a-1')
        self.send('A', 2, 'a-2', event='pending')
        with self.assertRaises(IncompleteCoverage) as caught:
            issue_statement(self.center, self.target, release.release_id, (1, 2))
        self.assertEqual(caught.exception.missing_lines, (2,))
        with self.assertRaises(StatementNotFound):
            read_statement(self.center, 1)
        self.assertEqual(reconcile(self.center, self.receiver, 'a-1'), first)
        self.assertEqual([entry.receipt for entry in read_receiver_log(self.receiver, self.target)], [first.receipt])

    def test_other_target_receipt(self):
        release = self.release([5, 11])
        open_job(self.center, 'X', 'x', release.release_id, (1, 2))
        open_job(self.center, 'Y', 'y', release.release_id, (2, 2))
        x = self.send('X', 1, 'x-1')
        self.send('X', 2, 'x-2', event='pending')
        y = self.send('Y', 2, 'y-2')
        with self.assertRaises(IncompleteCoverage) as caught:
            issue_statement(self.center, 'x', release.release_id, (1, 2))
        self.assertEqual(caught.exception.missing_lines, (2,))
        self.assertEqual(query_attempt(self.receiver, 'x', 'x-1'), x)
        self.assertEqual(query_attempt(self.receiver, 'y', 'y-2'), y)
        self.assertEqual([entry.receipt for entry in read_receiver_log(self.receiver, 'x')], [x.receipt])
        self.assertEqual([entry.receipt for entry in read_receiver_log(self.receiver, 'y')], [y.receipt])
        valid = issue_statement(self.center, 'y', release.release_id, (2, 2))
        self.assertEqual(valid.statement_id, 1)
        self.assertEqual(valid.rows[0].receipt, y.receipt)

    def test_equal_value_rows(self):
        release = self.release([5, 5])
        first_row, second_row = release.partitions[0].rows
        open_job(self.center, 'A', self.target, release.release_id, (1, 2))
        second = self.send('A', 2, 'a-2')
        handoff(self.center, 'A', 'B', (1, 2))
        self.assertEqual(read_job_status(self.center, 'B')[1].receipt, second.receipt)
        with self.assertRaises(SendIneligible):
            self.send('B', 2, 'b-2')
        first = self.send('B', 1, 'b-1')
        self.assertEqual(submit_attempt(self.receiver, self.target, 'a-2', second_row), second)
        statement = issue_statement(self.center, self.target, release.release_id, (1, 2))
        self.assertEqual([entry.row for entry in statement.rows], [first_row, second_row])
        self.assertEqual([entry.row.source.position for entry in statement.rows], [1, 2])
        self.assertEqual([entry.receipt for entry in statement.rows], [first.receipt, second.receipt])
        self.assertEqual(statement.balance, ReadingBalance(2, 0, 10))
        self.assertEqual([entry.receipt for entry in read_receiver_log(self.receiver, self.target)], [second.receipt, first.receipt])
        ingest(self.center, 'river', self.day, 'r2', [17, 19])
        self.reopen()
        self.assertEqual(read_statement(self.center, statement.statement_id), statement)

    def test_frozen_readback(self):
        release = self.release([5, 11])
        open_job(self.center, 'A', self.target, release.release_id, (1, 2))
        second = self.send('A', 2, 'a-2').receipt
        first = self.send('A', 1, 'a-1').receipt
        statement = issue_statement(self.center, self.target, release.release_id, (1, 2))
        ingest(self.center, 'river', self.day, 'r2', [19, 5])
        self.reopen()
        stored = read_statement(self.center, statement.statement_id)
        self.assertEqual(stored, statement)
        self.assertEqual([entry.row.value for entry in stored.rows], [5, 11])
        self.assertEqual([entry.row.source.batch.revision for entry in stored.rows], ['r1', 'r1'])
        self.assertEqual([entry.row.source.position for entry in stored.rows], [1, 2])
        self.assertEqual([entry.receipt for entry in stored.rows], [first, second])
        self.assertEqual(stored.balance, ReadingBalance(2, 0, 16))
        self.assertEqual(reconcile(self.center, self.receiver, 'a-2').status, AttemptStatus.RECEIVED)
        self.assertEqual(read_statement(self.center, statement.statement_id), stored)

    def test_subrange_totals(self):
        release = self.release([11, 0, None, -2])
        open_job(self.center, 'A', self.target, release.release_id, (2, 4))
        for line in (4, 2, 3):
            self.send('A', line, f'a-{line}')
        statement = issue_statement(self.center, self.target, release.release_id, (2, 4))
        self.assertEqual([entry.row.key.line for entry in statement.rows], [2, 3, 4])
        self.assertEqual([entry.row.value for entry in statement.rows], [0, None, -2])
        self.assertEqual(statement.balance, ReadingBalance(3, 1, -2))
        self.assertEqual(read_statement(self.center, statement.statement_id), statement)
        with self.assertRaises(IncompleteCoverage):
            issue_statement(self.center, self.target, release.release_id, (1, 4))

    def test_unknown_statement(self):
        release = self.release([5])
        open_job(self.center, 'A', self.target, release.release_id, (1, 1))
        self.send('A', 1, 'a-1')
        valid = issue_statement(self.center, self.target, release.release_id, (1, 1))
        with self.assertRaises(StatementNotFound):
            read_statement(self.center, 777)
        self.assertEqual(read_statement(self.center, valid.statement_id), valid)
