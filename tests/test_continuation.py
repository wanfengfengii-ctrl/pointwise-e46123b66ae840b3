from contextlib import closing
from dataclasses import asdict
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import unittest

from continuation_examples import compare_v1, deliver_c, prepare_example, require_denied_commit
from lake.audit.continuation import ContinuationRejected, ParentStatementNotFound
from lake.audit.readback import ContinuationNotFound, read_continuation, read_statement
from lake.audit.statement import issue_continuation, issue_statement
from lake.audit.workday import initialize_workspace
from lake.delivery.attempts import send
from lake.delivery.receiver import read_active_partition, read_receiver_log
from lake.dispatch.jobs import open_job
from lake.intake.batches import ingest
from lake.release.freeze import freeze
from lake.release.history import ReleaseNotFound
from lake.storage import BadInput, Conflict, open_database
from tests.support import WorkspaceCase


class ContinuationCase(WorkspaceCase):
    def setUp(self):
        super().setUp()
        self.example = prepare_example(self.center, self.receiver)

    def issue(self, **overrides):
        request = self.example.request()
        request.update(overrides)
        return issue_continuation(self.center, self.receiver, **request)

    def active(self):
        return read_active_partition(self.receiver, self.example.target, self.example.parent.statement_id)


class ContinuationTests(ContinuationCase):
    def test_pending_until_child_arrival(self):
        self.assertEqual(self.issue(), {'status': 'pending', 'missing_lines': [1]})
        self.assertEqual(self.active(), {'status': 'uncontinued'})
        deliver_c(self.center, self.receiver, self.example, target='other-archive')
        self.assertEqual(self.issue(), {'status': 'pending', 'missing_lines': [1]})
        self.assertEqual(self.active(), {'status': 'uncontinued'})
        deliver_c(self.center, self.receiver, self.example, hide_reply=True)
        self.assertEqual(
            self.center.execute("SELECT status FROM attempts WHERE attempt_id = 'archive-C'").fetchone()[0],
            'pending',
        )
        issued = self.issue()
        self.assertEqual(issued['status'], 'issued')
        self.assertTrue(issued['continuation_id'])
        self.assertNotEqual(self.active(), {'status': 'uncontinued'})

    def test_confirmation_contents(self):
        deliver_c(self.center, self.receiver, self.example)
        issued = self.issue()
        self.assertEqual(issued['status'], 'issued')
        active = self.active()
        self.assertEqual(active['status'], 'confirmed')
        confirmation = active['confirmation']
        self.assertEqual(confirmation['target'], self.example.target)
        self.assertEqual(confirmation['request_id'], 'correction-1')
        self.assertEqual(confirmation['parent_statement_id'], self.example.parent.statement_id)
        self.assertEqual(confirmation['parent_release_id'], self.example.parent.release_id)
        self.assertEqual(confirmation['child_release_id'], self.example.child.release_id)
        self.assertEqual(confirmation['partition'], 'east')
        self.assertEqual(
            [item['row'] for item in confirmation['rows']],
            [asdict(row) for row in self.example.child.rows],
        )
        self.assertEqual([item['kind'] for item in confirmation['rows']], ['received', 'reused'])
        self.assertEqual(
            [item['receipt']['attempt_id'] for item in confirmation['rows']],
            ['archive-C', 'parent-A'],
        )
        self.assertEqual(
            [item['receipt']['key']['release_id'] for item in confirmation['rows']],
            [self.example.child.release_id, self.example.parent.release_id],
        )
        self.assertEqual(
            [(item['row']['key']['line'], item['receipt']['key']['line'])
             for item in confirmation['rows']],
            [(1, 1), (2, 1)],
        )
        self.assertEqual(
            [item['row']['key']['release_id'] for item in confirmation['rows']],
            [self.example.child.release_id] * 2,
        )
        self.assertEqual([item['row'] for item in confirmation['retired_rows']],
                         [asdict(self.example.parent.rows[1].row)])
        self.assertEqual(
            [item['receipt']['attempt_id'] for item in confirmation['retired_rows']], ['parent-B'],
        )
        self.assertEqual(confirmation['balance'], {'row_count': 2, 'missing_count': 0, 'total': 16})
        self.assertEqual(
            read_continuation(self.center, issued['continuation_id']),
            {'continuation_id': issued['continuation_id'], 'confirmation': confirmation},
        )

    def test_shifted_rows_are_reused(self):
        deliver_c(self.center, self.receiver, self.example)
        self.issue()
        second_parent = issue_statement(self.center, self.example.target, self.example.parent.release_id, (1, 2))
        issued = issue_continuation(
            self.center, self.receiver, self.example.target, 'correction-2',
            second_parent.statement_id, self.example.competing_child.release_id, 'east',
        )
        self.assertEqual(issued['status'], 'issued')
        confirmation = read_continuation(self.center, issued['continuation_id'])['confirmation']
        self.assertEqual([item['kind'] for item in confirmation['rows']], ['reused', 'reused'])
        self.assertEqual([item['row']['key']['line'] for item in confirmation['rows']], [1, 2])
        self.assertEqual(
            [item['receipt']['key']['release_id'] for item in confirmation['rows']],
            [self.example.parent.release_id] * 2,
        )
        self.assertEqual(confirmation['retired_rows'], [])

    def test_same_value_other_source_needs_arrival(self):
        with TemporaryDirectory(prefix='linting-other-source-') as path:
            workspace = initialize_workspace(path)
            with workspace.connect() as (center, receiver):
                ingest(center, 'A', self.day, 'r1', [5])
                ingest(center, 'X', self.day, 'r2', [5])
                parent = freeze(center, [('east', [('A', self.day, 'r1')])]).partitions[0]
                child = freeze(center, [('east', [('X', self.day, 'r2')])]).partitions[0]
                open_job(center, 'p', self.target, parent.release_id, (1, 1))
                send(center, receiver, 'p', 1, 'p-A')
                statement = issue_statement(center, self.target, parent.release_id, (1, 1))
                request = {
                    'target': self.target, 'request_id': 'same-value',
                    'parent_statement_id': statement.statement_id,
                    'child_release_id': child.release_id, 'partition': 'east',
                }
                self.assertEqual(
                    issue_continuation(center, receiver, **request),
                    {'status': 'pending', 'missing_lines': [1]},
                )
                open_job(center, 'c', self.target, child.release_id, (1, 1))
                send(center, receiver, 'c', 1, 'c-X')
                issued = issue_continuation(center, receiver, **request)
                confirmation = read_continuation(center, issued['continuation_id'])['confirmation']
                self.assertEqual([item['kind'] for item in confirmation['rows']], ['received'])
                self.assertEqual(
                    [item['receipt']['attempt_id'] for item in confirmation['rows']], ['c-X'],
                )
                self.assertEqual(
                    [item['receipt']['attempt_id'] for item in confirmation['retired_rows']], ['p-A'],
                )

    def test_parent_must_cover_whole_partition(self):
        ingest(self.center, 'D', self.day, 'r1', [7])
        wide = freeze(self.center, [('east', [
            ('D', self.day, 'r1'), ('A', self.day, 'r1'), ('B', self.day, 'r1'),
        ])])
        open_job(self.center, 'wide', self.example.target, wide.release_id, (1, 3))
        for line in (1, 2, 3):
            send(self.center, self.receiver, 'wide', line, f'wide-{line}')
        partial = issue_statement(self.center, self.example.target, wide.release_id, (2, 3))
        with self.assertRaises(ContinuationRejected):
            issue_continuation(
                self.center, self.receiver, self.example.target, 'bad-parent',
                partial.statement_id, self.example.child.release_id, 'east',
            )
        with self.assertRaises(ContinuationNotFound):
            read_continuation(self.center, 'continuation-999')
        with self.assertRaises(ReleaseNotFound):
            issue_continuation(
                self.center, self.receiver, self.example.target, 'other-partition',
                self.example.parent.statement_id, self.example.child.release_id, 'west',
            )

    def test_empty_child_partition_retires_parent(self):
        ingest(self.center, 'empty', self.day, 'r1', [])
        empty = freeze(self.center, [('east', [('empty', self.day, 'r1')])])
        issued = issue_continuation(
            self.center, self.receiver, self.example.target, 'empty-child',
            self.example.parent.statement_id, empty.release_id, 'east',
        )
        self.assertEqual(issued['status'], 'issued')
        confirmation = read_continuation(self.center, issued['continuation_id'])['confirmation']
        self.assertEqual(confirmation['rows'], [])
        self.assertEqual([item['row']['key']['line'] for item in confirmation['retired_rows']], [1, 2])
        self.assertEqual(confirmation['balance'], {'row_count': 0, 'missing_count': 0, 'total': 0})

    def test_unknown_and_foreign_references(self):
        with self.assertRaises(ParentStatementNotFound):
            issue_continuation(
                self.center, self.receiver, self.example.target, 'missing-parent', 777,
                self.example.child.release_id, 'east',
            )
        with self.assertRaises(BadInput):
            issue_continuation(
                self.center, self.receiver, self.example.target, 'text-parent', 'continuation-1',
                self.example.child.release_id, 'east',
            )
        with self.assertRaises(ContinuationRejected):
            issue_continuation(
                self.center, self.receiver, 'other-archive', 'wrong-target',
                self.example.parent.statement_id, self.example.child.release_id, 'east',
            )
        with self.assertRaises(ContinuationNotFound):
            read_continuation(self.center, 'continuation-999')

    def test_old_statement_and_log_are_preserved(self):
        log_before = read_receiver_log(self.receiver, self.example.target)
        deliver_c(self.center, self.receiver, self.example)
        issued = self.issue()
        self.assertEqual(self.issue(), issued)
        self.assertEqual(read_statement(self.center, self.example.parent.statement_id), self.example.parent)
        self.assertEqual(
            [entry.receipt.attempt_id for entry in read_receiver_log(self.receiver, self.example.target)],
            [entry.receipt.attempt_id for entry in log_before] + ['archive-C'],
        )


class ContinuationTransactionTests(ContinuationCase):
    def test_missing_does_not_consume_request(self):
        self.assertEqual(self.issue(), {'status': 'pending', 'missing_lines': [1]})
        self.assertEqual(self.center.execute('SELECT COUNT(*) FROM continuations').fetchone()[0], 0)
        self.assertEqual(
            self.receiver.execute('SELECT COUNT(*) FROM continuation_confirmations').fetchone()[0], 0,
        )
        deliver_c(self.center, self.receiver, self.example)
        self.assertEqual(self.issue()['status'], 'issued')

    def test_target_commit_failure_keeps_parent_and_retries(self):
        deliver_c(self.center, self.receiver, self.example)
        require_denied_commit(self.receiver, lambda: self.issue())
        self.assertEqual(self.active(), {'status': 'uncontinued'})
        self.assertEqual(self.center.execute('SELECT COUNT(*) FROM continuations').fetchone()[0], 0)
        self.assertEqual(
            self.receiver.execute('SELECT COUNT(*) FROM continuation_confirmations').fetchone()[0], 0,
        )
        self.reopen()
        issued = self.issue()
        self.assertEqual(issued['status'], 'issued')
        self.assertEqual(self.active()['status'], 'confirmed')

    def test_center_commit_failure_resigns_after_reopen(self):
        deliver_c(self.center, self.receiver, self.example)
        require_denied_commit(self.center, lambda: self.issue())
        confirmation = self.active()
        self.assertEqual(confirmation['status'], 'confirmed')
        self.assertEqual(self.center.execute('SELECT COUNT(*) FROM continuations').fetchone()[0], 0)
        self.reopen()
        first = self.issue()
        self.assertEqual(first['status'], 'issued')
        self.assertEqual(
            read_continuation(self.center, first['continuation_id'])['confirmation'],
            confirmation['confirmation'],
        )
        self.assertEqual(self.issue(), first)
        self.reopen()
        self.assertEqual(self.issue(), first)
        self.assertEqual(self.active(), confirmation)

    def test_competing_requests_are_rejected(self):
        deliver_c(self.center, self.receiver, self.example)
        first = self.issue()
        for overrides in (
            {'request_id': 'correction-other', 'child_release_id': self.example.competing_child.release_id},
            {'child_release_id': self.example.competing_child.release_id},
            {'request_id': 'correction-other'},
        ):
            with self.subTest(overrides=overrides):
                with self.assertRaises(Conflict):
                    self.issue(**overrides)
        self.reopen()
        self.assertEqual(self.issue(), first)
        self.assertEqual(
            read_continuation(self.center, first['continuation_id'])['confirmation']['child_release_id'],
            self.example.child.release_id,
        )

    def test_center_readback_without_target_database(self):
        deliver_c(self.center, self.receiver, self.example)
        issued = self.issue()
        with TemporaryDirectory(prefix='center-only-') as path:
            shutil.copy(Path(self.path) / 'center.sqlite', Path(path) / 'center.sqlite')
            with closing(open_database(Path(path) / 'center.sqlite')) as only_center:
                self.assertEqual(
                    read_continuation(only_center, issued['continuation_id'])['confirmation']['request_id'],
                    'correction-1',
                )

    def test_target_readback_without_center_activity(self):
        deliver_c(self.center, self.receiver, self.example)
        issued = self.issue()
        confirmation = self.active()
        with TemporaryDirectory(prefix='target-only-') as path:
            shutil.copy(Path(self.path) / 'receiver.sqlite', Path(path) / 'receiver.sqlite')
            with closing(open_database(Path(path) / 'receiver.sqlite')) as only_target:
                self.assertEqual(
                    read_active_partition(
                        only_target, self.example.target, self.example.parent.statement_id,
                    ),
                    confirmation,
                )


class ContinuationJointVerification(WorkspaceCase):
    def test_v1_fixture_comparison(self):
        with TemporaryDirectory(prefix='linting-continuation-joint-') as path:
            original = compare_v1(initialize_workspace(path))
        self.assertEqual(original['v1_missing_child_lines'], [2])
        self.assertEqual(original['v1_totals'], [16, 16])
        self.assertEqual(original['v1_arrivals'], ['parent-A', 'parent-B', 'archive-C', 'child-A'])

    def test_reopened_pair_and_legacy_reading(self):
        example = prepare_example(self.center, self.receiver)
        deliver_c(self.center, self.receiver, example)
        request = example.request()
        first = issue_continuation(self.center, self.receiver, **request)
        self.reopen()
        self.reopen()
        self.assertEqual(issue_continuation(self.center, self.receiver, **request), first)
        self.assertEqual(read_statement(self.center, example.parent.statement_id), example.parent)
        confirmation = read_continuation(self.center, first['continuation_id'])['confirmation']
        self.assertEqual(confirmation['balance'], {'row_count': 2, 'missing_count': 0, 'total': 16})
        self.assertEqual(
            [entry.receipt.attempt_id for entry in read_receiver_log(self.receiver, example.target)],
            ['parent-A', 'parent-B', 'archive-C'],
        )


if __name__ == '__main__':
    unittest.main()


class ContinuationCliTests(ContinuationCase):
    def call_cli(self, command, payload):
        from contextlib import redirect_stderr, redirect_stdout
        from io import StringIO
        import json as json_module
        from lake.audit.workday import main

        output, errors = StringIO(), StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(['--workspace', self.path, command, json_module.dumps(payload)])
        if code != 0:
            self.fail(f'{command} exited {code}: {errors.getvalue()}')
        return json_module.loads(output.getvalue())

    def test_cli_round_trip(self):
        deliver_c(self.center, self.receiver, self.example)
        request = self.example.request()
        self.assertEqual(self.call_cli('active-partition', {
            'target': request['target'],
            'parent_statement_id': request['parent_statement_id'],
        }), {'status': 'uncontinued'})
        issued = self.call_cli('issue-continuation', request)
        self.assertEqual(issued['status'], 'issued')
        active = self.call_cli('active-partition', {
            'target': request['target'],
            'parent_statement_id': request['parent_statement_id'],
        })
        self.assertEqual(active['status'], 'confirmed')
        self.assertEqual(self.call_cli('read-continuation', {
            'continuation_id': issued['continuation_id'],
        }), {'continuation_id': issued['continuation_id'], 'confirmation': active['confirmation']})
        self.assertEqual(self.call_cli('read-statement', {
            'statement_id': request['parent_statement_id'],
        })['statement_id'], request['parent_statement_id'])

    def test_cli_rejection_is_nonzero(self):
        request = self.example.request()
        request['parent_statement_id'] = 777
        from contextlib import redirect_stderr, redirect_stdout
        from io import StringIO
        import json
        from lake.audit.workday import main

        errors = StringIO()
        with redirect_stdout(StringIO()), redirect_stderr(errors):
            code = main(['--workspace', self.path, 'issue-continuation', json.dumps(request)])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(errors.getvalue())['error'], 'ParentStatementNotFound')
