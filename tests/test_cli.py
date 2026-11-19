from contextlib import redirect_stderr, redirect_stdout
from io import StringIO
import json

from lake.audit.workday import main
from tests.support import WorkspaceCase


class CliTests(WorkspaceCase):
    def call_cli(self, command, payload=None, *, exit_code=0):
        argv = ['--workspace', self.path, command]
        if payload is not None:
            argv.append(json.dumps(payload))
        output, errors = StringIO(), StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            code = main(argv)
        self.assertEqual(code, exit_code, errors.getvalue())
        if code == 0:
            self.assertEqual(errors.getvalue(), '')
            return json.loads(output.getvalue())
        self.assertEqual(output.getvalue(), '')
        return json.loads(errors.getvalue())

    def test_workday_commands(self):
        self.assertEqual(self.call_cli('init'), {'center': 'center.sqlite', 'receiver': 'receiver.sqlite'})
        rows = self.call_cli('ingest', {
            'station': 'river', 'day': self.day, 'revision': 'r1', 'values': [5, 11],
        })
        self.assertEqual([row['source']['position'] for row in rows], [1, 2])
        release = self.call_cli('freeze', {'partitions': [['east', [['river', self.day, 'r1']]]]})
        release_id = release['release_id']
        partition = self.call_cli('read-release', {'release_id': release_id, 'partition': 'east'})
        self.assertEqual(partition['balance'], {'row_count': 2, 'missing_count': 0, 'total': 16})
        self.call_cli('open-job', {'job_id': 'A', 'target': self.target, 'release_id': release_id, 'bounds': [1, 2]})
        first = self.call_cli('send', {'job_id': 'A', 'line': 1, 'attempt_id': 'a-1', 'hide_reply': True})
        self.assertEqual(first['status'], 'pending')
        self.call_cli('handoff', {'from_job': 'A', 'to_job': 'B', 'bounds': [1, 2]})
        job = self.call_cli('job-status', {'job_id': 'B'})
        self.assertEqual([row['status'] for row in job], ['pending', 'ready'])
        self.assertEqual(self.call_cli('send', {'job_id': 'B', 'line': 1, 'attempt_id': 'extra'}, exit_code=2)['error'], 'SendIneligible')
        second = self.call_cli('send', {'job_id': 'B', 'line': 2, 'attempt_id': 'b-2'})
        self.call_cli('receiver-lookup', {'attempt_id': 'a-1', 'available': False})
        self.assertFalse(self.call_cli('reconcile', {'attempt_id': 'a-1'})['lookup_available'])
        log = self.call_cli('receiver-log', {'target': self.target})
        self.assertEqual([item['receipt']['attempt_id'] for item in log], ['a-1', 'b-2'])
        original = self.call_cli('receiver-outcome', {'attempt_id': 'a-1', 'status': 'received'})
        self.call_cli('receiver-lookup', {'attempt_id': 'a-1', 'available': True})
        self.assertEqual(self.call_cli('reconcile', {'attempt_id': 'a-1'}), original)
        statement = self.call_cli('issue-statement', {'target': self.target, 'release_id': release_id, 'bounds': [1, 2]})
        self.assertEqual([item['receipt'] for item in statement['rows']], [log[0]['receipt'], second['receipt']])
        self.assertEqual(statement['balance']['total'], 16)
        self.call_cli('init')
        self.assertEqual(self.call_cli('read-statement', {'statement_id': statement['statement_id']}), statement)
