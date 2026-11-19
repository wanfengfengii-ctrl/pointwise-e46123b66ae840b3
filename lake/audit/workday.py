import argparse
from contextlib import closing, contextmanager
from dataclasses import asdict, dataclass, is_dataclass
import inspect
import json
from pathlib import Path
import sqlite3
import sys
from tempfile import TemporaryDirectory

from lake.audit.continuation import (
    initialize_center_continuations, initialize_receiver_continuations, read_active_continuation,
)
from lake.audit.readback import read_continuation, read_statement
from lake.audit.statement import initialize_statements, issue_continuation, issue_statement
from lake.delivery.attempts import initialize_attempts, load_attempt, read_job_status, send
from lake.delivery.outcomes import AttemptStatus
from lake.delivery.receipts import initialize_receipts
from lake.delivery.receiver import (
    initialize_receiver, read_receiver_log, record_receiver_outcome, set_lookup_available,
)
from lake.delivery.reconcile import reconcile
from lake.dispatch.assignments import initialize_assignments
from lake.dispatch.eligibility import SendIneligible
from lake.dispatch.handoff import handoff
from lake.dispatch.jobs import initialize_jobs, open_job
from lake.intake.batches import ingest
from lake.intake.revisions import initialize_revisions
from lake.release.freeze import freeze
from lake.release.history import initialize_releases, load_frozen_index, read_release
from lake.release.partitions import select_frozen_rows
from lake.storage import BadInput, LakeError, open_database


@dataclass(frozen=True)
class Workspace:
    path: Path

    @contextmanager
    def connect(self):
        with closing(open_database(self.path / 'center.sqlite')) as center:
            with closing(open_database(self.path / 'receiver.sqlite')) as receiver:
                yield center, receiver


def initialize_workspace(path):
    workspace = Workspace(Path(path))
    workspace.path.mkdir(parents=True, exist_ok=True)
    with workspace.connect() as (center, receiver):
        initialize_revisions(center)
        initialize_releases(center)
        initialize_jobs(center)
        initialize_attempts(center)
        initialize_assignments(center)
        initialize_receipts(center)
        initialize_statements(center)
        initialize_center_continuations(center)
        initialize_receiver(receiver)
        initialize_receiver_continuations(receiver)
    return workspace


def _receiver_outcome(center, receiver, attempt_id, status):
    attempt = load_attempt(center, attempt_id)
    index = load_frozen_index(center, attempt.key.release_id)
    row, = select_frozen_rows(index, (attempt.key.line, attempt.key.line))
    return record_receiver_outcome(receiver, attempt.target, attempt.attempt_id, row, status)


def _receiver_lookup(center, receiver, attempt_id, available):
    attempt = load_attempt(center, attempt_id)
    set_lookup_available(receiver, attempt.target, attempt.attempt_id, available)
    return {'attempt_id': attempt.attempt_id, 'lookup_available': available}


_OPERATIONS = {
    'ingest': (ingest, (0,)),
    'freeze': (freeze, (0,)),
    'read-release': (read_release, (0,)),
    'open-job': (open_job, (0,)),
    'handoff': (handoff, (0,)),
    'send': (send, (0, 1)),
    'reconcile': (reconcile, (0, 1)),
    'issue-statement': (issue_statement, (0,)),
    'read-statement': (read_statement, (0,)),
    'issue-continuation': (issue_continuation, (0, 1)),
    'read-continuation': (read_continuation, (0,)),
    'active-partition': (read_active_continuation, (1,)),
    'job-status': (read_job_status, (0,)),
    'receiver-log': (read_receiver_log, (1,)),
    'receiver-outcome': (_receiver_outcome, (0, 1)),
    'receiver-lookup': (_receiver_lookup, (0, 1)),
}


def _json_default(value):
    if is_dataclass(value) and not isinstance(value, type):
        return asdict(value)
    raise TypeError(f'cannot serialize {type(value).__name__}')


def _json(value):
    return json.dumps(value, default=_json_default, ensure_ascii=False, sort_keys=True)


def _invoke(workspace, command, raw):
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        raise BadInput(f'expected a JSON object: {exc}') from exc
    if not isinstance(payload, dict):
        raise BadInput('command arguments must be a JSON object')
    operation, connections = _OPERATIONS[command]
    with workspace.connect() as databases:
        args = tuple(databases[index] for index in connections)
        try:
            request = inspect.signature(operation).bind(*args, **payload)
        except TypeError as exc:
            raise BadInput(str(exc)) from exc
        return operation(*request.args, **request.kwargs)


def _demo_equal(label, actual, expected):
    if actual != expected:
        raise LakeError(f'{label}: expected {expected!r}, got {actual!r}')


def _scenario(case_id, data, expected, actual, downstream_expected, downstream_actual, paths, oracle):
    print('POINTWISE_SCENARIO ' + _json({
        'case_id': case_id, 'input': data, 'expected': expected, 'actual': actual,
        'downstream_expected': downstream_expected, 'downstream_actual': downstream_actual,
        'code_paths': paths, 'oracle': oracle,
    }))


def _continuation_demo(target):
    """Show a corrected continuation through a center-signing failure and reopen."""
    with TemporaryDirectory(prefix='linting-continuation-') as path:
        return _continuation_scenario(path, target)


def _continuation_scenario(path, target):
    from continuation_examples import deliver_c, prepare_example, require_denied_commit
    from lake.audit.continuation import read_active_continuation
    from lake.storage import Conflict

    with initialize_workspace(path).connect() as (center, receiver):
        example = prepare_example(center, receiver)
        request = example.request()
        before = issue_continuation(center, receiver, **request)
        _demo_equal('continuation pending', before, {'status': 'pending', 'missing_lines': [1]})
        _demo_equal('target uncontinued', read_active_continuation(
            receiver, target, example.parent.statement_id), {'status': 'uncontinued'})
        deliver_c(center, receiver, example, hide_reply=True)
        _demo_equal('staged arrivals', [
            entry.receipt.attempt_id for entry in read_receiver_log(receiver, target)
        ], ['parent-A', 'parent-B', 'archive-C'])

        require_denied_commit(center, lambda: issue_continuation(center, receiver, **request))
        _demo_equal('no center continuation after failed commit',
                    center.execute('SELECT COUNT(*) FROM continuations').fetchone()[0], 0)

    with initialize_workspace(path).connect() as (center, receiver):
        target_confirmation = read_active_continuation(receiver, target, example.parent.statement_id)
        _demo_equal('target keeps confirmation', target_confirmation['status'], 'confirmed')
        rejections = []
        for overrides in (
            {'request_id': 'correction-other',
             'child_release_id': example.competing_child.release_id},
            {'child_release_id': example.competing_child.release_id},
        ):
            conflicting = dict(request, **overrides)
            try:
                issue_continuation(center, receiver, **conflicting)
            except Conflict as exc:
                rejections.append(type(exc).__name__)
            else:
                raise LakeError('a competing continuation was accepted')
        signed = issue_continuation(center, receiver, **request)
        _demo_equal('continuation replay', issue_continuation(center, receiver, **request), signed)
        stored = read_continuation(center, signed['continuation_id'])
        _demo_equal('confirmation shared', stored['confirmation'], target_confirmation['confirmation'])
        _demo_equal('continuation replay', read_continuation(
            center, signed['continuation_id']), stored)
        confirmation = stored['confirmation']
        expected_rows = {
            'lines': [1, 2], 'kinds': ['received', 'reused'],
            'attempts': ['archive-C', 'parent-A'],
        }
        actual_rows = {
            'lines': [item['row']['key']['line'] for item in confirmation['rows']],
            'kinds': [item['kind'] for item in confirmation['rows']],
            'attempts': [item['receipt']['attempt_id'] for item in confirmation['rows']],
        }
        _demo_equal('child order and receipts', actual_rows, expected_rows)
        _demo_equal('retired parent row', [
            (item['row']['key']['line'], item['receipt']['attempt_id'])
            for item in confirmation['retired_rows']
        ], [(2, 'parent-B')])
        _demo_equal('continuation balance', confirmation['balance'],
                    {'row_count': 2, 'missing_count': 0, 'total': 16})
        _demo_equal('old statement unchanged', read_statement(
            center, example.parent.statement_id), example.parent)
        _demo_equal('actual flow unchanged', [
            entry.receipt.attempt_id for entry in read_receiver_log(receiver, target)
        ], ['parent-A', 'parent-B', 'archive-C'])
        old_statement = read_statement(center, example.parent.statement_id)
        print('CONTINUATION_DEMO ' + _json({
            'continuation_id': signed['continuation_id'],
            'target_active_partition': {
                'status': 'confirmed',
                'balance': confirmation['balance'],
                'rows': actual_rows,
                'retired_rows': [
                    {'line': item['row']['key']['line'],
                     'attempt_id': item['receipt']['attempt_id']}
                    for item in confirmation['retired_rows']
                ],
            },
            'old_statement': {
                'statement_id': old_statement.statement_id,
                'balance': asdict(old_statement.balance),
                'attempts': [entry.receipt.attempt_id for entry in old_statement.rows],
            },
        }))
        _scenario(
            'continuation',
            {'request': request, 'child': [('C', 11), ('A', 5)],
             'parent': [('A', 5), ('B', 11)], 'rejected': rejections},
            {'status': 'issued', 'continuation_id': signed['continuation_id']},
            {'status': 'issued',
             'continuation_id': read_continuation(center, signed['continuation_id'])['continuation_id']},
            expected_rows, actual_rows,
            ['lake/audit/continuation.py', 'lake/audit/statement.py',
             'lake/audit/readback.py', 'lake/delivery/receiver.py'],
            'Reuse parent-A for the unchanged A reading at child line 2; take archive-C for '
            'child line 1; retire parent line 2 (B); add 11 + 5.')
    return stored


def _demo():
    sample_path = Path(__file__).resolve().parents[2] / 'samples' / 'workday.json'
    sample = json.loads(sample_path.read_text(encoding='utf-8'))
    day, target = sample['day'], sample['target']
    with TemporaryDirectory(prefix='linting-workday-') as path:
        workspace = initialize_workspace(path)
        with workspace.connect() as (center, receiver):
            ingest(center, 'hill', day, 'r1', sample['daily_rows'])
            daily = freeze(center, [('north', [('hill', day, 'r1')])])
            partition = read_release(center, daily.release_id, 'north')
            values = [row.value for row in partition.rows]
            actual_balance = asdict(partition.balance)
            expected_balance = {'row_count': 3, 'missing_count': 1, 'total': 16}
            _demo_equal('daily readings', values, [5, None, 11])
            _demo_equal('daily balance', actual_balance, expected_balance)
            _scenario('v1', sample['daily_rows'], [5, None, 11], values, expected_balance, actual_balance,
                      ['lake/release/freeze.py', 'lake/release/history.py', 'lake/release/balances.py'],
                      'Count the three input positions, skip one null and add 5 + 11.')

            ingest(center, 'river', day, 'r1', sample['handoff_rows'])
            delivery = freeze(center, [('east', [('river', day, 'r1')])])
            open_job(center, 'A', target, delivery.release_id, (1, 2))
            first = send(center, receiver, 'A', 1, 'A-1', hide_reply=True)
            _demo_equal('hidden outcome', first.status, AttemptStatus.PENDING)
            handoff(center, 'A', 'B', (1, 2))
            try:
                send(center, receiver, 'B', 1, 'B-1')
            except SendIneligible:
                pass
            else:
                raise LakeError('line 1 accepted a new attempt before the original was settled')
            second = send(center, receiver, 'B', 2, 'B-2')
            _demo_equal('line 2 can be delivered immediately', second.status, AttemptStatus.RECEIVED)
            original_arrivals = read_receiver_log(receiver, target)

        with initialize_workspace(path).connect() as (center, receiver):
            settled = reconcile(center, receiver, 'A-1')
            statement = issue_statement(center, target, delivery.release_id, (1, 2))
            stored = read_statement(center, statement.statement_id)
            _demo_equal('readback', stored, statement)
            _demo_equal('original receipt', settled.receipt, original_arrivals[0].receipt)
            _demo_equal('line 2 receipt', statement.rows[1].receipt, second.receipt)
            _demo_equal('repeated reconciliation', reconcile(center, receiver, 'A-1'), settled)
            arrivals = read_receiver_log(receiver, target)
            _demo_equal('receiver readback', arrivals, original_arrivals)
            expected_receipts = [[1, 'A-1'], [2, 'B-2']]
            actual_receipts = [[entry.row.key.line, entry.receipt.attempt_id] for entry in arrivals]
            _demo_equal('actual receipt sequence', actual_receipts, expected_receipts)
            expected_statement = {
                'values': [5, 11], 'positions': [1, 2], 'stations': ['river', 'river'],
                'attempts': ['A-1', 'B-2'], 'total': 16,
            }
            actual_statement = {
                'values': [entry.row.value for entry in stored.rows],
                'positions': [entry.row.source.position for entry in stored.rows],
                'stations': [entry.row.source.batch.station for entry in stored.rows],
                'attempts': [entry.receipt.attempt_id for entry in stored.rows],
                'total': stored.balance.total,
            }
            _demo_equal('signed statement', actual_statement, expected_statement)
            _scenario('v5', {'values': sample['handoff_rows'], 'target': target, 'handoff': ['A', 'B']},
                      expected_receipts, actual_receipts, expected_statement, actual_statement,
                      ['lake/dispatch/handoff.py', 'lake/delivery/receiver.py', 'lake/delivery/reconcile.py',
                       'lake/audit/statement.py', 'lake/audit/readback.py'],
                      'Use the original A attempt for line 1 and the B attempt for line 2, in frozen order. Add 5 + 11.')
        _continuation_demo(target)
    return stored


def main(argv=None):
    parser = argparse.ArgumentParser(description='Linting observation lake workday')
    parser.add_argument('--workspace', type=Path, help='directory for the center and receiver databases')
    commands = parser.add_subparsers(dest='command', required=True)
    commands.add_parser('init')
    commands.add_parser('demo')
    for name in _OPERATIONS:
        command = commands.add_parser(name)
        command.add_argument('payload', help='JSON object containing the named request arguments')
    args = parser.parse_args(argv)
    try:
        if args.command == 'demo':
            if args.workspace is not None:
                raise BadInput('demo uses a temporary workspace')
            result = _demo()
        else:
            if args.workspace is None:
                raise BadInput('--workspace is required')
            workspace = initialize_workspace(args.workspace)
            if args.command == 'init':
                result = {'center': 'center.sqlite', 'receiver': 'receiver.sqlite'}
            else:
                result = _invoke(workspace, args.command, args.payload)
        print(_json(result))
    except LakeError as exc:
        print(_json({'error': type(exc).__name__, 'message': str(exc)}), file=sys.stderr)
        return 2
    except (OSError, sqlite3.Error) as exc:
        print(_json({'error': type(exc).__name__, 'message': str(exc)}), file=sys.stderr)
        return 3
    return 0


if __name__ == '__main__':
    sys.set_int_max_str_digits(0)
    raise SystemExit(main())
