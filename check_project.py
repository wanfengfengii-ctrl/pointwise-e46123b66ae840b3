"""Run this project's full unittest suite from the repository root."""
import sys
from tempfile import TemporaryDirectory
import unittest

from continuation_examples import compare_v1
from lake.audit.workday import initialize_workspace


def joint_verification():
    with TemporaryDirectory(prefix='linting-joint-') as path:
        result = compare_v1(initialize_workspace(path))
    expected = {
        'v1_missing_child_lines': [2],
        'v1_totals': [16, 16],
        'v1_arrivals': ['parent-A', 'parent-B', 'archive-C', 'child-A'],
    }
    if result != expected:
        raise AssertionError(f'joint V1 comparison changed: {result!r}')
    print(f'JOINT_VERIFICATION {result}')


def main():
    suite = unittest.defaultTestLoader.discover('tests', top_level_dir='.')
    if suite.countTestCases():
        result = unittest.TextTestRunner(verbosity=2).run(suite)
    else:
        print('No tests discovered in tests/', file=sys.stderr)
        result = unittest.TestResult()
    for status, failed_cases in (
        ('FAIL', (test for test, _ in result.failures)),
        ('ERROR', (test for test, _ in result.errors)),
        ('UNEXPECTED_SUCCESS', result.unexpectedSuccesses),
    ):
        for test in failed_cases:
            print(f'{status} {test.id()}')
    successful = result.testsRun > 0 and result.wasSuccessful()
    print(f'TEST SUMMARY total={result.testsRun} failures={len(result.failures)} '
          f'errors={len(result.errors)} skipped={len(result.skipped)} '
          f'expected_failures={len(result.expectedFailures)} '
          f'unexpected_successes={len(result.unexpectedSuccesses)} '
          f'result={"PASS" if successful else "FAIL"}')
    if not successful:
        return 1
    try:
        joint_verification()
    except AssertionError as exc:
        print(f'JOINT_VERIFICATION FAIL {exc}', file=sys.stderr)
        return 1
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
