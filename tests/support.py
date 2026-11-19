from contextlib import ExitStack
from tempfile import TemporaryDirectory
import unittest

from lake.audit.workday import initialize_workspace
from lake.delivery.attempts import send
from lake.intake.batches import ingest
from lake.release.freeze import freeze


class WorkspaceCase(unittest.TestCase):
    day = '2026-04-17'
    target = 'archive'

    def setUp(self):
        folder = self.enterContext(TemporaryDirectory(prefix='linting-regression-'))
        self.path = folder
        self.connections = ExitStack()
        self.addCleanup(self.connections.close)
        self.reopen()

    def reopen(self):
        self.connections.close()
        workspace = initialize_workspace(self.path)
        self.center, self.receiver = self.connections.enter_context(workspace.connect())

    def release(self, values, *, station='river', revision='r1'):
        ingest(self.center, station, self.day, revision, values)
        return freeze(self.center, [('east', [(station, self.day, revision)])])

    def send(self, job, line, attempt, **options):
        return send(self.center, self.receiver, job, line, attempt, **options)
