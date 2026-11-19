from lake.intake.batches import ingest
from lake.intake.revisions import RevisionConflict
from lake.intake.validation import InvalidBatch
from lake.release.balances import ReadingBalance
from lake.release.freeze import ReleaseUnavailable, freeze
from lake.release.history import ReleaseNotFound, read_release
from tests.support import WorkspaceCase


class ReleaseTests(WorkspaceCase):
    def test_daily_balance(self):
        release = self.release([5, None, 11])
        partition = read_release(self.center, release.release_id, 'east')
        self.assertEqual(partition, release.partitions[0])
        self.assertEqual([row.key.line for row in partition.rows], [1, 2, 3])
        self.assertEqual([row.value for row in partition.rows], [5, None, 11])
        self.assertEqual([row.source.position for row in partition.rows], [1, 2, 3])
        self.assertEqual(partition.balance, ReadingBalance(3, 1, 16))

    def test_pinned_revision(self):
        ingest(self.center, 'river', self.day, 'r1', [5, None, 11])
        ingest(self.center, 'river', self.day, 'r2', [5, None, 19])
        release = freeze(self.center, [('east', [('river', self.day, 'r1')])])
        partition = read_release(self.center, release.release_id, 'east')
        self.assertEqual([row.value for row in partition.rows], [5, None, 11])
        self.assertEqual([row.source.batch.revision for row in partition.rows], ['r1'] * 3)
        self.assertEqual(partition.balance, ReadingBalance(3, 1, 16))

    def test_zero_and_missing(self):
        release = self.release([5, None, 0, -2])
        partition = read_release(self.center, release.release_id, 'east')
        self.assertEqual([row.value for row in partition.rows], [5, None, 0, -2])
        self.assertEqual(partition.balance, ReadingBalance(4, 1, 3))

    def test_init_repeat(self):
        old = self.release([5, None, 11])
        ingest(self.center, 'river', self.day, 'r2', [21, 0])
        self.reopen()
        self.reopen()
        self.assertEqual(read_release(self.center, old.release_id, 'east'), old.partitions[0])
        new = freeze(self.center, [('east', [('river', self.day, 'r2')])])
        self.assertEqual(read_release(self.center, new.release_id, 'east').balance, ReadingBalance(2, 0, 21))

    def test_missing_release(self):
        release = self.release([5])
        for release_id, name in ((777, 'east'), (release.release_id, 'west')):
            with self.subTest(release_id=release_id, name=name):
                with self.assertRaises(ReleaseNotFound):
                    read_release(self.center, release_id, name)

    def test_invalid_reading_keeps_existing_release(self):
        release = self.release([5, None, 11])
        for value in ('11', 1.25):
            with self.subTest(value=value):
                with self.assertRaises(InvalidBatch):
                    ingest(self.center, 'river', self.day, 'r2', [5, value])
        self.assertEqual(read_release(self.center, release.release_id, 'east'), release.partitions[0])

    def test_boolean_input(self):
        old = self.release([5, None, 11])
        with self.assertRaises(InvalidBatch):
            ingest(self.center, 'river', self.day, 'r2', [5, True])
        with self.assertRaises(ReleaseUnavailable):
            freeze(self.center, [('east', [('river', self.day, 'r2')])])
        self.assertEqual(read_release(self.center, old.release_id, 'east').balance, ReadingBalance(3, 1, 16))

    def test_atomic_ingest(self):
        old = self.release([5, 11])
        with self.assertRaises(InvalidBatch):
            ingest(self.center, 'river', self.day, 'r2', [2, 3, 'invalid'])
        with self.assertRaises(ReleaseUnavailable):
            freeze(self.center, [('east', [('river', self.day, 'r2')])])
        new = self.release([2, 3, 4], revision='r2')
        self.assertEqual([row.value for row in new.partitions[0].rows], [2, 3, 4])
        self.assertEqual(read_release(self.center, old.release_id, 'east'), old.partitions[0])

    def test_missing_revision_is_atomic(self):
        old = self.release([5, None, 11])
        ingest(self.center, 'hill', self.day, 'r2', [23])
        with self.assertRaises(ReleaseUnavailable):
            freeze(self.center, [('east', [('river', self.day, 'r1')]), ('west', [('hill', self.day, 'r1')])])
        with self.assertRaises(ReleaseNotFound):
            read_release(self.center, old.release_id + 1, 'east')
        self.assertEqual(read_release(self.center, old.release_id, 'east'), old.partitions[0])
        new = freeze(self.center, [('west', [('hill', self.day, 'r2')])])
        self.assertEqual(new.release_id, old.release_id + 1)

    def test_partition_and_source_order(self):
        ingest(self.center, 'hill', self.day, 'r1', [5, 5])
        ingest(self.center, 'river', self.day, 'r1', [11])
        ingest(self.center, 'park', self.day, 'r1', [13])
        release = freeze(self.center, [
            ('west', [('river', self.day, 'r1'), ('hill', self.day, 'r1')]),
            ('east', [('park', self.day, 'r1')]),
        ])
        self.assertEqual([part.partition for part in release.partitions], ['west', 'east'])
        west = read_release(self.center, release.release_id, 'west')
        east = read_release(self.center, release.release_id, 'east')
        self.assertEqual([row.value for row in west.rows], [11, 5, 5])
        self.assertEqual([row.source.position for row in west.rows], [1, 1, 2])
        self.assertEqual([row.key.line for row in west.rows + east.rows], [1, 2, 3, 4])

    def test_duplicate_revision_preserves_original(self):
        original = self.release([5])
        with self.assertRaises(RevisionConflict):
            ingest(self.center, 'river', self.day, 'r1', [7, 11])
        second = freeze(self.center, [('east', [('river', self.day, 'r1')])])
        self.assertEqual([row.value for row in second.partitions[0].rows], [5])
        self.assertEqual(read_release(self.center, original.release_id, 'east'), original.partitions[0])

    def test_integer_precision_across_reopen(self):
        large = 2 ** 100
        release = self.release([large, -large + 3, None, 0])
        self.reopen()
        stored = read_release(self.center, release.release_id, 'east')
        self.assertEqual([row.value for row in stored.rows], [large, -large + 3, None, 0])
        self.assertEqual(stored.balance, ReadingBalance(4, 1, 3))
