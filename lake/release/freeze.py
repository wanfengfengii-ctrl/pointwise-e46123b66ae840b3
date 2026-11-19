from dataclasses import dataclass

from lake.release.balances import summarize_partition
from lake.release.history import FrozenPartition, allocate_release_number, save_release
from lake.release.partitions import build_partitions
from lake.release.sources import MissingSourceRevision, resolve_sources
from lake.storage import BadInput, LakeError, write_transaction


class ReleaseUnavailable(LakeError):
    pass


@dataclass(frozen=True)
class FrozenRelease:
    release_id: int
    partitions: tuple[FrozenPartition, ...]


def freeze(center, partitions):
    with write_transaction(center):
        try:
            sources = resolve_sources(center, partitions)
        except (MissingSourceRevision, BadInput) as exc:
            raise ReleaseUnavailable(str(exc)) from exc
        release_id = allocate_release_number(center)
        frozen = tuple(
            FrozenPartition(release_id, name, rows, summarize_partition(rows))
            for name, rows in build_partitions(release_id, sources)
        )
        save_release(center, release_id, frozen)
    return FrozenRelease(release_id, frozen)
