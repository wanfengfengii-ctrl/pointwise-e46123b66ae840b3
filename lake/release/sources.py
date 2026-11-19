from lake.intake.revisions import load_revision
from lake.intake.rows import BatchRevisionKey
from lake.intake.validation import validate_revision_key
from lake.storage import BadInput, NotFound, identifier


class MissingSourceRevision(NotFound):
    pass


def resolve_sources(db, partitions):
    if not isinstance(partitions, (tuple, list)) or not partitions:
        raise BadInput('partitions must be a nonempty ordered list of partition and sources pairs')
    seen_partitions = set()
    seen_batches = set()
    resolved = []
    for entry in partitions:
        if not isinstance(entry, (tuple, list)) or len(entry) != 2:
            raise BadInput('partition entries must contain a name and an ordered source list')
        name, sources = entry
        name = identifier(name, 'partition')
        if name in seen_partitions:
            raise BadInput(f'duplicate partition: {name}')
        seen_partitions.add(name)
        if not isinstance(sources, (tuple, list)) or not sources:
            raise BadInput(f'partition {name} needs at least one source batch')
        rows = []
        for source in sources:
            if isinstance(source, BatchRevisionKey):
                source = (source.station, source.day, source.revision)
            if not isinstance(source, (tuple, list)) or len(source) != 3:
                raise BadInput('source must contain station, day and revision')
            key = validate_revision_key(*source)
            if key in seen_batches:
                raise BadInput(f'duplicate source batch: {key}')
            seen_batches.add(key)
            batch = load_revision(db, key)
            if batch is None:
                raise MissingSourceRevision(f'source revision not found: {key}')
            rows.extend(batch)
        resolved.append((name, tuple(rows)))
    return tuple(resolved)
