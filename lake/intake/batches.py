from lake.intake.revisions import save_revision
from lake.intake.rows import make_rows
from lake.intake.validation import validate_batch_input
from lake.storage import write_transaction


def ingest(center, station, day, revision, values):
    key, validated_values = validate_batch_input(station, day, revision, values)
    rows = make_rows(key, validated_values)
    with write_transaction(center):
        save_revision(center, key, rows)
    return rows
