"""
Tests for Duplicate Event Handling and Deduplication Semantics.
"""

from src.validation import EventValidator
from src.cdc_processor import CDCProcessor


def test_in_batch_duplicate_handling(db_manager, valid_order_event):
    """Duplicate event_id within the same batch should be skipped and counted as DUPLICATE."""
    validator = EventValidator()
    processor = CDCProcessor()
    event = validator.validate(valid_order_event, "test.json", 1, "run_01")

    batch_dedup_set = set()
    with db_manager.transaction() as conn:
        status1, rej1 = processor.process_event(event, conn, "run_01", batch_dedup_set)
        assert status1 == "APPLIED_INSERT"

        # Same event_id presented again in the same batch
        status2, rej2 = processor.process_event(event, conn, "run_01", batch_dedup_set)
        assert status2 == "DUPLICATE"
        assert rej2 is None

    # Verify event_history contains only 1 entry
    conn = db_manager.get_connection()
    cur = conn.execute("SELECT COUNT(*) FROM event_history WHERE event_id = ?;", (event.event_id,))
    cnt = cur.fetchone()[0]
    conn.close()
    assert cnt == 1


def test_cross_run_duplicate_handling(db_manager, valid_order_event):
    """Event already persisted in previous run should be detected as DUPLICATE in subsequent run."""
    validator = EventValidator()
    processor = CDCProcessor()
    event = validator.validate(valid_order_event, "test.json", 1, "run_01")

    # Run 1: Insert into DB
    with db_manager.transaction() as conn:
        processor.process_event(event, conn, "run_01", set())

    # Run 2: Re-ingest with clean in-memory set
    with db_manager.transaction() as conn:
        status, rej = processor.process_event(event, conn, "run_02", set())
        assert status == "DUPLICATE"
        assert rej is None
