"""
Tests for Negative Scenarios, Edge Cases, and Transactional Rollback.
"""

import pytest
from pathlib import Path
import json
import sqlite3

from src.pipeline import CDCPipeline
from src.config import PipelineConfig
from src.validation import EventValidator
from src.cdc_processor import CDCProcessor
from src.models import RejectionCode


def test_empty_batch_execution(tmp_path):
    """Empty input file should process with 0 counts without error."""
    empty_file = tmp_path / "empty_batch.json"
    empty_file.write_text("[]", encoding="utf-8")

    db_path = str(tmp_path / "edge_empty.db")
    pipeline = CDCPipeline(config=PipelineConfig(db_path=db_path))
    tracker = pipeline.run(empty_file)

    assert tracker.records_read == 0
    assert tracker.records_accepted == 0
    assert tracker.records_rejected == 0
    assert tracker.status == "SUCCESS"


def test_delete_nonexistent_entity(db_manager):
    """DELETE event on an unseen entity must be rejected and quarantined."""
    validator = EventValidator()
    processor = CDCProcessor()
    delete_ghost = {
        "event_id": "ghost_del",
        "event_type": "DELETE",
        "event_timestamp": "2026-10-30T12:00:00Z",
        "source_sequence": 1,
        "order_id": "ORD-GHOST-888",
        "customer_id": "CUST-GHOST",
        "order_status": "CANCELLED",
        "total_amount": 0.0,
        "currency": "USD",
        "shipping_address": "Ghost St",
        "item_count": 0,
    }
    event = validator.validate(delete_ghost, "test.json", 1, "run_01")
    with db_manager.transaction() as conn:
        status, rej = processor.process_event(event, conn, "run_01", set())
        assert status == "REJECTED"
        assert rej is not None
        assert rej.rejection_code == RejectionCode.DELETE_NON_EXISTENT_ENTITY.value


def test_illegal_status_transition_reversion(db_manager, valid_order_event):
    """Attempting to revert an order from terminal 'DELIVERED' status back to 'PENDING' must be rejected."""
    validator = EventValidator()
    processor = CDCProcessor()

    # Step 1: Create order directly in DELIVERED status
    valid_order_event["order_status"] = "DELIVERED"
    ev1 = validator.validate(valid_order_event, "test.json", 1, "run_01")
    with db_manager.transaction() as conn:
        processor.process_event(ev1, conn, "run_01", set())

    # Step 2: Attempt illegal UPDATE to PENDING
    revert_dict = dict(valid_order_event)
    revert_dict["event_id"] = "evt_revert_deliv"
    revert_dict["event_type"] = "UPDATE"
    revert_dict["source_sequence"] = 2
    revert_dict["order_status"] = "PENDING"
    ev2 = validator.validate(revert_dict, "test.json", 2, "run_01")

    with db_manager.transaction() as conn:
        status, rej = processor.process_event(ev2, conn, "run_01", set())
        assert status == "REJECTED"
        assert rej is not None
        assert rej.rejection_code == RejectionCode.INVALID_STATUS_TRANSITION.value


def test_transactional_rollback_on_partial_failure(db_manager, valid_order_event):
    """
    Simulates a mid-batch unexpected system failure.
    The database transaction MUST roll back entirely, ensuring no partial or corrupted rows persist.
    """
    validator = EventValidator()
    processor = CDCProcessor()
    ev1 = validator.validate(valid_order_event, "test.json", 1, "run_01")

    # Deliberately raise an exception inside a transaction after inserting
    with pytest.raises(RuntimeError):
        with db_manager.transaction() as conn:
            processor.process_event(ev1, conn, "run_01", set())
            raise RuntimeError("Simulated mid-batch network/power crash!")

    # Verify that nothing was committed due to rollback
    current = db_manager.get_current_state("ORD-TEST-001")
    assert current is None

    conn = db_manager.get_connection()
    cur = conn.execute("SELECT COUNT(*) FROM event_history;")
    assert cur.fetchone()[0] == 0
    conn.close()
