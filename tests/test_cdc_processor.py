"""
Tests for CDCProcessor lifecycle mutations (INSERT, UPDATE, DELETE/Tombstone, and Audit History).
"""

import json
from src.cdc_processor import CDCProcessor
from src.validation import EventValidator
from src.models import ValidatedEvent


def test_cdc_insert_lifecycle(db_manager, valid_order_event):
    """INSERT event creates new record in current_state and appends to event_history."""
    validator = EventValidator()
    event: ValidatedEvent = validator.validate(valid_order_event, "test.json", 1, "run_01")
    processor = CDCProcessor()

    with db_manager.transaction() as conn:
        status, rej = processor.process_event(event, conn, "run_01", set())
        assert status == "APPLIED_INSERT"
        assert rej is None

    current = db_manager.get_current_state("ORD-TEST-001")
    assert current is not None
    assert current["order_status"] == "PENDING"
    assert current["total_amount"] == 150.00
    assert current["is_deleted"] == 0
    assert current["record_version"] == 1
    assert current["last_sequence"] == 1


def test_cdc_update_lifecycle(db_manager, valid_order_event):
    """UPDATE event modifies current_state, increments version, and records before/after in history."""
    validator = EventValidator()
    processor = CDCProcessor()

    # 1. Initial Insert
    event1 = validator.validate(valid_order_event, "test.json", 1, "run_01")
    with db_manager.transaction() as conn:
        processor.process_event(event1, conn, "run_01", set())

    # 2. Update to CONFIRMED with new amount
    update_dict = dict(valid_order_event)
    update_dict["event_id"] = "test_evt_002"
    update_dict["event_type"] = "UPDATE"
    update_dict["source_sequence"] = 2
    update_dict["order_status"] = "CONFIRMED"
    update_dict["total_amount"] = 175.50
    event2 = validator.validate(update_dict, "test.json", 2, "run_01")

    with db_manager.transaction() as conn:
        status, rej = processor.process_event(event2, conn, "run_01", set())
        assert status == "APPLIED_UPDATE"
        assert rej is None

    current = db_manager.get_current_state("ORD-TEST-001")
    assert current["order_status"] == "CONFIRMED"
    assert current["total_amount"] == 175.50
    assert current["record_version"] == 2
    assert current["last_sequence"] == 2

    # Check History Table has 2 records
    conn = db_manager.get_connection()
    cur = conn.execute("SELECT * FROM event_history WHERE order_id = 'ORD-TEST-001' ORDER BY source_sequence ASC;")
    hist_rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    assert len(hist_rows) == 2
    assert hist_rows[0]["payload_before"] is None
    assert hist_rows[1]["payload_before"] is not None
    assert json.loads(hist_rows[1]["payload_before"])["order_status"] == "PENDING"


def test_cdc_delete_tombstone(db_manager, valid_order_event):
    """DELETE event applies tombstone (is_deleted=1, deleted_at) rather than hard purging."""
    validator = EventValidator()
    processor = CDCProcessor()

    # Initial Insert
    event1 = validator.validate(valid_order_event, "test.json", 1, "run_01")
    with db_manager.transaction() as conn:
        processor.process_event(event1, conn, "run_01", set())

    # Delete
    delete_dict = dict(valid_order_event)
    delete_dict["event_id"] = "test_evt_del_01"
    delete_dict["event_type"] = "DELETE"
    delete_dict["source_sequence"] = 2
    delete_dict["event_timestamp"] = "2026-10-30T12:05:00Z"
    event_del = validator.validate(delete_dict, "test.json", 2, "run_01")

    with db_manager.transaction() as conn:
        status, rej = processor.process_event(event_del, conn, "run_01", set())
        assert status == "APPLIED_DELETE"
        assert rej is None

    current = db_manager.get_current_state("ORD-TEST-001")
    assert current["is_deleted"] == 1
    assert current["deleted_at"] == "2026-10-30T12:05:00Z"
    assert current["record_version"] == 2
