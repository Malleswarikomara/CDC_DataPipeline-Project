"""
Tests for Out-of-Order and Late-Arriving Event Handling.
"""

from src.validation import EventValidator
from src.cdc_processor import CDCProcessor


def test_out_of_order_event_does_not_regress_current_state(db_manager, valid_order_event):
    """
    When a higher sequence event arrives first (seq 3), followed by a late-arriving lower sequence (seq 2):
    1. Seq 3 mutates current_state (last_sequence = 3).
    2. Late Seq 2 is preserved in event_history for auditability.
    3. Current_state is NOT regressed to seq 2.
    """
    validator = EventValidator()
    processor = CDCProcessor()

    # Step 1: Ingest Initial Insert (seq 1)
    ev1 = validator.validate(valid_order_event, "test.json", 1, "run_01")
    with db_manager.transaction() as conn:
        processor.process_event(ev1, conn, "run_01", set())

    # Step 2: Ingest Newer UPDATE with sequence 3 (skipping sequence 2)
    ev3_dict = dict(valid_order_event)
    ev3_dict["event_id"] = "evt_seq_3"
    ev3_dict["event_type"] = "UPDATE"
    ev3_dict["source_sequence"] = 3
    ev3_dict["order_status"] = "SHIPPED"
    ev3_dict["total_amount"] = 300.00
    ev3 = validator.validate(ev3_dict, "test.json", 2, "run_01")
    with db_manager.transaction() as conn:
        status3, _ = processor.process_event(ev3, conn, "run_01", set())
        assert status3 == "APPLIED_UPDATE"

    curr_after_seq3 = db_manager.get_current_state("ORD-TEST-001")
    assert curr_after_seq3["last_sequence"] == 3
    assert curr_after_seq3["order_status"] == "SHIPPED"
    assert curr_after_seq3["total_amount"] == 300.00

    # Step 3: Now late-arriving event with sequence 2 arrives!
    ev2_dict = dict(valid_order_event)
    ev2_dict["event_id"] = "evt_seq_2_late"
    ev2_dict["event_type"] = "UPDATE"
    ev2_dict["source_sequence"] = 2
    ev2_dict["order_status"] = "CONFIRMED"
    ev2_dict["total_amount"] = 200.00
    ev2 = validator.validate(ev2_dict, "test.json", 3, "run_01")
    with db_manager.transaction() as conn:
        status2, _ = processor.process_event(ev2, conn, "run_01", set())
        assert status2 == "OUT_OF_ORDER_HISTORY_ONLY"

    # CRITICAL ASSERTION: Current state MUST NOT have regressed!
    curr_final = db_manager.get_current_state("ORD-TEST-001")
    assert curr_final["last_sequence"] == 3  # Still sequence 3!
    assert curr_final["order_status"] == "SHIPPED"  # Still SHIPPED!
    assert curr_final["total_amount"] == 300.00  # Still 300.00!

    # CRITICAL ASSERTION: Both events MUST exist in event_history for auditable trail
    conn = db_manager.get_connection()
    cur = conn.execute("SELECT event_id, source_sequence FROM event_history WHERE order_id = 'ORD-TEST-001';")
    hist_entries = {r["event_id"]: r["source_sequence"] for r in cur.fetchall()}
    conn.close()

    assert "evt_seq_3" in hist_entries
    assert "evt_seq_2_late" in hist_entries
    assert hist_entries["evt_seq_2_late"] == 2
