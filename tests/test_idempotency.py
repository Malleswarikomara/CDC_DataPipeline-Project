"""
Tests for Idempotency, Safe Reruns, and Determinism.
"""

from pathlib import Path
import json

from src.pipeline import CDCPipeline
from src.config import PipelineConfig


def test_pipeline_rerun_idempotency(tmp_path, valid_order_event):
    """
    Rerunning the exact same input batch against the target database:
    1. First run applies all mutations.
    2. Second run detects 100% of records as duplicates.
    3. current_state rows, hashes, and versions remain 100% identical.
    4. event_history does not add any redundant rows.
    """
    test_file = tmp_path / "orders_batch.json"
    batch_data = [
        valid_order_event,
        {
            "event_id": "test_evt_002",
            "event_type": "UPDATE",
            "event_timestamp": "2026-10-30T12:05:00Z",
            "source_sequence": 2,
            "order_id": valid_order_event["order_id"],
            "customer_id": valid_order_event["customer_id"],
            "order_status": "CONFIRMED",
            "total_amount": 180.00,
            "currency": "USD",
            "shipping_address": valid_order_event["shipping_address"],
            "item_count": 2,
        },
    ]
    test_file.write_text(json.dumps(batch_data), encoding="utf-8")

    db_path = str(tmp_path / "idempotent.db")
    config = PipelineConfig(db_path=db_path)
    pipeline = CDCPipeline(config=config)

    # Run 1
    tracker1 = pipeline.run(test_file)
    assert tracker1.records_read == 2
    assert tracker1.records_accepted == 2
    assert tracker1.records_duplicate == 0

    state_after_run1 = pipeline.db.get_current_state(valid_order_event["order_id"])
    assert state_after_run1["record_version"] == 2
    assert state_after_run1["last_sequence"] == 2

    # Check history count
    conn = pipeline.db.get_connection()
    cur = conn.execute("SELECT COUNT(*) FROM event_history;")
    hist_count_run1 = cur.fetchone()[0]
    conn.close()
    assert hist_count_run1 == 2

    # Run 2: Exact same input file
    tracker2 = pipeline.run(test_file)
    assert tracker2.records_read == 2
    assert tracker2.records_accepted == 0
    assert tracker2.records_duplicate == 2  # 100% deduplicated!

    state_after_run2 = pipeline.db.get_current_state(valid_order_event["order_id"])
    # MUST BE IDENTICAL
    assert state_after_run2 == state_after_run1

    # History count MUST NOT have grown
    conn = pipeline.db.get_connection()
    cur = conn.execute("SELECT COUNT(*) FROM event_history;")
    hist_count_run2 = cur.fetchone()[0]
    conn.close()
    assert hist_count_run2 == hist_count_run1
