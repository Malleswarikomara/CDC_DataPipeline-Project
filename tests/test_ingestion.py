"""
Tests for EventIngestor (file formats, streaming, empty files, malformed syntax).
"""

import pytest
from pathlib import Path
import json
import csv

from src.ingestion import EventIngestor
from src.models import RejectionCode


def test_ingest_empty_file(tmp_path):
    """Empty files should yield gracefully without errors."""
    empty_file = tmp_path / "empty.json"
    empty_file.write_text("", encoding="utf-8")

    ingestor = EventIngestor(batch_size=10)
    batches = list(ingestor.stream_file(empty_file, run_id="test_run"))
    assert len(batches) == 1
    raw_events, rejects = batches[0]
    assert len(raw_events) == 0
    assert len(rejects) == 0


def test_ingest_malformed_json_syntax(tmp_path):
    """Malformed JSON syntax should route to syntax rejects."""
    bad_json = tmp_path / "bad.json"
    bad_json.write_text("{ unclosed_json: [123 ", encoding="utf-8")

    ingestor = EventIngestor(batch_size=10)
    batches = list(ingestor.stream_file(bad_json, run_id="test_run"))
    assert len(batches) == 1
    raw_events, rejects = batches[0]
    assert len(raw_events) == 0
    assert len(rejects) == 1
    assert rejects[0].rejection_code == RejectionCode.MALFORMED_RECORD.value


def test_ingest_jsonl_streaming(tmp_path):
    """Line-delimited JSONL streaming with mixed valid and broken lines."""
    jsonl_file = tmp_path / "stream.jsonl"
    lines = [
        json.dumps({"event_id": "e1", "order_id": "ORD-1"}),
        "INVALID_JSON_CORRUPTED_LINE",
        json.dumps({"event_id": "e2", "order_id": "ORD-2"}),
    ]
    jsonl_file.write_text("\n".join(lines), encoding="utf-8")

    ingestor = EventIngestor(batch_size=2)
    all_raw = []
    all_rej = []
    for raw_batch, rej_batch in ingestor.stream_file(jsonl_file, run_id="test_run"):
        all_raw.extend(raw_batch)
        all_rej.extend(rej_batch)

    assert len(all_raw) == 2
    assert len(all_rej) == 1
    assert all_rej[0].source_row_num == 2


def test_ingest_csv_format(tmp_path, valid_order_event):
    """CSV file ingestion should correctly parse headers and dict rows."""
    csv_file = tmp_path / "orders.csv"
    with open(csv_file, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(valid_order_event.keys()))
        writer.writeheader()
        writer.writerow(valid_order_event)

    ingestor = EventIngestor(batch_size=10)
    batches = list(ingestor.stream_file(csv_file, run_id="test_run"))
    raw_events, rejects = batches[0]
    assert len(raw_events) == 1
    assert len(rejects) == 0
    assert raw_events[0].raw_payload["order_id"] == "ORD-TEST-001"


def test_ingest_nonexistent_file():
    """Non-existent file should raise FileNotFoundError."""
    ingestor = EventIngestor()
    with pytest.raises(FileNotFoundError):
        list(ingestor.stream_file("non_existent_file.json", run_id="test_run"))
