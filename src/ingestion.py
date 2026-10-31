"""
Ingestion layer for CDC events.
Streams raw event data from CSV, JSON, and JSONL sources in memory-efficient chunks.
Handles malformed file structures, empty files, and captures file metadata and line coordinates.
"""

from pathlib import Path
from typing import Generator, List, Dict, Any, Tuple, Union
import csv
import json
import logging

from src.models import RawEvent, RejectedRecord, RejectionCode
from datetime import datetime, timezone

logger = logging.getLogger("cdc_pipeline.ingestion")

def get_utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class EventIngestor:
    """Streams raw event records from input files into RawEvent dataclass objects."""

    def __init__(self, batch_size: int = 1000):
        self.batch_size = batch_size

    def stream_file(
        self, file_path: Union[str, Path], run_id: str
    ) -> Generator[Tuple[List[RawEvent], List[RejectedRecord]], None, None]:
        """
        Generator that yields tuples of (valid_raw_events_batch, syntax_rejected_records_batch).
        Streams data in chunks of `self.batch_size`.
        """
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Input file not found at: {path}")

        file_size = path.stat().st_size
        if file_size == 0:
            logger.warning(f"Input file '{path.name}' is completely empty (0 bytes).")
            yield [], []
            return

        suffix = path.suffix.lower()
        if suffix == ".csv":
            yield from self._stream_csv(path, run_id)
        elif suffix == ".jsonl":
            yield from self._stream_jsonl(path, run_id)
        elif suffix == ".json":
            yield from self._stream_json(path, run_id)
        else:
            raise ValueError(f"Unsupported file format '{suffix}'. Supported: .csv, .json, .jsonl")

    def _stream_csv(
        self, path: Path, run_id: str
    ) -> Generator[Tuple[List[RawEvent], List[RejectedRecord]], None, None]:
        raw_batch: List[RawEvent] = []
        reject_batch: List[RejectedRecord] = []

        with open(path, "r", encoding="utf-8-sig", newline="") as f:
            reader = csv.DictReader(f)
            if reader.fieldnames is None:
                yield [], []
                return

            row_num = 1  # Header is row 1
            for row in reader:
                row_num += 1
                try:
                    # Strip whitespace from keys and values
                    clean_row = {}
                    for k, v in row.items():
                        if k is not None:
                            clean_k = k.strip()
                            clean_v = v.strip() if isinstance(v, str) else v
                            # Convert empty string to None if nullable, but keep raw representation
                            clean_row[clean_k] = clean_v
                    
                    event_id = clean_row.get("event_id")
                    raw_batch.append(
                        RawEvent(
                            source_file=path.name,
                            source_row_num=row_num,
                            raw_payload=clean_row,
                            raw_event_id=event_id,
                        )
                    )
                except Exception as e:
                    reject_batch.append(
                        RejectedRecord(
                            run_id=run_id,
                            source_file=path.name,
                            source_row_num=row_num,
                            raw_event_id=None,
                            rejection_code=RejectionCode.MALFORMED_RECORD.value,
                            rejection_reason=f"Failed to parse CSV row: {e}",
                            raw_payload=str(row),
                            failed_at=get_utc_now_iso(),
                        )
                    )

                if len(raw_batch) + len(reject_batch) >= self.batch_size:
                    yield raw_batch, reject_batch
                    raw_batch = []
                    reject_batch = []

            if raw_batch or reject_batch:
                yield raw_batch, reject_batch

    def _stream_jsonl(
        self, path: Path, run_id: str
    ) -> Generator[Tuple[List[RawEvent], List[RejectedRecord]], None, None]:
        raw_batch: List[RawEvent] = []
        reject_batch: List[RejectedRecord] = []

        with open(path, "r", encoding="utf-8") as f:
            row_num = 0
            for line in f:
                row_num += 1
                line_str = line.strip()
                if not line_str:
                    continue  # Ignore empty lines
                try:
                    payload = json.loads(line_str)
                    raw_event_id = payload.get("event_id") if isinstance(payload, dict) else None
                    raw_batch.append(
                        RawEvent(
                            source_file=path.name,
                            source_row_num=row_num,
                            raw_payload=payload,
                            raw_event_id=str(raw_event_id) if raw_event_id is not None else None,
                        )
                    )
                except Exception as e:
                    reject_batch.append(
                        RejectedRecord(
                            run_id=run_id,
                            source_file=path.name,
                            source_row_num=row_num,
                            raw_event_id=None,
                            rejection_code=RejectionCode.MALFORMED_RECORD.value,
                            rejection_reason=f"Invalid JSON line syntax: {e}",
                            raw_payload=line_str,
                            failed_at=get_utc_now_iso(),
                        )
                    )

                if len(raw_batch) + len(reject_batch) >= self.batch_size:
                    yield raw_batch, reject_batch
                    raw_batch = []
                    reject_batch = []

            if raw_batch or reject_batch:
                yield raw_batch, reject_batch

    def _stream_json(
        self, path: Path, run_id: str
    ) -> Generator[Tuple[List[RawEvent], List[RejectedRecord]], None, None]:
        raw_batch: List[RawEvent] = []
        reject_batch: List[RejectedRecord] = []

        with open(path, "r", encoding="utf-8") as f:
            content = f.read().strip()
            if not content:
                yield [], []
                return

            try:
                data = json.loads(content)
            except Exception as e:
                reject_batch.append(
                    RejectedRecord(
                        run_id=run_id,
                        source_file=path.name,
                        source_row_num=1,
                        raw_event_id=None,
                        rejection_code=RejectionCode.MALFORMED_RECORD.value,
                        rejection_reason=f"Failed to parse JSON file content: {e}",
                        raw_payload=content[:200] + ("..." if len(content) > 200 else ""),
                        failed_at=get_utc_now_iso(),
                    )
                )
                yield [], reject_batch
                return

        if not isinstance(data, list):
            reject_batch.append(
                RejectedRecord(
                    run_id=run_id,
                    source_file=path.name,
                    source_row_num=1,
                    raw_event_id=None,
                    rejection_code=RejectionCode.MALFORMED_RECORD.value,
                    rejection_reason=f"Expected JSON root array of objects, got {type(data).__name__}",
                    raw_payload=str(data)[:200],
                    failed_at=datetime.utcnow().isoformat() + "Z",
                )
            )
            yield [], reject_batch
            return

        row_num = 0
        for item in data:
            row_num += 1
            raw_event_id = item.get("event_id") if isinstance(item, dict) else None
            raw_batch.append(
                RawEvent(
                    source_file=path.name,
                    source_row_num=row_num,
                    raw_payload=item,
                    raw_event_id=str(raw_event_id) if raw_event_id is not None else None,
                )
            )

            if len(raw_batch) >= self.batch_size:
                yield raw_batch, []
                raw_batch = []

        if raw_batch:
            yield raw_batch, []
