"""
Observability and telemetry module for the CDC Pipeline.
Provides structured logging, operational metrics tracking, runtime timers,
and automated run summaries for auditability.
"""

from datetime import datetime, timezone
import json
import logging
import sys
import uuid
from typing import Optional, Dict, Any

from src.models import PipelineRun, RunStatus


def get_utc_now() -> datetime:
    """Returns current timezone-aware UTC datetime."""
    return datetime.now(timezone.utc)


def setup_logger(name: str = "cdc_pipeline", level: str = "INFO") -> logging.Logger:
    """Configures structured console logging."""
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            fmt="[%(asctime)s] [%(levelname)-7s] [%(name)s] %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%SZ",
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)

    return logger


class RunTracker:
    """Tracks operational statistics and health of a single pipeline execution."""

    def __init__(self, source_file: str, run_id: Optional[str] = None):
        now = get_utc_now()
        self.run_id = run_id or f"run_{now.strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"
        self.source_file = source_file
        self.start_dt = now
        self.end_dt: Optional[datetime] = None

        self.records_read = 0
        self.records_accepted = 0
        self.records_rejected = 0
        self.records_duplicate = 0
        self.inserts_applied = 0
        self.updates_applied = 0
        self.deletes_applied = 0
        self.out_of_order_count = 0
        self.status = RunStatus.RUNNING.value
        self.error_message: Optional[str] = None

    def mark_success(self):
        """Marks run as finished successfully or partially successful if rejects exist."""
        self.end_dt = get_utc_now()
        if self.records_rejected > 0:
            self.status = RunStatus.PARTIAL_SUCCESS.value
        else:
            self.status = RunStatus.SUCCESS.value

    def mark_failed(self, error: Exception):
        """Marks run as failed with error context."""
        self.end_dt = get_utc_now()
        self.status = RunStatus.FAILED.value
        self.error_message = str(error)

    @property
    def duration_seconds(self) -> float:
        """Returns total elapsed execution time in seconds."""
        end = self.end_dt or get_utc_now()
        return round((end - self.start_dt).total_seconds(), 4)

    def to_model(self) -> PipelineRun:
        """Serializes tracker into a PipelineRun dataclass instance."""
        return PipelineRun(
            run_id=self.run_id,
            start_time=self.start_dt.isoformat(),
            end_time=self.end_dt.isoformat() if self.end_dt else None,
            status=self.status,
            source_file=self.source_file,
            records_read=self.records_read,
            records_accepted=self.records_accepted,
            records_rejected=self.records_rejected,
            records_duplicate=self.records_duplicate,
            inserts_applied=self.inserts_applied,
            updates_applied=self.updates_applied,
            deletes_applied=self.deletes_applied,
            out_of_order_count=self.out_of_order_count,
            duration_seconds=self.duration_seconds,
            error_message=self.error_message,
        )

    def generate_summary_text(self) -> str:
        """Generates a clean ASCII summary card of the run metrics."""
        m = self.to_model()
        rate = round(m.records_read / max(m.duration_seconds, 0.001), 2)
        summary = f"""
================================================================================
                    CDC PIPELINE EXECUTION SUMMARY
================================================================================
 Run ID              : {m.run_id}
 Execution Status    : {m.status}
 Source File         : {m.source_file}
 Start Time (UTC)    : {m.start_time}
 End Time (UTC)      : {m.end_time or 'In Progress'}
 Elapsed Duration    : {m.duration_seconds} sec ({rate:,} rec/sec)
--------------------------------------------------------------------------------
 Ingestion Metrics:
   - Total Records Read     : {m.records_read:,}
   - Accepted Records       : {m.records_accepted:,}
   - Quarantined Rejects    : {m.records_rejected:,}
   - Duplicate Events (Dedup): {m.records_duplicate:,}
 Target Mutations:
   - INSERT Operations      : {m.inserts_applied:,}
   - UPDATE Operations      : {m.updates_applied:,}
   - DELETE (Tombstones)    : {m.deletes_applied:,}
 CDC Edge Cases:
   - Out-of-Order Events    : {m.out_of_order_count:,}
--------------------------------------------------------------------------------
 Error Message       : {m.error_message or 'None'}
================================================================================
"""
        return summary.strip()
