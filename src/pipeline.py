"""
Pipeline orchestration layer.
Ties together ingestion, validation, CDC processing, persistence, observability,
and error handling into a single transactional execution pipeline.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict, Any, Union
import json
import logging

from src.config import PipelineConfig, get_default_config
from src.database import DatabaseManager
from src.ingestion import EventIngestor
from src.validation import EventValidator
from src.cdc_processor import CDCProcessor
from src.monitoring import RunTracker, setup_logger
from src.models import ValidatedEvent, RejectedRecord


def get_utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class CDCPipeline:
    """End-to-end Change Data Capture ingestion, validation, and loading pipeline."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or get_default_config()
        self.db = DatabaseManager(self.config.db_path)
        self.validator = EventValidator(config=self.config)
        self.processor = CDCProcessor(config=self.config)
        self.ingestor = EventIngestor(batch_size=self.config.batch_size)
        self.logger = setup_logger("cdc_pipeline", level=self.config.log_level)

    def run(self, file_path: Union[str, Path], run_id: Optional[str] = None) -> RunTracker:
        """
        Executes the CDC pipeline for a given input file.
        
        Args:
            file_path: Path to the CSV, JSON, or JSONL input file.
            run_id: Optional explicit run identifier for backfill or rerun tracking.
            
        Returns:
            RunTracker instance containing final execution telemetry.
        """
        path = Path(file_path)
        tracker = RunTracker(source_file=path.name, run_id=run_id)
        self.logger.info(f"Starting CDC Pipeline execution: Run ID = '{tracker.run_id}' for file: '{path.name}'")

        # Persist initial running state
        self.db.save_pipeline_run(tracker.to_model().to_dict())

        # Set to track deduplication within this execution across batches
        processed_event_ids_in_run = set()

        try:
            for raw_events, syntax_rejects in self.ingestor.stream_file(path, tracker.run_id):
                with self.db.transaction() as conn:
                    # 1. Handle syntax-level ingestion rejects
                    for rej in syntax_rejects:
                        tracker.records_read += 1
                        tracker.records_rejected += 1
                        self._persist_rejection(conn, rej)
                        self._persist_source_event(
                            conn,
                            run_id=tracker.run_id,
                            source_file=path.name,
                            source_row_num=rej.source_row_num,
                            raw_event_id=None,
                            raw_payload=rej.raw_payload,
                            status="REJECTED",
                        )

                    # 2. Process raw records
                    for raw in raw_events:
                        tracker.records_read += 1
                        raw_payload_str = (
                            json.dumps(raw.raw_payload)
                            if isinstance(raw.raw_payload, (dict, list))
                            else str(raw.raw_payload)
                        )

                        # Semantic validation
                        validation_res = self.validator.validate(
                            raw_payload=raw.raw_payload,
                            source_file=raw.source_file,
                            source_row_num=raw.source_row_num,
                            run_id=tracker.run_id,
                        )

                        if isinstance(validation_res, RejectedRecord):
                            # Record validation failure
                            tracker.records_rejected += 1
                            self._persist_rejection(conn, validation_res)
                            self._persist_source_event(
                                conn,
                                run_id=tracker.run_id,
                                source_file=raw.source_file,
                                source_row_num=raw.source_row_num,
                                raw_event_id=raw.raw_event_id,
                                raw_payload=raw_payload_str,
                                status="REJECTED",
                            )
                            continue

                        # Record passed validation -> ValidatedEvent
                        validated_event: ValidatedEvent = validation_res

                        # Apply CDC logic
                        status, proc_rej = self.processor.process_event(
                            event=validated_event,
                            conn=conn,
                            run_id=tracker.run_id,
                            processed_in_batch=processed_event_ids_in_run,
                        )

                        if status == "DUPLICATE":
                            tracker.records_duplicate += 1
                            self._persist_source_event(
                                conn,
                                run_id=tracker.run_id,
                                source_file=raw.source_file,
                                source_row_num=raw.source_row_num,
                                raw_event_id=validated_event.event_id,
                                raw_payload=raw_payload_str,
                                status="DUPLICATE",
                            )

                        elif status == "REJECTED":
                            tracker.records_rejected += 1
                            if proc_rej:
                                self._persist_rejection(conn, proc_rej)
                            self._persist_source_event(
                                conn,
                                run_id=tracker.run_id,
                                source_file=raw.source_file,
                                source_row_num=raw.source_row_num,
                                raw_event_id=validated_event.event_id,
                                raw_payload=raw_payload_str,
                                status="REJECTED",
                            )

                        else:
                            # Accepted and applied to history & state
                            tracker.records_accepted += 1
                            self._persist_source_event(
                                conn,
                                run_id=tracker.run_id,
                                source_file=raw.source_file,
                                source_row_num=raw.source_row_num,
                                raw_event_id=validated_event.event_id,
                                raw_payload=raw_payload_str,
                                status="ACCEPTED",
                            )

                            if status == "APPLIED_INSERT":
                                tracker.inserts_applied += 1
                            elif status == "APPLIED_UPDATE":
                                tracker.updates_applied += 1
                            elif status == "APPLIED_DELETE":
                                tracker.deletes_applied += 1
                            elif status == "OUT_OF_ORDER_HISTORY_ONLY":
                                tracker.out_of_order_count += 1

            # Run completed successfully (or partial with rejects)
            tracker.mark_success()
            self.logger.info(f"CDC Pipeline completed with status: {tracker.status}")

        except Exception as e:
            tracker.mark_failed(e)
            self.logger.error(f"CDC Pipeline failed during execution: {e}", exc_info=True)
            raise
        finally:
            # Persist final telemetry
            self.db.save_pipeline_run(tracker.to_model().to_dict())
            self.logger.info("\n" + tracker.generate_summary_text())

        return tracker

    def _persist_rejection(self, conn, rej: RejectedRecord):
        """Helper to insert quarantined record into rejected_events."""
        conn.execute(
            """
            INSERT INTO rejected_events (
                run_id, source_file, source_row_num, raw_event_id,
                rejection_code, rejection_reason, raw_payload, failed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?);
            """,
            (
                rej.run_id,
                rej.source_file,
                rej.source_row_num,
                rej.raw_event_id,
                rej.rejection_code,
                rej.rejection_reason,
                rej.raw_payload,
                rej.failed_at,
            ),
        )

    def _persist_source_event(
        self,
        conn,
        run_id: str,
        source_file: str,
        source_row_num: int,
        raw_event_id: Optional[str],
        raw_payload: str,
        status: str,
    ):
        """Helper to insert staged raw event into source_events."""
        now_iso = get_utc_now_iso()
        conn.execute(
            """
            INSERT INTO source_events (
                run_id, source_file, source_row_num, raw_event_id,
                raw_payload, received_at, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?);
            """,
            (
                run_id,
                source_file,
                source_row_num,
                raw_event_id,
                raw_payload,
                now_iso,
                status,
            ),
        )
