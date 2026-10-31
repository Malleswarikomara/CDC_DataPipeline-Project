"""
Core Change Data Capture (CDC) processing engine.
Handles INSERT, UPDATE, and DELETE semantics, deterministic deduplication,
out-of-order sequencing, deletion tombstones, and audit history preservation.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Optional, List, Tuple
import json
import logging
import sqlite3

from src.models import (
    ValidatedEvent,
    RejectedRecord,
    OrderCurrentState,
    EventHistoryRecord,
    RejectionCode,
    EventType,
)
from src.config import PipelineConfig, get_default_config

logger = logging.getLogger("cdc_pipeline.processor")

def get_utc_now_iso() -> str:
    """Returns current UTC ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


class CDCProcessor:
    """Stateful processor that applies validated CDC events to current_state and event_history."""

    def __init__(self, config: Optional[PipelineConfig] = None):
        self.config = config or get_default_config()
        self.status_ranks_map = dict(self.config.status_ranks)
        self.terminal_statuses = set(self.config.terminal_statuses)

    def is_valid_status_transition(self, current_status: str, new_status: str) -> bool:
        """
        Validates state transitions:
        - Terminal states ('DELIVERED', 'CANCELLED') cannot change to any other state.
        - Active states can transition to equal or higher ranks, or to terminal states.
        - Regressing backwards in rank (e.g. SHIPPED -> PENDING) is prohibited.
        """
        if current_status == new_status:
            return True
        if current_status in self.terminal_statuses:
            return False
        curr_rank = self.status_ranks_map.get(current_status, 0)
        new_rank = self.status_ranks_map.get(new_status, 0)
        return new_rank >= curr_rank

    def process_event(
        self,
        event: ValidatedEvent,
        conn: sqlite3.Connection,
        run_id: str,
        processed_in_batch: set,
    ) -> Tuple[str, Optional[RejectedRecord]]:
        """
        Processes a single validated event inside an active database transaction.
        
        Returns:
            Tuple[status, Optional[RejectedRecord]]
            where status is one of: 'APPLIED_INSERT', 'APPLIED_UPDATE', 'APPLIED_DELETE',
                                    'OUT_OF_ORDER_HISTORY_ONLY', 'DUPLICATE', 'REJECTED'
        """
        now_iso = get_utc_now_iso()

        # 1. Deduplication check: in-batch set or in event_history table
        if event.event_id in processed_in_batch:
            logger.info(f"Duplicate event_id '{event.event_id}' detected within current batch. Skipping.")
            return "DUPLICATE", None

        cur = conn.execute("SELECT 1 FROM event_history WHERE event_id = ? LIMIT 1;", (event.event_id,))
        if cur.fetchone() is not None:
            logger.info(f"Duplicate event_id '{event.event_id}' already exists in event_history. Skipping.")
            return "DUPLICATE", None

        # Fetch existing state for this order_id
        cur = conn.execute("SELECT * FROM current_state WHERE order_id = ?;", (event.order_id,))
        existing_row = cur.fetchone()
        existing = dict(existing_row) if existing_row else None

        # 2. Case: Entity already exists in current_state
        if existing is not None:
            last_seq = existing["last_sequence"]

            # Out-of-order / Late-arriving event check
            if event.source_sequence <= last_seq:
                logger.warning(
                    f"Out-of-order event detected for order_id '{event.order_id}'. "
                    f"Incoming sequence {event.source_sequence} <= current sequence {last_seq}. "
                    f"Preserving in audit history without mutating current_state."
                )
                # History preservation rule: Append to event_history, do not mutate current_state
                conn.execute(
                    """
                    INSERT INTO event_history (
                        event_id, order_id, event_type, source_sequence,
                        event_timestamp, payload_before, payload_after, run_id, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        event.event_id,
                        event.order_id,
                        event.event_type,
                        event.source_sequence,
                        event.event_timestamp,
                        json.dumps(existing),
                        json.dumps(event.to_dict()),
                        run_id,
                        now_iso,
                    ),
                )
                processed_in_batch.add(event.event_id)
                return "OUT_OF_ORDER_HISTORY_ONLY", None

            # Newer sequence arriving
            # Check if existing record is currently tombstoned (is_deleted == 1)
            if existing["is_deleted"] == 1:
                if event.event_type == EventType.UPDATE.value:
                    rej = RejectedRecord(
                        run_id=run_id,
                        source_file=event.source_file,
                        source_row_num=event.source_row_num,
                        raw_event_id=event.event_id,
                        rejection_code=RejectionCode.INVALID_STATUS_TRANSITION.value,
                        rejection_reason=f"Cannot apply UPDATE to tombstoned/deleted order '{event.order_id}'",
                        raw_payload=json.dumps(event.to_dict()),
                        failed_at=now_iso,
                    )
                    return "REJECTED", rej

                elif event.event_type == EventType.DELETE.value:
                    rej = RejectedRecord(
                        run_id=run_id,
                        source_file=event.source_file,
                        source_row_num=event.source_row_num,
                        raw_event_id=event.event_id,
                        rejection_code=RejectionCode.INVALID_STATUS_TRANSITION.value,
                        rejection_reason=f"Order '{event.order_id}' is already tombstoned/deleted",
                        raw_payload=json.dumps(event.to_dict()),
                        failed_at=now_iso,
                    )
                    return "REJECTED", rej

                elif event.event_type == EventType.INSERT.value:
                    # Entity reincarnation / reactivation
                    logger.info(f"Re-activating previously tombstoned order '{event.order_id}' via new INSERT")
                    new_version = existing["record_version"] + 1
                    new_hash = event.compute_payload_hash()
                    conn.execute(
                        """
                        UPDATE current_state SET
                            customer_id = ?,
                            order_status = ?,
                            total_amount = ?,
                            currency = ?,
                            shipping_address = ?,
                            item_count = ?,
                            updated_at = ?,
                            last_event_id = ?,
                            last_sequence = ?,
                            is_deleted = 0,
                            deleted_at = NULL,
                            record_version = ?,
                            record_hash = ?
                        WHERE order_id = ?;
                        """,
                        (
                            event.customer_id,
                            event.order_status,
                            event.total_amount,
                            event.currency,
                            event.shipping_address,
                            event.item_count,
                            event.event_timestamp,
                            event.event_id,
                            event.source_sequence,
                            new_version,
                            new_hash,
                            event.order_id,
                        ),
                    )
                    conn.execute(
                        """
                        INSERT INTO event_history (
                            event_id, order_id, event_type, source_sequence,
                            event_timestamp, payload_before, payload_after, run_id, ingested_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                        """,
                        (
                            event.event_id,
                            event.order_id,
                            event.event_type,
                            event.source_sequence,
                            event.event_timestamp,
                            json.dumps(existing),
                            json.dumps(event.to_dict()),
                            run_id,
                            now_iso,
                        ),
                    )
                    processed_in_batch.add(event.event_id)
                    return "APPLIED_INSERT", None

            # Existing record is active (is_deleted == 0)
            if event.event_type == EventType.DELETE.value:
                # Apply Tombstone
                new_version = existing["record_version"] + 1
                conn.execute(
                    """
                    UPDATE current_state SET
                        is_deleted = 1,
                        deleted_at = ?,
                        updated_at = ?,
                        last_event_id = ?,
                        last_sequence = ?,
                        record_version = ?
                    WHERE order_id = ?;
                    """,
                    (
                        event.event_timestamp,
                        event.event_timestamp,
                        event.event_id,
                        event.source_sequence,
                        new_version,
                        event.order_id,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO event_history (
                        event_id, order_id, event_type, source_sequence,
                        event_timestamp, payload_before, payload_after, run_id, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        event.event_id,
                        event.order_id,
                        event.event_type,
                        event.source_sequence,
                        event.event_timestamp,
                        json.dumps(existing),
                        json.dumps({"tombstone": True, "deleted_at": event.event_timestamp}),
                        run_id,
                        now_iso,
                    ),
                )
                processed_in_batch.add(event.event_id)
                return "APPLIED_DELETE", None

            elif event.event_type in (EventType.UPDATE.value, EventType.INSERT.value):
                # Check status transition validity if status changed
                curr_status = existing["order_status"]
                new_status = event.order_status
                if not self.is_valid_status_transition(curr_status, new_status):
                    rej = RejectedRecord(
                        run_id=run_id,
                        source_file=event.source_file,
                        source_row_num=event.source_row_num,
                        raw_event_id=event.event_id,
                        rejection_code=RejectionCode.INVALID_STATUS_TRANSITION.value,
                        rejection_reason=f"Illegal status transition from '{curr_status}' to '{new_status}' for order '{event.order_id}'",
                        raw_payload=json.dumps(event.to_dict()),
                        failed_at=now_iso,
                    )
                    return "REJECTED", rej

                # Apply update
                new_version = existing["record_version"] + 1
                new_hash = event.compute_payload_hash()
                conn.execute(
                    """
                    UPDATE current_state SET
                        customer_id = ?,
                        order_status = ?,
                        total_amount = ?,
                        currency = ?,
                        shipping_address = ?,
                        item_count = ?,
                        updated_at = ?,
                        last_event_id = ?,
                        last_sequence = ?,
                        record_version = ?,
                        record_hash = ?
                    WHERE order_id = ?;
                    """,
                    (
                        event.customer_id,
                        event.order_status,
                        event.total_amount,
                        event.currency,
                        event.shipping_address,
                        event.item_count,
                        event.event_timestamp,
                        event.event_id,
                        event.source_sequence,
                        new_version,
                        new_hash,
                        event.order_id,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO event_history (
                        event_id, order_id, event_type, source_sequence,
                        event_timestamp, payload_before, payload_after, run_id, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
                    """,
                    (
                        event.event_id,
                        event.order_id,
                        event.event_type,
                        event.source_sequence,
                        event.event_timestamp,
                        json.dumps(existing),
                        json.dumps(event.to_dict()),
                        run_id,
                        now_iso,
                    ),
                )
                processed_in_batch.add(event.event_id)
                status_code = "APPLIED_UPDATE" if event.event_type == EventType.UPDATE.value else "APPLIED_INSERT"
                return status_code, None

        # 3. Case: Entity does NOT exist in current_state
        else:
            if event.event_type == EventType.DELETE.value:
                # Delete attempted on non-existent entity
                rej = RejectedRecord(
                    run_id=run_id,
                    source_file=event.source_file,
                    source_row_num=event.source_row_num,
                    raw_event_id=event.event_id,
                    rejection_code=RejectionCode.DELETE_NON_EXISTENT_ENTITY.value,
                    rejection_reason=f"Cannot DELETE non-existent order '{event.order_id}'",
                    raw_payload=json.dumps(event.to_dict()),
                    failed_at=now_iso,
                )
                return "REJECTED", rej

            elif event.event_type == EventType.INSERT.value:
                # Standard Initial INSERT
                new_hash = event.compute_payload_hash()
                conn.execute(
                    """
                    INSERT INTO current_state (
                        order_id, customer_id, order_status, total_amount,
                        currency, shipping_address, item_count, created_at,
                        updated_at, last_event_id, last_sequence, is_deleted,
                        deleted_at, record_version, record_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, 1, ?);
                    """,
                    (
                        event.order_id,
                        event.customer_id,
                        event.order_status,
                        event.total_amount,
                        event.currency,
                        event.shipping_address,
                        event.item_count,
                        event.event_timestamp,
                        event.event_timestamp,
                        event.event_id,
                        event.source_sequence,
                        new_hash,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO event_history (
                        event_id, order_id, event_type, source_sequence,
                        event_timestamp, payload_before, payload_after, run_id, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?);
                    """,
                    (
                        event.event_id,
                        event.order_id,
                        event.event_type,
                        event.source_sequence,
                        event.event_timestamp,
                        json.dumps(event.to_dict()),
                        run_id,
                        now_iso,
                    ),
                )
                processed_in_batch.add(event.event_id)
                return "APPLIED_INSERT", None

            elif event.event_type == EventType.UPDATE.value:
                # Out-of-order initial creation via UPDATE
                logger.warning(
                    f"Received UPDATE event for previously unseen order '{event.order_id}'. "
                    f"Creating initial state from UPDATE payload."
                )
                new_hash = event.compute_payload_hash()
                conn.execute(
                    """
                    INSERT INTO current_state (
                        order_id, customer_id, order_status, total_amount,
                        currency, shipping_address, item_count, created_at,
                        updated_at, last_event_id, last_sequence, is_deleted,
                        deleted_at, record_version, record_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, NULL, 1, ?);
                    """,
                    (
                        event.order_id,
                        event.customer_id,
                        event.order_status,
                        event.total_amount,
                        event.currency,
                        event.shipping_address,
                        event.item_count,
                        event.event_timestamp,
                        event.event_timestamp,
                        event.event_id,
                        event.source_sequence,
                        new_hash,
                    ),
                )
                conn.execute(
                    """
                    INSERT INTO event_history (
                        event_id, order_id, event_type, source_sequence,
                        event_timestamp, payload_before, payload_after, run_id, ingested_at
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?);
                    """,
                    (
                        event.event_id,
                        event.order_id,
                        event.event_type,
                        event.source_sequence,
                        event.event_timestamp,
                        json.dumps(event.to_dict()),
                        run_id,
                        now_iso,
                    ),
                )
                processed_in_batch.add(event.event_id)
                return "APPLIED_UPDATE", None

        return "UNKNOWN", None
