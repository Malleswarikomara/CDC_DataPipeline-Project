"""
Validation and quarantine layer for CDC events.
Performs strict schema validation, type parsing, boundary checks, and business rule enforcement.
Routes non-conforming records to RejectedRecord quarantine objects with deterministic failure codes.
"""

from datetime import datetime, timezone
from typing import Dict, Any, Tuple, Optional, Union
import logging
import json

from src.models import (
    ValidatedEvent,
    RejectedRecord,
    RejectionCode,
    EventType,
    OrderStatus,
)
from src.config import PipelineConfig, get_default_config

logger = logging.getLogger("cdc_pipeline.validation")

def get_utc_now_iso() -> str:
    """Returns current UTC ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()

# Expected schema fields
EXPECTED_FIELDS = {
    "event_id",
    "event_type",
    "event_timestamp",
    "source_sequence",
    "order_id",
    "customer_id",
    "order_status",
    "total_amount",
    "currency",
    "shipping_address",
    "item_count",
}

REQUIRED_NON_NULL_FIELDS = {
    "event_id",
    "event_type",
    "event_timestamp",
    "source_sequence",
    "order_id",
    "customer_id",
    "order_status",
    "total_amount",
    "currency",
    "item_count",
}


class EventValidator:
    """Validates raw event dictionaries against schema, data types, and business constraints."""

    def __init__(self, config: Optional[PipelineConfig] = None, strict_schema: bool = True):
        self.config = config or get_default_config()
        self.strict_schema = strict_schema

    def validate(
        self,
        raw_payload: Any,
        source_file: str,
        source_row_num: int,
        run_id: str,
    ) -> Union[ValidatedEvent, RejectedRecord]:
        """
        Validates an event payload.
        Returns a ValidatedEvent if valid, otherwise returns a RejectedRecord for quarantine.
        """
        now_iso = get_utc_now_iso()
        raw_payload_str = json.dumps(raw_payload) if isinstance(raw_payload, (dict, list)) else str(raw_payload)

        # 1. Structural check: Must be a dictionary
        if not isinstance(raw_payload, dict):
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=None,
                rejection_code=RejectionCode.MALFORMED_RECORD.value,
                rejection_reason=f"Payload is not a valid JSON/dict structure; received type: {type(raw_payload).__name__}",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        raw_event_id = str(raw_payload.get("event_id", "")).strip() or None

        # 2. Strict Schema Check: Detect unexpected schema drift
        payload_keys = set(raw_payload.keys())
        if self.strict_schema:
            extra_keys = payload_keys - EXPECTED_FIELDS
            if extra_keys:
                return RejectedRecord(
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=source_row_num,
                    raw_event_id=raw_event_id,
                    rejection_code=RejectionCode.UNEXPECTED_SCHEMA_CHANGE.value,
                    rejection_reason=f"Payload contains unexpected schema columns: {sorted(list(extra_keys))}",
                    raw_payload=raw_payload_str,
                    failed_at=now_iso,
                )

        # 3. Missing Required Fields Check
        for field in REQUIRED_NON_NULL_FIELDS:
            if field not in raw_payload or raw_payload[field] is None:
                return RejectedRecord(
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=source_row_num,
                    raw_event_id=raw_event_id,
                    rejection_code=RejectionCode.MISSING_REQUIRED_FIELD.value,
                    rejection_reason=f"Missing mandatory non-null field: '{field}'",
                    raw_payload=raw_payload_str,
                    failed_at=now_iso,
                )
            if isinstance(raw_payload[field], str) and raw_payload[field].strip() == "":
                return RejectedRecord(
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=source_row_num,
                    raw_event_id=raw_event_id,
                    rejection_code=RejectionCode.MISSING_REQUIRED_FIELD.value,
                    rejection_reason=f"Mandatory field '{field}' cannot be empty string",
                    raw_payload=raw_payload_str,
                    failed_at=now_iso,
                )

        # 4. Event Type Validation
        event_type_str = str(raw_payload["event_type"]).upper().strip()
        if event_type_str not in self.config.allowed_event_types:
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.INVALID_EVENT_TYPE.value,
                rejection_reason=f"Unsupported event_type: '{raw_payload['event_type']}'. Expected: {self.config.allowed_event_types}",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        # 5. Timestamp Parsing (ISO-8601 validation)
        ts_raw = str(raw_payload["event_timestamp"]).strip()
        try:
            clean_ts = ts_raw.replace("Z", "+00:00") if ts_raw.endswith("Z") else ts_raw
            parsed_dt = datetime.fromisoformat(clean_ts)
            formatted_ts = ts_raw
        except Exception as e:
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.INVALID_TIMESTAMP.value,
                rejection_reason=f"Malformed ISO-8601 timestamp: '{ts_raw}' ({e})",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        # 6. Sequence Number Validation
        try:
            sequence_val = int(raw_payload["source_sequence"])
            if sequence_val < 1:
                return RejectedRecord(
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=source_row_num,
                    raw_event_id=raw_event_id,
                    rejection_code=RejectionCode.INVALID_SEQUENCE.value,
                    rejection_reason=f"source_sequence must be a positive integer >= 1; got {sequence_val}",
                    raw_payload=raw_payload_str,
                    failed_at=now_iso,
                )
        except (ValueError, TypeError):
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.INVALID_SEQUENCE.value,
                rejection_reason=f"source_sequence must be numeric integer; got '{raw_payload['source_sequence']}'",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        # 7. Order Status Validation
        status_str = str(raw_payload["order_status"]).upper().strip()
        if status_str not in self.config.allowed_statuses:
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.INVALID_STATUS.value,
                rejection_reason=f"Unsupported order_status: '{raw_payload['order_status']}'. Expected: {self.config.allowed_statuses}",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        # 8. Total Amount Validation
        try:
            amount_val = float(raw_payload["total_amount"])
            if amount_val < 0.0:
                return RejectedRecord(
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=source_row_num,
                    raw_event_id=raw_event_id,
                    rejection_code=RejectionCode.NEGATIVE_AMOUNT.value,
                    rejection_reason=f"total_amount cannot be negative; got {amount_val}",
                    raw_payload=raw_payload_str,
                    failed_at=now_iso,
                )
        except (ValueError, TypeError):
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.INVALID_DATA_TYPE.value,
                rejection_reason=f"total_amount must be a valid numeric float; got '{raw_payload['total_amount']}'",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        # 9. Currency Validation
        curr_str = str(raw_payload["currency"]).upper().strip()
        if curr_str not in self.config.allowed_currencies:
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.INVALID_DATA_TYPE.value,
                rejection_reason=f"currency '{curr_str}' not in allowed currencies: {self.config.allowed_currencies}",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        # 10. Item Count Validation
        try:
            item_cnt = int(raw_payload["item_count"])
            if item_cnt < 0:
                return RejectedRecord(
                    run_id=run_id,
                    source_file=source_file,
                    source_row_num=source_row_num,
                    raw_event_id=raw_event_id,
                    rejection_code=RejectionCode.INVALID_DATA_TYPE.value,
                    rejection_reason=f"item_count cannot be negative; got {item_cnt}",
                    raw_payload=raw_payload_str,
                    failed_at=now_iso,
                )
        except (ValueError, TypeError):
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.INVALID_DATA_TYPE.value,
                rejection_reason=f"item_count must be integer; got '{raw_payload['item_count']}'",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        # 11. Customer ID and Order ID Validation
        order_id_clean = str(raw_payload["order_id"]).strip()
        customer_id_clean = str(raw_payload["customer_id"]).strip()
        if not order_id_clean or not customer_id_clean:
            return RejectedRecord(
                run_id=run_id,
                source_file=source_file,
                source_row_num=source_row_num,
                raw_event_id=raw_event_id,
                rejection_code=RejectionCode.MISSING_REQUIRED_FIELD.value,
                rejection_reason="order_id and customer_id must be non-empty strings",
                raw_payload=raw_payload_str,
                failed_at=now_iso,
            )

        shipping_addr = raw_payload.get("shipping_address")
        if shipping_addr is not None:
            shipping_addr = str(shipping_addr).strip()

        # If everything passes, return clean ValidatedEvent
        return ValidatedEvent(
            event_id=raw_event_id,
            event_type=event_type_str,
            event_timestamp=formatted_ts,
            source_sequence=sequence_val,
            order_id=order_id_clean,
            customer_id=customer_id_clean,
            order_status=status_str,
            total_amount=round(amount_val, 2),
            currency=curr_str,
            shipping_address=shipping_addr,
            item_count=item_cnt,
            source_file=source_file,
            source_row_num=source_row_num,
        )
