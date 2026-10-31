"""
Data models and type definitions for the CDC Pipeline.
Includes domain enums, event definitions, audit records, and operational entities.
"""

from dataclasses import dataclass, asdict
from enum import Enum
from typing import Optional, Dict, Any
import hashlib
import json


class EventType(str, Enum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class OrderStatus(str, Enum):
    PENDING = "PENDING"
    CONFIRMED = "CONFIRMED"
    SHIPPED = "SHIPPED"
    DELIVERED = "DELIVERED"
    CANCELLED = "CANCELLED"


class RejectionCode(str, Enum):
    MALFORMED_RECORD = "MALFORMED_RECORD"
    MISSING_REQUIRED_FIELD = "MISSING_REQUIRED_FIELD"
    INVALID_DATA_TYPE = "INVALID_DATA_TYPE"
    INVALID_EVENT_TYPE = "INVALID_EVENT_TYPE"
    INVALID_STATUS = "INVALID_STATUS"
    NEGATIVE_AMOUNT = "NEGATIVE_AMOUNT"
    INVALID_TIMESTAMP = "INVALID_TIMESTAMP"
    INVALID_SEQUENCE = "INVALID_SEQUENCE"
    INVALID_STATUS_TRANSITION = "INVALID_STATUS_TRANSITION"
    DELETE_NON_EXISTENT_ENTITY = "DELETE_NON_EXISTENT_ENTITY"
    UNEXPECTED_SCHEMA_CHANGE = "UNEXPECTED_SCHEMA_CHANGE"
    INVALID_REFERENCE_KEY = "INVALID_REFERENCE_KEY"


class RunStatus(str, Enum):
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    FAILED = "FAILED"
    PARTIAL_SUCCESS = "PARTIAL_SUCCESS"


@dataclass
class RawEvent:
    """Represents an unprocessed raw incoming event from source storage."""
    source_file: str
    source_row_num: int
    raw_payload: Dict[str, Any]
    raw_event_id: Optional[str] = None


@dataclass
class ValidatedEvent:
    """Represents an incoming event that passed schema and semantic validation."""
    event_id: str
    event_type: str
    event_timestamp: str
    source_sequence: int
    order_id: str
    customer_id: str
    order_status: str
    total_amount: float
    currency: str
    shipping_address: Optional[str]
    item_count: int
    source_file: str
    source_row_num: int

    def compute_payload_hash(self) -> str:
        """Computes deterministic SHA-256 hash of core domain payload."""
        payload = {
            "order_id": self.order_id,
            "customer_id": self.customer_id,
            "order_status": self.order_status,
            "total_amount": f"{self.total_amount:.2f}",
            "currency": self.currency,
            "shipping_address": self.shipping_address or "",
            "item_count": self.item_count,
        }
        serialized = json.dumps(payload, sort_keys=True)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class OrderCurrentState:
    """Represents the current, canonical state of an order entity."""
    order_id: str
    customer_id: str
    order_status: str
    total_amount: float
    currency: str
    shipping_address: Optional[str]
    item_count: int
    created_at: str
    updated_at: str
    last_event_id: str
    last_sequence: int
    is_deleted: int  # 0 = active, 1 = tombstone
    deleted_at: Optional[str]
    record_version: int
    record_hash: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class EventHistoryRecord:
    """Represents an immutable historical audit record of an accepted event."""
    event_id: str
    order_id: str
    event_type: str
    source_sequence: int
    event_timestamp: str
    payload_before: Optional[str]
    payload_after: Optional[str]
    run_id: str
    ingested_at: str
    history_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class RejectedRecord:
    """Represents a quarantined invalid event with rich diagnostic context."""
    run_id: str
    source_file: str
    source_row_num: int
    raw_event_id: Optional[str]
    rejection_code: str
    rejection_reason: str
    raw_payload: str
    failed_at: str
    quarantine_id: Optional[int] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineRun:
    """Represents the operational execution metadata and metrics for a run."""
    run_id: str
    start_time: str
    end_time: Optional[str]
    status: str
    source_file: str
    records_read: int = 0
    records_accepted: int = 0
    records_rejected: int = 0
    records_duplicate: int = 0
    inserts_applied: int = 0
    updates_applied: int = 0
    deletes_applied: int = 0
    out_of_order_count: int = 0
    duration_seconds: float = 0.0
    error_message: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)
