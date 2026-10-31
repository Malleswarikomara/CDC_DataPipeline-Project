"""
Tests for EventValidator (schema, null checks, boundaries, business rules).
"""

import pytest
from src.validation import EventValidator
from src.models import ValidatedEvent, RejectedRecord, RejectionCode


def test_validation_happy_path(valid_order_event):
    """Valid payload should produce a ValidatedEvent."""
    validator = EventValidator()
    result = validator.validate(valid_order_event, "file.json", 1, "run_01")
    assert isinstance(result, ValidatedEvent)
    assert result.order_id == "ORD-TEST-001"
    assert result.total_amount == 150.00
    assert result.source_sequence == 1


@pytest.mark.parametrize("missing_field", [
    "event_id", "event_type", "event_timestamp", "source_sequence",
    "order_id", "customer_id", "order_status", "total_amount", "currency"
])
def test_validation_missing_required_field(valid_order_event, missing_field):
    """Missing any mandatory field should route to RejectedRecord."""
    del valid_order_event[missing_field]
    validator = EventValidator()
    result = validator.validate(valid_order_event, "file.json", 1, "run_01")
    assert isinstance(result, RejectedRecord)
    assert result.rejection_code == RejectionCode.MISSING_REQUIRED_FIELD.value


def test_validation_negative_amount(valid_order_event):
    """Negative total_amount must be rejected."""
    valid_order_event["total_amount"] = -25.50
    validator = EventValidator()
    result = validator.validate(valid_order_event, "file.json", 1, "run_01")
    assert isinstance(result, RejectedRecord)
    assert result.rejection_code == RejectionCode.NEGATIVE_AMOUNT.value


def test_validation_invalid_timestamp(valid_order_event):
    """Malformed timestamp string should be rejected."""
    valid_order_event["event_timestamp"] = "invalid_timestamp"
    validator = EventValidator()
    result = validator.validate(valid_order_event, "file.json", 1, "run_01")
    assert isinstance(result, RejectedRecord)
    assert result.rejection_code == RejectionCode.INVALID_TIMESTAMP.value


def test_validation_invalid_sequence(valid_order_event):
    """Non-positive sequence numbers must be rejected."""
    valid_order_event["source_sequence"] = 0
    validator = EventValidator()
    result = validator.validate(valid_order_event, "file.json", 1, "run_01")
    assert isinstance(result, RejectedRecord)
    assert result.rejection_code == RejectionCode.INVALID_SEQUENCE.value


def test_validation_invalid_event_type(valid_order_event):
    """Event types other than INSERT, UPDATE, DELETE must be rejected."""
    valid_order_event["event_type"] = "MERGE"
    validator = EventValidator()
    result = validator.validate(valid_order_event, "file.json", 1, "run_01")
    assert isinstance(result, RejectedRecord)
    assert result.rejection_code == RejectionCode.INVALID_EVENT_TYPE.value


def test_validation_unexpected_schema_column(valid_order_event):
    """Unexpected additional columns must be rejected under strict schema validation."""
    valid_order_event["unauthorized_column"] = "unexpected_data"
    validator = EventValidator(strict_schema=True)
    result = validator.validate(valid_order_event, "file.json", 1, "run_01")
    assert isinstance(result, RejectedRecord)
    assert result.rejection_code == RejectionCode.UNEXPECTED_SCHEMA_CHANGE.value


def test_validation_non_dict_payload():
    """Non-dictionary payload (e.g., list or integer) should be caught as malformed."""
    validator = EventValidator()
    result = validator.validate("just a string", "file.json", 1, "run_01")
    assert isinstance(result, RejectedRecord)
    assert result.rejection_code == RejectionCode.MALFORMED_RECORD.value
