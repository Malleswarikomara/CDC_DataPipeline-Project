"""
Pytest configuration and reusable test fixtures.
"""

import pytest
from pathlib import Path
import tempfile
import json
import sqlite3

from src.config import PipelineConfig
from src.database import DatabaseManager
from src.pipeline import CDCPipeline


@pytest.fixture
def temp_db_path(tmp_path):
    """Provides a temporary, isolated SQLite database file path."""
    db_file = tmp_path / "test_cdc.db"
    return str(db_file)


@pytest.fixture
def db_manager(temp_db_path):
    """Provides an initialized DatabaseManager on a temporary database."""
    return DatabaseManager(temp_db_path)


@pytest.fixture
def pipeline(temp_db_path):
    """Provides a CDCPipeline instance bound to an isolated temporary database."""
    config = PipelineConfig(db_path=temp_db_path, log_level="DEBUG")
    return CDCPipeline(config=config)


@pytest.fixture
def valid_order_event():
    """Provides a valid baseline INSERT event dictionary."""
    return {
        "event_id": "test_evt_001",
        "event_type": "INSERT",
        "event_timestamp": "2026-10-30T12:00:00Z",
        "source_sequence": 1,
        "order_id": "ORD-TEST-001",
        "customer_id": "CUST-TEST-001",
        "order_status": "PENDING",
        "total_amount": 150.00,
        "currency": "USD",
        "shipping_address": "100 Innovation Way, Tech City",
        "item_count": 2,
    }
