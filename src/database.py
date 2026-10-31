"""
Database management layer for the CDC Pipeline.
Manages SQLite connections, DDL schema creation, indexing, connection pooling,
and transactional context managers for ACID guarantees.
"""

import sqlite3
from contextlib import contextmanager
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
import logging

logger = logging.getLogger("cdc_pipeline.database")


SCHEMA_DDL = """
-- 1. Source Raw Events Staging Table
CREATE TABLE IF NOT EXISTS source_events (
    raw_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_num INTEGER NOT NULL,
    raw_event_id TEXT,
    raw_payload TEXT NOT NULL,
    received_at TEXT NOT NULL,
    status TEXT NOT NULL -- ACCEPTED, REJECTED, DUPLICATE
);

-- 2. Target Current State Canonical Table
CREATE TABLE IF NOT EXISTS current_state (
    order_id TEXT PRIMARY KEY,
    customer_id TEXT NOT NULL,
    order_status TEXT NOT NULL,
    total_amount REAL NOT NULL,
    currency TEXT NOT NULL,
    shipping_address TEXT,
    item_count INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    last_event_id TEXT NOT NULL,
    last_sequence INTEGER NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0, -- 0 = Active, 1 = Tombstone
    deleted_at TEXT,
    record_version INTEGER NOT NULL DEFAULT 1,
    record_hash TEXT NOT NULL
);

-- 3. Target Event History Audit Table (Append-Only)
CREATE TABLE IF NOT EXISTS event_history (
    history_id INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id TEXT NOT NULL UNIQUE,
    order_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    source_sequence INTEGER NOT NULL,
    event_timestamp TEXT NOT NULL,
    payload_before TEXT,
    payload_after TEXT,
    run_id TEXT NOT NULL,
    ingested_at TEXT NOT NULL
);

-- 4. Quarantine / Rejected Events Table
CREATE TABLE IF NOT EXISTS rejected_events (
    quarantine_id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id TEXT NOT NULL,
    source_file TEXT NOT NULL,
    source_row_num INTEGER NOT NULL,
    raw_event_id TEXT,
    rejection_code TEXT NOT NULL,
    rejection_reason TEXT NOT NULL,
    raw_payload TEXT NOT NULL,
    failed_at TEXT NOT NULL
);

-- 5. Operational Pipeline Runs Table
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id TEXT PRIMARY KEY,
    start_time TEXT NOT NULL,
    end_time TEXT,
    status TEXT NOT NULL,
    source_file TEXT NOT NULL,
    records_read INTEGER NOT NULL DEFAULT 0,
    records_accepted INTEGER NOT NULL DEFAULT 0,
    records_rejected INTEGER NOT NULL DEFAULT 0,
    records_duplicate INTEGER NOT NULL DEFAULT 0,
    inserts_applied INTEGER NOT NULL DEFAULT 0,
    updates_applied INTEGER NOT NULL DEFAULT 0,
    deletes_applied INTEGER NOT NULL DEFAULT 0,
    out_of_order_count INTEGER NOT NULL DEFAULT 0,
    duration_seconds REAL NOT NULL DEFAULT 0.0,
    error_message TEXT
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_curr_order_id ON current_state(order_id);
CREATE INDEX IF NOT EXISTS idx_curr_status ON current_state(order_status);
CREATE INDEX IF NOT EXISTS idx_curr_deleted ON current_state(is_deleted);
CREATE INDEX IF NOT EXISTS idx_hist_order_seq ON event_history(order_id, source_sequence);
CREATE INDEX IF NOT EXISTS idx_hist_event_id ON event_history(event_id);
CREATE INDEX IF NOT EXISTS idx_source_event_id ON source_events(raw_event_id);
CREATE INDEX IF NOT EXISTS idx_source_run ON source_events(run_id);
CREATE INDEX IF NOT EXISTS idx_rej_run ON rejected_events(run_id);
CREATE INDEX IF NOT EXISTS idx_rej_code ON rejected_events(rejection_code);
"""


class DatabaseManager:
    """Encapsulates SQLite connection management and atomic transactional operations."""

    def __init__(self, db_path: str = "data/cdc_pipeline.db"):
        self.db_path = db_path
        if db_path != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_connection()

    def _init_connection(self):
        """Initializes database schema and configures performance PRAGMAs."""
        conn = self.get_connection()
        try:
            # PRAGMA optimizations
            conn.execute("PRAGMA foreign_keys = ON;")
            if self.db_path != ":memory:":
                conn.execute("PRAGMA journal_mode = WAL;")
                conn.execute("PRAGMA synchronous = NORMAL;")
            conn.execute("PRAGMA busy_timeout = 10000;")
            conn.executescript(SCHEMA_DDL)
            conn.commit()
            logger.debug(f"Database schema initialized successfully at: {self.db_path}")
        finally:
            conn.close()

    def get_connection(self) -> sqlite3.Connection:
        """Returns a new connection with row_factory set to sqlite3.Row."""
        conn = sqlite3.connect(self.db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def transaction(self):
        """Context manager providing an atomic ACID transaction with automatic commit/rollback."""
        conn = self.get_connection()
        try:
            conn.execute("BEGIN IMMEDIATE;")
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction aborted and rolled back due to error: {e}")
            raise
        finally:
            conn.close()

    def event_already_processed(self, event_id: str) -> bool:
        """Checks if an event_id was previously accepted and committed into event_history."""
        conn = self.get_connection()
        try:
            cur = conn.execute("SELECT 1 FROM event_history WHERE event_id = ? LIMIT 1;", (event_id,))
            return cur.fetchone() is not None
        finally:
            conn.close()

    def get_current_state(self, order_id: str, conn: Optional[sqlite3.Connection] = None) -> Optional[Dict[str, Any]]:
        """Retrieves current state for a given order_id."""
        should_close = False
        if conn is None:
            conn = self.get_connection()
            should_close = True
        try:
            cur = conn.execute("SELECT * FROM current_state WHERE order_id = ?;", (order_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            if should_close:
                conn.close()

    def save_pipeline_run(self, run_dict: Dict[str, Any]):
        """Persists or updates pipeline run telemetry in pipeline_runs table."""
        query = """
        INSERT INTO pipeline_runs (
            run_id, start_time, end_time, status, source_file,
            records_read, records_accepted, records_rejected, records_duplicate,
            inserts_applied, updates_applied, deletes_applied, out_of_order_count,
            duration_seconds, error_message
        ) VALUES (
            :run_id, :start_time, :end_time, :status, :source_file,
            :records_read, :records_accepted, :records_rejected, :records_duplicate,
            :inserts_applied, :updates_applied, :deletes_applied, :out_of_order_count,
            :duration_seconds, :error_message
        )
        ON CONFLICT(run_id) DO UPDATE SET
            end_time = excluded.end_time,
            status = excluded.status,
            records_read = excluded.records_read,
            records_accepted = excluded.records_accepted,
            records_rejected = excluded.records_rejected,
            records_duplicate = excluded.records_duplicate,
            inserts_applied = excluded.inserts_applied,
            updates_applied = excluded.updates_applied,
            deletes_applied = excluded.deletes_applied,
            out_of_order_count = excluded.out_of_order_count,
            duration_seconds = excluded.duration_seconds,
            error_message = excluded.error_message;
        """
        conn = self.get_connection()
        try:
            conn.execute(query, run_dict)
            conn.commit()
        finally:
            conn.close()

    def fetch_run_summary(self, run_id: str) -> Optional[Dict[str, Any]]:
        """Fetches execution summary for a given run_id."""
        conn = self.get_connection()
        try:
            cur = conn.execute("SELECT * FROM pipeline_runs WHERE run_id = ?;", (run_id,))
            row = cur.fetchone()
            return dict(row) if row else None
        finally:
            conn.close()

    def get_table_counts(self) -> Dict[str, int]:
        """Returns row counts for all primary tables for monitoring and reconciliation."""
        conn = self.get_connection()
        counts = {}
        tables = ["source_events", "current_state", "event_history", "rejected_events", "pipeline_runs"]
        try:
            for tbl in tables:
                cur = conn.execute(f"SELECT COUNT(*) FROM {tbl};")
                counts[tbl] = cur.fetchone()[0]
            # Additional aggregate
            cur = conn.execute("SELECT COUNT(*) FROM current_state WHERE is_deleted = 0;")
            counts["current_state_active"] = cur.fetchone()[0]
            cur = conn.execute("SELECT COUNT(*) FROM current_state WHERE is_deleted = 1;")
            counts["current_state_tombstones"] = cur.fetchone()[0]
            return counts
        finally:
            conn.close()
