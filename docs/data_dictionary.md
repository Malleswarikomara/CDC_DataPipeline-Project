# Data Dictionary: CDC Pipeline Target & Staging Tables

This document outlines the detailed physical data model, column specifications, constraints, data types, nullability, and business grain for all tables in the CDC Pipeline database.

---

## 1. `source_events` (Raw Staging Table)
- **Description**: Staging table recording every raw event ingested into the pipeline, establishing complete data lineage.
- **Grain**: One row per raw incoming event record per file ingest.

| Column Name | Data Type | Nullable | Primary / Foreign Key | Description |
|:---|:---|:---:|:---:|:---|
| `raw_id` | INTEGER | NO | Primary Key (Auto-inc) | Surrogate identifier for the staged raw record. |
| `run_id` | TEXT | NO | FK -> `pipeline_runs(run_id)` | Execution run identifier. |
| `source_file` | TEXT | NO | None | Name of the source file (e.g., `orders.jsonl`). |
| `source_row_num` | INTEGER | NO | None | 1-based line or row coordinate in the source file. |
| `raw_event_id` | TEXT | YES | None | Extracted event identifier, if present in raw payload. |
| `raw_payload` | TEXT | NO | None | Full raw JSON representation of the incoming record. |
| `received_at` | TEXT | NO | None | ISO-8601 UTC timestamp when record was staged. |
| `status` | TEXT | NO | None | Processing outcome: `ACCEPTED`, `REJECTED`, or `DUPLICATE`. |

---

## 2. `current_state` (Canonical Target View)
- **Description**: The authoritative, latest known state of each order entity. Incorporates soft-deletion tombstones and state versioning.
- **Grain**: Exactly one row per unique `order_id`.

| Column Name | Data Type | Nullable | Primary / Foreign Key | Description |
|:---|:---|:---:|:---:|:---|
| `order_id` | TEXT | NO | Primary Key | Business identifier of the order (e.g., `ORD-101`). |
| `customer_id` | TEXT | NO | None | Identifier of the customer placing the order. |
| `order_status` | TEXT | NO | None | Latest status (`PENDING`, `CONFIRMED`, `SHIPPED`, `DELIVERED`, `CANCELLED`). |
| `total_amount` | REAL | NO | None | Latest monetary value of the order (numeric decimal). |
| `currency` | TEXT | NO | None | Standard 3-letter currency code (e.g., `USD`, `EUR`). |
| `shipping_address` | TEXT | YES | None | Current delivery street address. |
| `item_count` | INTEGER | NO | None | Quantity of items included in the order. |
| `created_at` | TEXT | NO | None | ISO-8601 UTC timestamp of initial entity creation. |
| `updated_at` | TEXT | NO | None | ISO-8601 UTC timestamp of most recently applied event. |
| `last_event_id` | TEXT | NO | FK -> `event_history(event_id)` | Identifier of the event that produced this state. |
| `last_sequence` | INTEGER | NO | None | Highest source sequence number applied to this entity. |
| `is_deleted` | INTEGER | NO | None | Soft-deletion tombstone flag: `0` = Active, `1` = Deleted. |
| `deleted_at` | TEXT | YES | None | ISO-8601 UTC timestamp when tombstone was applied (`NULL` if active). |
| `record_version` | INTEGER | NO | None | Monotonically increasing revision counter (starts at 1). |
| `record_hash` | TEXT | NO | None | SHA-256 hash of core payload fields for fast change detection. |

---

## 3. `event_history` (Immutable Audit Log)
- **Description**: Append-only log recording every accepted event that transitioned the system. Stores before/after payload snapshots for point-in-time state reconstruction.
- **Grain**: One row per unique `event_id`.

| Column Name | Data Type | Nullable | Primary / Foreign Key | Description |
|:---|:---|:---:|:---:|:---|
| `history_id` | INTEGER | NO | Primary Key (Auto-inc) | Surrogate sequence identifier. |
| `event_id` | TEXT | NO | UNIQUE | Deterministic unique event ID from the source. |
| `order_id` | TEXT | NO | FK -> `current_state(order_id)` | Business entity identifier. |
| `event_type` | TEXT | NO | None | CDC action: `INSERT`, `UPDATE`, or `DELETE`. |
| `source_sequence` | INTEGER | NO | None | Sequence counter from producer indicating event order. |
| `event_timestamp` | TEXT | NO | None | ISO-8601 UTC timestamp emitted by the producer. |
| `payload_before` | TEXT | YES | None | JSON snapshot of `current_state` prior to mutation (`NULL` for INSERTs). |
| `payload_after` | TEXT | YES | None | JSON snapshot of event payload applied. |
| `run_id` | TEXT | NO | FK -> `pipeline_runs(run_id)` | Run ID that committed this event. |
| `ingested_at` | TEXT | NO | None | ISO-8601 UTC timestamp when written to database. |

---

## 4. `rejected_events` (Quarantine Dead-Letter Queue)
- **Description**: Quarantine table isolating all malformed, non-conforming, or conflicting records with rich diagnostic metadata.
- **Grain**: One row per rejected raw event.

| Column Name | Data Type | Nullable | Primary / Foreign Key | Description |
|:---|:---|:---:|:---:|:---|
| `quarantine_id` | INTEGER | NO | Primary Key (Auto-inc) | Surrogate identifier for quarantined item. |
| `run_id` | TEXT | NO | FK -> `pipeline_runs(run_id)` | Execution run identifier. |
| `source_file` | TEXT | NO | None | Source file name where error originated. |
| `source_row_num` | INTEGER | NO | None | Source line coordinate of the failed record. |
| `raw_event_id` | TEXT | YES | None | Event ID if parseable from the payload. |
| `rejection_code` | TEXT | NO | None | Machine-readable rejection code (e.g., `NEGATIVE_AMOUNT`). |
| `rejection_reason` | TEXT | NO | None | Human-readable explanation of validation or CDC failure. |
| `raw_payload` | TEXT | NO | None | Raw record string for debugging and replay. |
| `failed_at` | TEXT | NO | None | ISO-8601 UTC timestamp when rejected. |

---

## 5. `pipeline_runs` (Operational Telemetry Table)
- **Description**: High-level execution log capturing duration, throughput, and row count metrics per pipeline execution.
- **Grain**: Exactly one row per pipeline run execution.

| Column Name | Data Type | Nullable | Primary / Foreign Key | Description |
|:---|:---|:---:|:---:|:---|
| `run_id` | TEXT | NO | Primary Key | Deterministic run identifier (e.g., `run_20261030_120000_a1b2c3`). |
| `start_time` | TEXT | NO | None | ISO-8601 UTC timestamp when execution began. |
| `end_time` | TEXT | YES | None | ISO-8601 UTC timestamp when execution completed. |
| `status` | TEXT | NO | None | Execution state: `RUNNING`, `SUCCESS`, `PARTIAL_SUCCESS`, `FAILED`. |
| `source_file` | TEXT | NO | None | Input file processed. |
| `records_read` | INTEGER | NO | None | Total raw records ingested. |
| `records_accepted` | INTEGER | NO | None | Records successfully validated and applied. |
| `records_rejected` | INTEGER | NO | None | Records routed to quarantine. |
| `records_duplicate` | INTEGER | NO | None | Duplicate events safely ignored. |
| `inserts_applied` | INTEGER | NO | None | Number of new entities created in `current_state`. |
| `updates_applied` | INTEGER | NO | None | Number of entity mutations applied to `current_state`. |
| `deletes_applied` | INTEGER | NO | None | Number of soft-deletion tombstones applied. |
| `out_of_order_count` | INTEGER | NO | None | Number of late events preserved in history without mutating state. |
| `duration_seconds` | REAL | NO | None | Total elapsed execution wall-clock time in seconds. |
| `error_message` | TEXT | YES | None | Exception message if pipeline aborted. |
