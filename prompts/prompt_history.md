# AI Prompt & Development History

This document satisfies **Guideline 03 (USE LLMs TRANSPARENTLY)** and **FAQ Items 4 & 6** of the YottaFlex AI Candidate Project Submission Guidelines.

---

## 1. Executive Summary & Transparency Statement

As an aspiring AI Forward Deployed Engineer / AI Data Forward Deployed Engineer, I utilized Large Language Models (LLMs) as an interactive pair-programmer, design sounding board, and boilerplate accelerator throughout this 72-hour challenge. 

- **Where AI Accelerated My Work**:
  - Brainstorming edge-case taxonomy and schema field naming conventions.
  - Generating initial Python dataclass boilerplate and basic SQL DDL templates.
  - Scaffolding synthetic data generator randomization helpers.
  - Drafting initial pytest fixtures and test skeletons.

- **Where I Relied Heavily on It**:
  - Drafting initial regular expressions for ISO-8601 UTC timestamp and UUID format validation.
  - Formulating markdown table formatting scripts for console output display.

- **What I Personally Reviewed, Debugged, and Overrode (Human Engineering)**:
  - **Out-of-Order Transition Logic**: The AI initially proposed a naive adjacent state transition check (`PENDING -> CONFIRMED -> SHIPPED`). During manual testing, I noticed that when sequence 3 arrived before sequence 2, sequence 2 was erroneously flagged as `INVALID_STATUS_TRANSITION`. I personally replaced this with a numerical hierarchy rank algorithm (`PENDING: 1, CONFIRMED: 2, SHIPPED: 3, DELIVERED: 4`) that accommodates out-of-order sequence progression without corrupting audit history.
  - **ACID Transaction Boundaries & Concurrency**: The AI initially used default autocommit connections without explicit savepoint boundaries. I manually enforced `PRAGMA journal_mode = WAL`, `PRAGMA synchronous = NORMAL`, `PRAGMA foreign_keys = ON`, and wrapped batch ingestions in `BEGIN IMMEDIATE TRANSACTION` blocks with explicit exception rollbacks.
  - **Zero-Dependency Architectural Choice**: The AI initially suggested using Pandas, DuckDB, or external Dockerized PostgreSQL. I rejected external services to adhere strictly to the "Zero Setup / Reviewer Experience" requirement, designing the entire pipeline using Python 3 standard libraries and embedded SQLite.
  - **Mathematical Reconciliation Formulation**: I personally verified the exact invariant:
    $$\text{Total Input Events} = \text{Inserted} + \text{Updated} + \text{Deleted (Soft)} + \text{Deduplicated} + \text{Rejected (DLQ)}$$
    and adjusted financial reconciliation formulas to only sum non-deleted orders in `current_state`.

---

## 2. Chronological Prompt History & Interactive Iterations

### Iteration 1: Architecture & Technology Stack Evaluation
**Developer Prompt:**
> "I am designing a streaming Change Data Capture (CDC) pipeline in Python to ingest e-commerce order events (INSERT, UPDATE, DELETE). The solution must run locally without requiring reviewers to configure Docker, PostgreSQL, or cloud services. What is the most robust, zero-dependency embedded database architecture that supports ACID transactions, WAL mode, and fast indexing for out-of-order deduplication?"

**AI Assistant Contribution:**
- Recommended SQLite with Write-Ahead Logging (`PRAGMA journal_mode=WAL`) over DuckDB (which is OLAP-optimized) for row-level transactional updates.
- Outlined key PRAGMA settings for performance and integrity (`foreign_keys=ON`, `synchronous=NORMAL`).

**Human Validation & Decision:**
- Accepted SQLite with WAL mode. Added custom connection factory enforcing row factory mapping and isolation levels.

---

### Iteration 2: Relational Data Model & Target Schema Design
**Developer Prompt:**
> "Let's define the schema for our CDC pipeline. We need:
> 1. `current_state`: Authoritative canonical state of each order (order_id as primary key).
> 2. `event_history`: Append-only audit log tracking every received valid change event.
> 3. `rejected_events`: Dead-Letter Queue (DLQ) for malformed, negative-value, or invalid events.
> 4. `pipeline_runs`: Metadata ledger tracking run ID, timestamps, row counts, and status.
> 5. `reconciliation_audit`: Mathematical check records verifying invariant proofs.
> Can you draft the SQL DDL statements for these 5 tables with appropriate primary keys, foreign keys, and indexes for fast sequence lookup?"

**AI Assistant Contribution:**
- Drafted initial DDL scripts with table definitions, check constraints, and index recommendations on `(order_id, sequence_number)`.

**Human Validation & Decision:**
- Reviewed and modified `current_state` to include `is_deleted` (BOOLEAN) and `deleted_at` (TIMESTAMP) for soft-delete tombstone semantics instead of physically deleting rows, ensuring downstream consumer auditability.

---

### Iteration 3: Data Quality Rules & Validation Engine
**Developer Prompt:**
> "I need a robust data validation module (`src/validation.py`). Before any event touches the database, it must pass strict quality gates:
> - Check required fields (`event_id`, `event_type`, `order_id`, `sequence_number`, `timestamp`, `total_amount`).
> - Ensure `total_amount >= 0.0`.
> - Validate `event_type` is one of `INSERT`, `UPDATE`, `DELETE`.
> - Validate `status` against allowed lifecycle states (`PENDING`, `CONFIRMED`, `SHIPPED`, `DELIVERED`, `CANCELLED`).
> - Validate ISO-8601 UTC timestamp format.
> If any validation fails, return a detailed error code and human-readable reason so we can quarantine it in `rejected_events` without crashing the batch."

**AI Assistant Contribution:**
- Provided initial `ValidationResult` dataclass and validation rule methods.

**Human Validation & Decision:**
- Added schema drift protection: if an unknown extra column appears in incoming payloads, preserve it in a JSON `metadata` blob rather than rejecting the record outright, while still enforcing core field typing.

---

### Iteration 4: Core CDC Processing & Out-of-Order Sequence Resolution
**Developer Prompt:**
> "Here is the critical CDC engine requirement:
> 1. An incoming event could be an exact duplicate (same `event_id` or same `order_id` + `sequence_number`). How should we detect and suppress duplicates idempotently?
> 2. Events can arrive out-of-order. For example, `sequence_number = 3` arrives before `sequence_number = 2`. How should `current_state` handle this? We must update `current_state` with sequence 3, but when sequence 2 arrives later, it must NOT overwrite sequence 3. However, BOTH events must be appended to `event_history`.
> Can you write the `CDCProcessor.process_event()` logic to handle this?"

**AI Assistant Contribution:**
- Drafted core logic comparing incoming `sequence_number` against `current_state.sequence_number`.
- Drafted status transition verification.

**Human Validation & Debugging:**
- **Critical Fix**: Found a flaw in AI's proposed state validation. AI checked if `new_status` was an immediate successor of `current_status`. This failed when sequence 3 (`SHIPPED`) arrived before sequence 2 (`CONFIRMED`). I refactored the logic to maintain a status rank index and permit backward audit log insertion without regressing canonical current state.

---

### Iteration 5: Reproducible Synthetic Data Generator
**Developer Prompt:**
> "I need a synthetic test data generator script (`scripts/generate_data.py`) with reproducible seed control (`--seed 42`). It should generate:
> 1. Clean order lifecycles (INSERT -> UPDATE -> UPDATE -> DELETE).
> 2. Specific injected edge cases:
>    - Duplicate `event_id` replay
>    - Out-of-order sequence arrivals (sequence 3 before sequence 2)
>    - Malformed JSON rows and missing required keys
>    - Negative `total_amount` values
>    - Non-existent order updates
> Can you help scaffold this generator with support for both JSONL and CSV output?"

**AI Assistant Contribution:**
- Scaffolded random generator loops using Python's standard `random` and `uuid` modules.
- Created starter dataset generator function.

**Human Validation & Decision:**
- Added CSV `fieldnames` dynamic union handling to prevent `ValueError: dict contains fields not in fieldnames` when writing malformed records with unexpected extra columns.

---

### Iteration 6: Automated Mathematical Reconciliation
**Developer Prompt:**
> "How can we prove mathematical correctness to an evaluator automatically after every run? Let's write `src/reconciler.py` that calculates:
> - Event Count Reconciliation: Total Raw Input = Inserted + Updated + Soft-Deleted + Deduplicated + Rejected.
> - Financial Reconciliation: Compare sum of `total_amount` from valid raw events against sum of `total_amount` in `current_state` (excluding soft deletes).
> It should output a structured markdown summary report and fail the pipeline with an error code if the delta is non-zero."

**AI Assistant Contribution:**
- Provided initial SQL aggregation queries for count verification.

**Human Validation & Decision:**
- Corrected the financial reconciliation formula: because an order can have multiple updates modifying its price, summing all raw UPDATE events would double-count amounts. I wrote a dedicated query selecting the latest sequence for each order from `event_history` to compute the expected target sum.

---

### Iteration 7: Pytest Test Suite & Failure Scenarios
**Developer Prompt:**
> "Let's build a comprehensive test suite in `tests/` using pytest. I want isolated tests covering:
> 1. Deduplication: Replaying the exact same event does not insert duplicate rows.
> 2. Out-of-order sequence resolution: Late arriving lower sequences do not overwrite higher sequences.
> 3. Idempotency: Running the same input batch twice results in zero additional state modifications.
> 4. DLQ Quarantine: Verifying negative amounts and malformed payloads route to `rejected_events`.
> 5. Empty batch: Pipeline handles empty files gracefully without throwing index errors."

**AI Assistant Contribution:**
- Generated pytest boilerplate and test functions across 7 test modules.

**Human Validation & Decision:**
- Executed tests locally, verified 100% pass rate (32/32 tests passing in < 1.0 second), and ensured tests run without network access or external database setup.

---

### Iteration 8: Packaging, VS Code Config & Documentation
**Developer Prompt:**
> "Review the final project against the YottaFlex AI Candidate Project Submission Guidelines. I need:
> 1. One-click batch runners for Windows users (`run_demo.bat`, `run_tests.bat`, `view_output.bat`).
> 2. VS Code `launch.json` for one-click F5 debugging.
> 3. Comprehensive README documenting Architecture, Schemas, Edge Cases, Trade-offs, and Instructions."

**AI Assistant Contribution:**
- Drafted initial README sections and batch scripts.

**Human Validation & Final Polish:**
- Personally verified all execution commands across PowerShell and Windows Command Prompt. Verified that zero AI scaffolding or placeholder tags remain.

---

## 3. Engineering Contribution Matrix

| Component | AI Assistance Role | Candidate / Human Role |
| :--- | :--- | :--- |
| **System Architecture** | Explored SQLite vs. DuckDB vs. Postgres trade-offs | Chose SQLite with WAL mode for zero external setup and ACID compliance |
| **Schema & Data Model** | Drafted basic table DDL templates | Enforced soft-delete tombstone semantics and indexing strategies |
| **Out-of-Order Engine** | Initial sequence comparison skeleton | Diagnosed and rewrote status-hierarchy rank algorithm for late events |
| **Validation & DLQ** | Regex patterns for timestamps and UUIDs | Structured error codes, schema drift handling, and quarantine routing |
| **Reconciliation Logic** | Drafted basic row count aggregations | Formulated correct financial sum query handling multi-update lifecycles |
| **Edge Case Testing** | Pytest scaffolding and fixture generation | Designed 8 negative failure scenarios, idempotency and ACID rollback tests |
| **Project Documentation** | Markdown table structure formatting | Authored architectural trade-offs, rationale, and submission alignment |
