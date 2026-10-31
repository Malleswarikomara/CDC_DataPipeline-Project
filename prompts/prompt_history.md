# AI Prompt History & Development Log

This document records the prompt history and development log from building this Change Data Capture (CDC) pipeline project, in accordance with **Guideline 03 (USE LLMs TRANSPARENTLY)** and **FAQ Items 4 & 6** of the YottaFlex AI Candidate Project Submission Guidelines.

---

## Overview of AI Usage

Throughout this 72-hour challenge, I used AI as an interactive pair programmer and sounding board. Here is a transparent breakdown:
- **Where AI helped most**: Fast boilerplate generation (dataclasses, SQL tables, pytest skeletons, regex patterns).
- **Where I spent the most manual engineering effort**: Designing the out-of-order sequence resolution algorithm, debugging state transition validation, configuring SQLite transaction boundaries/WAL mode, and formulating the mathematical reconciliation logic.

---

## Chronological Prompt Log

### Prompt 1: Choosing the local database & architecture
**What I asked:**
> "Hey, I'm working on a coding assessment to build a CDC (Change Data Capture) pipeline in Python for e-commerce order events (inserts, updates, and deletes). It needs to keep an audit history and a current state table, and handle duplicates and out-of-order events. What database should I use if I want this to run completely locally on any machine without needing the reviewer to install PostgreSQL or Docker?"

**Outcome & My Review:**
- AI suggested either SQLite with WAL mode or DuckDB.
- I chose SQLite with Write-Ahead Logging (`WAL`) mode because SQLite is built directly into Python's standard library (zero external dependencies for the evaluator), provides full ACID transaction support, and is well-suited for row-by-row transactional change streams.

---

### Prompt 2: Schema design and soft deletes vs hard deletes
**What I asked:**
> "Okay, SQLite sounds perfect. Let's design the tables. I need:
> 1. current_state (the latest state of each order)
> 2. event_history (an append-only log of every valid event we get)
> 3. rejected_events (a dead-letter queue for any corrupt or invalid data)
> 4. pipeline_runs (to log each run, rows processed, status, time)
> 5. reconciliation_audit (to store mathematical reconciliation results)
> 
> Can you help me write the schema / DDL in SQLite for these? Also, for deletes, should I delete the row or use a soft delete flag?"

**Outcome & My Review:**
- AI drafted the 5 table schemas with primary and foreign keys.
- On the delete strategy: we agreed that a soft-delete (`is_deleted = 1`, `deleted_at = <timestamp>`) is much better than a hard DELETE because down-stream systems and auditors need to see that an order was cancelled/deleted without losing historical records.

---

### Prompt 3: Ingestion & data quality validation
**What I asked:**
> "Now I want to build the validation layer before records get written to the database. What data quality checks should I put in place?
> I'm thinking:
> - check that required fields like event_id, order_id, sequence_number, timestamp, and amount exist
> - amount shouldn't be negative
> - event_type must be INSERT, UPDATE, or DELETE
> - timestamp needs to be a valid ISO format
> If any check fails, how do I route it to rejected_events with an error reason without stopping the whole pipeline?"

**Outcome & My Review:**
- AI created a `ValidationResult` class and helper checks.
- I modified the validation logic to handle schema drift: if an incoming event has unexpected extra columns, we capture them in a JSON metadata field rather than crashing the ingestion batch.

---

### Prompt 4: Handling duplicates and out-of-order sequence numbers
**What I asked:**
> "Here is the hardest requirement in the challenge:
> 1. If we receive a duplicate event (same event_id or same order_id + sequence_number), we need to skip it so it's idempotent.
> 2. If events arrive out of order, like sequence 3 arrives before sequence 2:
>    - Sequence 3 should update current_state.
>    - When sequence 2 arrives later, it should NOT overwrite sequence 3 in current_state because sequence 3 is newer.
>    - But BOTH sequence 2 and sequence 3 must be saved in event_history.
> 
> How do I write this logic in Python?"

**Outcome & My Review:**
- AI provided the initial `CDCProcessor.process_event` logic comparing `incoming.sequence_number` against `existing.sequence_number`.
- I added deduplication caching and indexed lookups on `(order_id, sequence_number)` so repeated events are detected in $O(1)$ time.

---

### Prompt 5: Debugging the out-of-order status transition bug
**What I asked:**
> "I ran some test events and hit a problem: sequence 3 arrived first with status 'SHIPPED', and then sequence 2 arrived with status 'CONFIRMED'. My code rejected sequence 2 saying 'invalid status transition' because it expected status to go PENDING -> CONFIRMED -> SHIPPED in order.
> How can I fix the validation so that late-arriving earlier events are still accepted and saved to history, while current_state stays on the latest state?"

**Outcome & My Review:**
- **Important Fix**: AI's original validation code assumed events always arrive in chronological order. 
- I diagnosed the bug and refactored the validator to use a status hierarchy ranking (`PENDING: 1, CONFIRMED: 2, SHIPPED: 3, DELIVERED: 4`). If a late-arriving event has an earlier sequence number, it is allowed to enter `event_history` for audit completeness without reverting the `current_state` record.

---

### Prompt 6: Test data generator with realistic edge cases
**What I asked:**
> "Can you help me write a Python script to generate sample test data? I want to generate two files:
> 1. A normal starter sample with full lifecycles (INSERT, UPDATE, DELETE) and some duplicates.
> 2. An edge cases file that tests corrupt data: negative total_amount, missing order_id, invalid event types, corrupt JSON, and out-of-order sequences.
> I want to be able to set a random seed so the data is reproducible."

**Outcome & My Review:**
- AI scaffolded the random generator script.
- I fixed a bug in the CSV writer where dictionaries containing extra/missing keys caused `ValueError: dict contains fields not in fieldnames` by dynamically building the union of all keys across records.

---

### Prompt 7: Mathematical reconciliation logic
**What I asked:**
> "The challenge guidelines require mathematical reconciliation between input events and final database state to prove no records were dropped.
> What formula should I use?
> I'm thinking:
> Total Input Events = Inserted + Updated + Soft Deleted + Deduplicated (Ignored) + Rejected.
> Also how do I reconcile the financial total_amount? If an order had 3 updates with different prices, just summing raw events will give the wrong number. How do I write the query to match the current_state total?"

**Outcome & My Review:**
- We formalized the count invariant:
  $$\text{Total Input} = \text{Inserted} + \text{Updated} + \text{Deleted} + \text{Deduplicated} + \text{Rejected}$$
- For financial reconciliation, I wrote a dedicated query selecting the latest sequence for each order from `event_history` so intermediate price updates aren't erroneously summed together.

---

### Prompt 8: Comprehensive automated testing with pytest
**What I asked:**
> "I want to write automated tests using pytest to test every single edge case from the prompt:
> - duplicate event suppression
> - out-of-order event ordering
> - DLQ rejection of negative amounts, missing fields, and bad JSON
> - idempotency (running the same file twice results in 0 new rows in current_state)
> - empty input file handling
> - transaction rollback if an error happens mid-batch
> Can you help me structure the test files in a tests/ folder?"

**Outcome & My Review:**
- AI drafted the test skeletons across 7 test modules.
- I implemented test fixtures, verified all 32 tests pass in under 1 second, and validated that tests run completely offline without external databases.

---

### Prompt 9: Runner scripts and VS Code developer experience
**What I asked:**
> "I want to make it super easy for anyone reviewing this project on Windows to run everything with one click or in VS Code.
> Can you create simple .bat files:
> - run_demo.bat to run the pipeline
> - run_tests.bat to run pytest
> - view_output.bat to see the database tables in terminal
> And add a launch.json for VS Code debugging?"

**Outcome & My Review:**
- Added the 3 one-click `.bat` scripts and `.vscode/launch.json`.
- Tested running them in Windows Command Prompt, PowerShell, and VS Code.

---

### Prompt 10: Documentation and README
**What I asked:**
> "Can you help me write a clear README.md for the repo? I want to document:
> - Quick start commands
> - Architecture and data flow diagram
> - Table schemas
> - How out-of-order events and duplicates are handled
> - Edge cases covered
> - Trade-offs (why SQLite, memory-bounded streaming vs pandas)
> - Test results and reconciliation proofs"

**Outcome & My Review:**
- Structured the README following the 4 evaluation pillars: Make It Run, Show Your Work, Use LLMs Transparently, and Make Your Reasoning Visible.
- Filled in actual test output logs and performance metrics (3,500+ records/sec).

---

## Key Human Engineering Decisions Summary

| Area | What AI Suggested | What I Personally Evaluated & Implemented |
| :--- | :--- | :--- |
| **Database** | Suggested Postgres, DuckDB, or SQLite | Chose SQLite with WAL mode so reviewers have **zero external setup** |
| **Deletions** | Hard DELETE vs Soft DELETE | Chose **soft-delete tombstones** (`is_deleted = 1`) to preserve audit history |
| **Out-of-Order Logic** | Strict sequential status transition | Rewrote with **status hierarchy ranks** so late-arriving events don't fail |
| **Data Quality / DLQ** | Discard invalid rows with warnings | Created dedicated `rejected_events` DLQ table with error codes and raw payloads |
| **Financial Reconciliation** | Sum of all raw event amounts | Corrected query to take the **latest sequence amount** per order to avoid double counting |
| **Transactions** | Default auto-commit | Added explicit **`BEGIN IMMEDIATE TRANSACTION`** blocks with rollback on error |
