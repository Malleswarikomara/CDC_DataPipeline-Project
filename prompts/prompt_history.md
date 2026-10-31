# Prompt History

### Prompt 1: Database selection
**Prompt:**
> "Hey, I'm building a CDC pipeline in Python to process order events (insert, update, delete). It needs to run locally without Docker or PostgreSQL. What database should I use?"

**Notes:**
AI suggested SQLite with WAL mode. I chose SQLite because it has zero external dependencies, runs anywhere, and supports ACID transactions.

---

### Prompt 2: Table design & delete strategy
**Prompt:**
> "Can you help me design the SQLite tables? I need one for current state, one for event history, and one for rejected bad data. Also for deletes, is it better to soft delete or hard delete?"

**Notes:**
AI drafted the table schemas. For deletes, I decided to use soft deletes (`is_deleted = 1` and `deleted_at`) to preserve the audit trail.

---

### Prompt 3: Duplicate and out-of-order events
**Prompt:**
> "How should I handle duplicates and out-of-order events? Like if sequence 3 comes before sequence 2, how do I make sure sequence 2 doesn't overwrite sequence 3 in current state, but still gets saved to history?"

**Notes:**
AI provided the comparison logic using `sequence_number`. I added indexing and deduplication checks so incoming duplicate events are safely ignored.

---

### Prompt 4: Fixing out-of-order status validation
**Prompt:**
> "I hit a bug: sequence 3 arrived first with status SHIPPED, then sequence 2 arrived with status CONFIRMED. My code threw an invalid transition error. How do I fix this so earlier sequences don't fail validation?"

**Notes:**
The original code checked for strict sequential status flow. I fixed it by using status hierarchy ranks (`PENDING: 1, CONFIRMED: 2, SHIPPED: 3, DELIVERED: 4`) so late-arriving events can still be recorded into history.

---

### Prompt 5: Mathematical reconciliation
**Prompt:**
> "How do I reconcile raw input events with the database to prove no data was lost? What's the count formula, and how do I reconcile the total dollar amounts?"

**Notes:**
We defined the count formula:
$$\text{Total Input} = \text{Inserted} + \text{Updated} + \text{Deleted} + \text{Deduplicated} + \text{Rejected}$$
For the financial check, I wrote a query to pick the latest sequence per order so multiple updates wouldn't double-count prices.

---

## Summary of Key Decisions

| Area | What AI Suggested | What I Decided / Implemented |
| :--- | :--- | :--- |
| **Database** | SQLite or DuckDB | Used SQLite with WAL mode for zero-setup local execution |
| **Deletions** | Hard vs soft delete | Implemented soft deletes with `is_deleted` and `deleted_at` |
| **Ordering** | Basic sequence comparison | Tracked sequence numbers and added deduplication lookup |
| **State Bug** | Strict status check | Rewrote with status ranking to handle out-of-order arrivals |
| **Reconciliation** | Basic event count query | Fixed dollar amount query to only take latest sequence per order |
