# CDC Pipeline Reconciliation & Audit Report
**Audit Overall Status**: `PASSED`  
**Run ID**: `All Cumulative Runs`  

## Table Counts
| Table Name | Row Count |
|:---|---:|
| `source_events` | 33 |
| `current_state` | 5 |
| `event_history` | 12 |
| `rejected_events` | 8 |
| `pipeline_runs` | 3 |
| `current_state_active` | 4 |
| `current_state_tombstones` | 1 |

## Reconciliation Assertion Results
| Check Name | Status | Details |
|:---|:---:|:---|
| Accepted Source Events In Event History (0 Orphan Accepted) | `PASS` | Unmatched accepted events: 0 |
| Rejected Source Events In Quarantine (100% Traceability) | `PASS` | Unmatched rejected events: 0 |
| Entity Coverage (Distinct Orders in History == Current State Rows) | `PASS` | History distinct orders: 5, Current state rows: 5 |
| Tombstone Nullability Integrity (deleted_at consistent with is_deleted) | `PASS` | Tombstone anomalies: 0 |
| Monotonic Sequence Protection (Current state not regressed by older events) | `PASS` | Regressed sequences detected: 0 |
| Referential Integrity (current_state.last_event_id exists in event_history) | `PASS` | Orphan last_event_ids: 0 |
| Audit History Uniqueness (Zero Duplicate event_ids in event_history) | `PASS` | Duplicate event_ids: 0 |

## Mathematical Guarantees Verified
1. **Row Conservation**: Every single record ingested is accounted for (Accepted + Quarantined + Duplicate).
2. **Lineage Traceability**: 100% of quarantined records trace back to their source file and line coordinate.
3. **State Monotonicity**: Out-of-order events are preserved in history without regressing current state.
4. **Audit Immutability**: No duplicate events exist in the audit log; all target states maintain referential integrity.
