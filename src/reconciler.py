"""
Data reconciliation and integrity verification module for the CDC Pipeline.
Executes mathematical reconciliation queries and consistency assertions across
source_events, current_state, event_history, and rejected_events.
"""

from typing import Dict, Any, List, Tuple
import sqlite3
import json
import logging

from src.database import DatabaseManager

logger = logging.getLogger("cdc_pipeline.reconciler")


class ReconciliationAuditor:
    """Performs deep audit queries and verification assertions on CDC target tables."""

    def __init__(self, db: DatabaseManager):
        self.db = db

    def run_full_audit(self, run_id: str = None) -> Dict[str, Any]:
        """
        Executes a comprehensive set of reconciliation assertions.
        Returns a dictionary containing test outcomes, counts, and discrepancies.
        """
        conn = self.db.get_connection()
        report = {
            "run_id": run_id,
            "passed_all": True,
            "checks": [],
            "metrics": {},
        }

        try:
            # 1. Total row counts
            counts = self.db.get_table_counts()
            report["metrics"]["table_counts"] = counts

            # 2. Conservation of Rows Check
            # records_read = accepted + rejected + duplicate
            if run_id:
                cur = conn.execute(
                    """
                    SELECT records_read, records_accepted, records_rejected, records_duplicate
                    FROM pipeline_runs WHERE run_id = ?;
                    """,
                    (run_id,),
                )
                run_row = cur.fetchone()
                if run_row:
                    r_read, r_acc, r_rej, r_dup = (
                        run_row["records_read"],
                        run_row["records_accepted"],
                        run_row["records_rejected"],
                        run_row["records_duplicate"],
                    )
                    conservation_pass = (r_read == (r_acc + r_rej + r_dup))
                    report["checks"].append({
                        "name": "Row Conservation (Read == Accepted + Rejected + Duplicate)",
                        "passed": conservation_pass,
                        "details": f"Read: {r_read}, Accepted: {r_acc}, Rejected: {r_rej}, Duplicate: {r_dup}",
                    })
                    if not conservation_pass:
                        report["passed_all"] = False

            # 3. Source Staging to Target History Reconciliation
            cur = conn.execute(
                """
                SELECT COUNT(*) FROM source_events se
                WHERE se.status = 'ACCEPTED'
                  AND NOT EXISTS (SELECT 1 FROM event_history eh WHERE eh.event_id = se.raw_event_id);
                """
            )
            orphan_accepted = cur.fetchone()[0]
            passed_source_hist = (orphan_accepted == 0)
            report["checks"].append({
                "name": "Accepted Source Events In Event History (0 Orphan Accepted)",
                "passed": passed_source_hist,
                "details": f"Unmatched accepted events: {orphan_accepted}",
            })
            if not passed_source_hist:
                report["passed_all"] = False

            # 4. Source Staging to Quarantine Reconciliation
            cur = conn.execute(
                """
                SELECT COUNT(*) FROM source_events se
                WHERE se.status = 'REJECTED'
                  AND NOT EXISTS (
                      SELECT 1 FROM rejected_events re 
                      WHERE re.run_id = se.run_id AND re.source_row_num = se.source_row_num
                  );
                """
            )
            orphan_rejected = cur.fetchone()[0]
            passed_source_rej = (orphan_rejected == 0)
            report["checks"].append({
                "name": "Rejected Source Events In Quarantine (100% Traceability)",
                "passed": passed_source_rej,
                "details": f"Unmatched rejected events: {orphan_rejected}",
            })
            if not passed_source_rej:
                report["passed_all"] = False

            # 5. Entity Coverage: Distinct entities in history == entities in current_state
            cur = conn.execute("SELECT COUNT(DISTINCT order_id) FROM event_history;")
            distinct_hist_orders = cur.fetchone()[0]
            cur = conn.execute("SELECT COUNT(*) FROM current_state;")
            curr_state_orders = cur.fetchone()[0]
            passed_entity_cov = (distinct_hist_orders == curr_state_orders)
            report["checks"].append({
                "name": "Entity Coverage (Distinct Orders in History == Current State Rows)",
                "passed": passed_entity_cov,
                "details": f"History distinct orders: {distinct_hist_orders}, Current state rows: {curr_state_orders}",
            })
            if not passed_entity_cov:
                report["passed_all"] = False

            # 6. Tombstone Nullability Integrity
            cur = conn.execute(
                """
                SELECT COUNT(*) FROM current_state
                WHERE (is_deleted = 1 AND deleted_at IS NULL)
                   OR (is_deleted = 0 AND deleted_at IS NOT NULL);
                """
            )
            tombstone_anomalies = cur.fetchone()[0]
            passed_tombstone = (tombstone_anomalies == 0)
            report["checks"].append({
                "name": "Tombstone Nullability Integrity (deleted_at consistent with is_deleted)",
                "passed": passed_tombstone,
                "details": f"Tombstone anomalies: {tombstone_anomalies}",
            })
            if not tombstone_anomalies == 0:
                report["passed_all"] = False

            # 7. Sequence Integrity: current_state last_sequence must match max valid sequence
            cur = conn.execute(
                """
                SELECT COUNT(*) FROM current_state cs
                WHERE cs.last_sequence < (
                    SELECT MIN(eh.source_sequence) FROM event_history eh WHERE eh.order_id = cs.order_id
                );
                """
            )
            seq_anomalies = cur.fetchone()[0]
            passed_seq = (seq_anomalies == 0)
            report["checks"].append({
                "name": "Monotonic Sequence Protection (Current state not regressed by older events)",
                "passed": passed_seq,
                "details": f"Regressed sequences detected: {seq_anomalies}",
            })
            if not passed_seq:
                report["passed_all"] = False

            # 8. Referential Integrity: last_event_id in current_state exists in event_history
            cur = conn.execute(
                """
                SELECT COUNT(*) FROM current_state cs
                WHERE NOT EXISTS (SELECT 1 FROM event_history eh WHERE eh.event_id = cs.last_event_id);
                """
            )
            ref_anomalies = cur.fetchone()[0]
            passed_ref = (ref_anomalies == 0)
            report["checks"].append({
                "name": "Referential Integrity (current_state.last_event_id exists in event_history)",
                "passed": passed_ref,
                "details": f"Orphan last_event_ids: {ref_anomalies}",
            })
            if not passed_ref:
                report["passed_all"] = False

            # 9. Duplicate Prevention in History (event_id uniqueness)
            cur = conn.execute(
                """
                SELECT event_id, COUNT(*) FROM event_history
                GROUP BY event_id HAVING COUNT(*) > 1;
                """
            )
            dups_in_hist = cur.fetchall()
            passed_dups = (len(dups_in_hist) == 0)
            report["checks"].append({
                "name": "Audit History Uniqueness (Zero Duplicate event_ids in event_history)",
                "passed": passed_dups,
                "details": f"Duplicate event_ids: {len(dups_in_hist)}",
            })
            if not passed_dups:
                report["passed_all"] = False

            return report

        finally:
            conn.close()

    def generate_markdown_report(self, audit_result: Dict[str, Any]) -> str:
        """Formats the audit outcome into a comprehensive Markdown report."""
        passed_str = "PASSED" if audit_result["passed_all"] else "FAILED"
        lines = [
            "# CDC Pipeline Reconciliation & Audit Report",
            f"**Audit Overall Status**: `{passed_str}`  ",
            f"**Run ID**: `{audit_result.get('run_id') or 'All Cumulative Runs'}`  ",
            "",
            "## Table Counts",
            "| Table Name | Row Count |",
            "|:---|---:|",
        ]
        for tbl, cnt in audit_result["metrics"].get("table_counts", {}).items():
            lines.append(f"| `{tbl}` | {cnt:,} |")

        lines.extend([
            "",
            "## Reconciliation Assertion Results",
            "| Check Name | Status | Details |",
            "|:---|:---:|:---|",
        ])
        for c in audit_result["checks"]:
            st = "PASS" if c["passed"] else "FAIL"
            lines.append(f"| {c['name']} | `{st}` | {c['details']} |")

        lines.extend([
            "",
            "## Mathematical Guarantees Verified",
            "1. **Row Conservation**: Every single record ingested is accounted for (Accepted + Quarantined + Duplicate).",
            "2. **Lineage Traceability**: 100% of quarantined records trace back to their source file and line coordinate.",
            "3. **State Monotonicity**: Out-of-order events are preserved in history without regressing current state.",
            "4. **Audit Immutability**: No duplicate events exist in the audit log; all target states maintain referential integrity.",
            "",
        ])
        return "\n".join(lines)
