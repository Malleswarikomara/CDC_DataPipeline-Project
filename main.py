"""
Main Application Entry Point for the Change Data Capture (CDC) Pipeline.
Demonstrates end-to-end processing of CDC events, edge-case quarantine,
deduplication, out-of-order handling, and mathematical reconciliation.
"""

import argparse
from pathlib import Path
import sys

from src.config import PipelineConfig, BASE_DIR
from src.pipeline import CDCPipeline
from src.reconciler import ReconciliationAuditor
from src.database import DatabaseManager


def run_demonstration():
    """Runs a complete end-to-end multi-stage demonstration."""
    print("=" * 80)
    print("       DATA ENGINEERING CHANGE DATA CAPTURE (CDC) PIPELINE DEMO")
    print("=" * 80)

    db_path = str(BASE_DIR / "data" / "demo_cdc_pipeline.db")
    # Clean previous demo database if exists for clean demonstration
    if Path(db_path).exists():
        try:
            Path(db_path).unlink()
        except Exception:
            pass

    config = PipelineConfig(db_path=db_path, log_level="INFO")
    pipeline = CDCPipeline(config=config)

    # Stage 1: Process Starter Sample (Happy path, out-of-order, updates, deletes, duplicates)
    starter_file = BASE_DIR / "data" / "sample" / "starter_sample.json"
    print("\n[STAGE 1/4] Processing Starter Sample Events (Lifecycle + Out-of-Order + Dedup)...")
    tracker1 = pipeline.run(starter_file)

    # Stage 2: Process Edge Cases Sample (Quarantine & Rejections)
    edge_file = BASE_DIR / "data" / "sample" / "edge_cases_sample.json"
    print("\n[STAGE 2/4] Processing Edge Cases Sample (Quarantine Routing & Schema Drift)...")
    tracker2 = pipeline.run(edge_file)

    # Stage 3: Test Idempotency (Re-running Starter Sample)
    print("\n[STAGE 3/4] Re-running Starter Sample to Verify Idempotency...")
    tracker3 = pipeline.run(starter_file)
    print(f"  -> Re-run Duplicate Count: {tracker3.records_duplicate} (Expected: {tracker1.records_read})")
    print(f"  -> Re-run New Target Mutations: {tracker3.inserts_applied + tracker3.updates_applied + tracker3.deletes_applied} (Expected: 0)")

    # Stage 4: Comprehensive Reconciliation & Integrity Audit
    print("\n[STAGE 4/4] Executing Deep Mathematical Reconciliation & Integrity Audit...")
    auditor = ReconciliationAuditor(pipeline.db)
    audit_res = auditor.run_full_audit()
    report_md = auditor.generate_markdown_report(audit_res)
    print("\n" + report_md)

    # Save report artifact
    report_path = BASE_DIR / "reports" / "validation_report.md"
    report_path.parent.mkdir(parents=True, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        f.write(report_md)
    print(f"\n[REPORT PERSISTED] Validation report written to: {report_path}")

    # Export sample target tables to data/output/ for immediate review
    export_target_tables(pipeline.db, BASE_DIR / "data" / "output")

    print("\n" + "=" * 80)
    print("                  DEMONSTRATION COMPLETED SUCCESSFULLY")
    print("=" * 80)


def export_target_tables(db: DatabaseManager, output_dir: Path):
    """Exports current_state, event_history, and rejected_events to CSV for inspection."""
    import csv
    output_dir.mkdir(parents=True, exist_ok=True)
    conn = db.get_connection()
    try:
        for tbl in ["current_state", "event_history", "rejected_events", "pipeline_runs"]:
            cur = conn.execute(f"SELECT * FROM {tbl};")
            rows = [dict(r) for r in cur.fetchall()]
            if rows:
                out_file = output_dir / f"{tbl}.csv"
                with open(out_file, "w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
                    writer.writeheader()
                    writer.writerows(rows)
                print(f"  -> Exported {len(rows):,} rows of '{tbl}' to: {out_file}")
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline Runner")
    parser.add_argument("--file", "-f", type=str, default=None, help="Custom input file to process")
    parser.add_argument("--db", type=str, default="data/cdc_pipeline.db", help="SQLite DB path")
    args = parser.parse_args()

    if args.file:
        config = PipelineConfig(db_path=args.db)
        pipeline = CDCPipeline(config=config)
        pipeline.run(args.file)
    else:
        run_demonstration()


if __name__ == "__main__":
    main()
