"""
Reconciliation & Data Quality Audit CLI.
Runs deep audit checks against current_state, event_history, rejected_events, and pipeline_runs.
Outputs reports to console and writes artifacts to reports/reconciliation_report.txt and reports/validation_report.md.
"""

import argparse
from pathlib import Path
import sys

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.database import DatabaseManager
from src.reconciler import ReconciliationAuditor


def main():
    parser = argparse.ArgumentParser(description="CDC Pipeline Reconciliation & Audit Runner")
    parser.add_argument("--db", type=str, default="data/cdc_pipeline.db", help="SQLite database path")
    parser.add_argument("--run-id", type=str, default=None, help="Optional specific run_id to audit")
    parser.add_argument("--out-dir", type=str, default="reports", help="Directory to save report files")
    args = parser.parse_args()

    db_path = Path(args.db)
    if not db_path.exists():
        print(f"Error: Database file '{args.db}' not found.", file=sys.stderr)
        sys.exit(1)

    db = DatabaseManager(str(db_path))
    auditor = ReconciliationAuditor(db)
    result = auditor.run_full_audit(run_id=args.run_id)
    report_md = auditor.generate_markdown_report(result)

    print(report_md)

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_file = out_dir / "reconciliation_report.md"
    with open(report_file, "w", encoding="utf-8") as f:
        f.write(report_md)
    print(f"\nSaved Markdown reconciliation report to: {report_file}")

    if not result["passed_all"]:
        print("\n[WARNING] Some reconciliation checks failed!", file=sys.stderr)
        sys.exit(2)
    else:
        print("\n[SUCCESS] All reconciliation checks passed with 100% data integrity!")


if __name__ == "__main__":
    main()
