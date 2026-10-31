"""
Command-line interface to execute the CDC pipeline against input files.
Usage:
    python scripts/run_pipeline.py --file data/sample/starter_sample.json
    python scripts/run_pipeline.py --file data/large/cdc_events_10000.jsonl --batch-size 2000
"""

import argparse
from pathlib import Path
import sys

# Ensure project root is in sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.config import PipelineConfig
from src.pipeline import CDCPipeline
from src.reconciler import ReconciliationAuditor


def main():
    parser = argparse.ArgumentParser(description="Run the CDC Ingestion & Processing Pipeline")
    parser.add_argument("--file", "-f", type=str, required=True, help="Path to input data file (JSON, JSONL, or CSV)")
    parser.add_argument("--db", type=str, default="data/cdc_pipeline.db", help="SQLite database path")
    parser.add_argument("--batch-size", "-b", type=int, default=1000, help="Streaming batch size")
    parser.add_argument("--log-level", type=str, default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    parser.add_argument("--reconcile", action="store_true", help="Run full reconciliation audit after processing")
    args = parser.parse_args()

    input_file = Path(args.file)
    if not input_file.exists():
        print(f"Error: Input file '{args.file}' does not exist.", file=sys.stderr)
        sys.exit(1)

    config = PipelineConfig(
        db_path=args.db,
        batch_size=args.batch_size,
        log_level=args.log_level,
    )

    pipeline = CDCPipeline(config=config)
    tracker = pipeline.run(file_path=input_file)

    if args.reconcile:
        print("\n" + "=" * 80)
        print("RUNNING POST-EXECUTION RECONCILIATION AUDIT")
        print("=" * 80)
        auditor = ReconciliationAuditor(pipeline.db)
        audit_res = auditor.run_full_audit(run_id=tracker.run_id)
        report_md = auditor.generate_markdown_report(audit_res)
        print(report_md)


if __name__ == "__main__":
    main()
