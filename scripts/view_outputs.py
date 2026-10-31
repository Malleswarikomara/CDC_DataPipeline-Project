"""
Utility script to display formatted output tables in the terminal.
Usage:
    python scripts/view_outputs.py
"""

from pathlib import Path
import csv
import sys

BASE_DIR = Path(__file__).resolve().parent.parent
OUTPUT_DIR = BASE_DIR / "data" / "output"


def print_table(csv_path: Path, max_rows: int = 10):
    if not csv_path.exists():
        print(f"File not found: {csv_path.name}")
        return

    print("=" * 100)
    print(f" TABLE: {csv_path.stem.upper()} ({csv_path.name})")
    print("=" * 100)

    with open(csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader, None)
        if not headers:
            print("Empty file")
            return

        rows = []
        for i, row in enumerate(reader):
            if i >= max_rows:
                break
            rows.append(row)

    # Compute column widths
    col_widths = [len(h) for h in headers]
    for row in rows:
        for idx, val in enumerate(row):
            display_val = val[:28] + ".." if len(val) > 30 else val
            col_widths[idx] = max(col_widths[idx], len(display_val))

    # Print header
    header_str = " | ".join(f"{h:<{col_widths[i]}}" for i, h in enumerate(headers))
    sep_str = "-+-".join("-" * col_widths[i] for i in range(len(headers)))
    print(header_str)
    print(sep_str)

    # Print rows
    for row in rows:
        row_str = " | ".join(
            f"{(val[:28] + '..' if len(val) > 30 else val):<{col_widths[i]}}"
            for i, val in enumerate(row)
        )
        print(row_str)

    print(f"\n[Showing up to {max_rows} rows from: {csv_path}]\n")


def main():
    tables = [
        OUTPUT_DIR / "current_state.csv",
        OUTPUT_DIR / "event_history.csv",
        OUTPUT_DIR / "rejected_events.csv",
        OUTPUT_DIR / "pipeline_runs.csv",
    ]

    for tbl in tables:
        print_table(tbl, max_rows=6)


if __name__ == "__main__":
    main()
