# Large & Scalable Benchmark Datasets

This directory contains scalable datasets used for high-volume benchmarking, throughput analysis, and stress testing.

## Included Preview Dataset
- **`cdc_events_1000.jsonl`** (~300 KB): A 1,000-record benchmark preview containing order lifecycles (INSERT, UPDATE, DELETE), out-of-order sequence arrivals, and duplicate replays. Designed to open and preview directly in GitHub without browser performance truncation.

## Generating Larger Datasets On-Demand
You can generate larger datasets (10,000, 100,000, or 1,000,000 records) locally using the scalable generator:

```bash
# Generate 10,000 records (JSONL + CSV)
python scripts/generate_data.py --count 10000 --format all

# Generate 100,000 records (JSONL)
python scripts/generate_data.py --count 100000 --format jsonl

# Run the performance benchmark
python scripts/benchmark_performance.py --file data/large/cdc_events_1000.jsonl
```
