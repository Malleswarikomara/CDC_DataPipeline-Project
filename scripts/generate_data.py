"""
Synthetic CDC Event Data Generator.
Generates deterministic, reproducible datasets with fixed random seeds.
Produces:
 1. Small starter samples (JSON and CSV) for immediate review.
 2. Targeted edge-case samples containing all required negative scenarios.
 3. Configurable large-scale datasets (10,000 to 100,000+ records) for stress and performance benchmarking.
"""

import argparse
from datetime import datetime, timedelta
import json
import random
from pathlib import Path
from typing import List, Dict, Any
import csv


def generate_starter_dataset() -> List[Dict[str, Any]]:
    """Generates a clean starter dataset demonstrating complete entity lifecycles, dedup, and out-of-order."""
    base_time = datetime(2026, 10, 30, 10, 0, 0)
    events = [
        # Entity 1: Complete happy path lifecycle
        {
            "event_id": "evt_order_101_1",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=1)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-101",
            "customer_id": "CUST-001",
            "order_status": "PENDING",
            "total_amount": 120.50,
            "currency": "USD",
            "shipping_address": "123 Market St, San Francisco, CA",
            "item_count": 2,
        },
        {
            "event_id": "evt_order_101_2",
            "event_type": "UPDATE",
            "event_timestamp": (base_time + timedelta(minutes=5)).isoformat() + "Z",
            "source_sequence": 2,
            "order_id": "ORD-101",
            "customer_id": "CUST-001",
            "order_status": "CONFIRMED",
            "total_amount": 120.50,
            "currency": "USD",
            "shipping_address": "123 Market St, San Francisco, CA",
            "item_count": 2,
        },
        {
            "event_id": "evt_order_101_3",
            "event_type": "UPDATE",
            "event_timestamp": (base_time + timedelta(minutes=10)).isoformat() + "Z",
            "source_sequence": 3,
            "order_id": "ORD-101",
            "customer_id": "CUST-001",
            "order_status": "SHIPPED",
            "total_amount": 120.50,
            "currency": "USD",
            "shipping_address": "123 Market St, San Francisco, CA",
            "item_count": 2,
        },
        {
            "event_id": "evt_order_101_4",
            "event_type": "UPDATE",
            "event_timestamp": (base_time + timedelta(minutes=15)).isoformat() + "Z",
            "source_sequence": 4,
            "order_id": "ORD-101",
            "customer_id": "CUST-001",
            "order_status": "DELIVERED",
            "total_amount": 120.50,
            "currency": "USD",
            "shipping_address": "123 Market St, San Francisco, CA",
            "item_count": 2,
        },

        # Entity 2: Order created then deleted (tombstoned)
        {
            "event_id": "evt_order_102_1",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=2)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-102",
            "customer_id": "CUST-002",
            "order_status": "PENDING",
            "total_amount": 49.99,
            "currency": "USD",
            "shipping_address": "456 Broadway, New York, NY",
            "item_count": 1,
        },
        {
            "event_id": "evt_order_102_2",
            "event_type": "DELETE",
            "event_timestamp": (base_time + timedelta(minutes=7)).isoformat() + "Z",
            "source_sequence": 2,
            "order_id": "ORD-102",
            "customer_id": "CUST-002",
            "order_status": "CANCELLED",
            "total_amount": 49.99,
            "currency": "USD",
            "shipping_address": "456 Broadway, New York, NY",
            "item_count": 1,
        },

        # Entity 3: Out-of-order arrival scenario
        # Seq 1 arrives -> Seq 3 arrives -> Seq 2 (late) arrives
        {
            "event_id": "evt_order_103_1",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=3)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-103",
            "customer_id": "CUST-003",
            "order_status": "PENDING",
            "total_amount": 250.00,
            "currency": "USD",
            "shipping_address": "789 Pine St, Seattle, WA",
            "item_count": 5,
        },
        {
            "event_id": "evt_order_103_3",
            "event_type": "UPDATE",
            "event_timestamp": (base_time + timedelta(minutes=20)).isoformat() + "Z",
            "source_sequence": 3,
            "order_id": "ORD-103",
            "customer_id": "CUST-003",
            "order_status": "SHIPPED",
            "total_amount": 250.00,
            "currency": "USD",
            "shipping_address": "789 Pine St, Seattle, WA",
            "item_count": 5,
        },
        {
            "event_id": "evt_order_103_2",
            "event_type": "UPDATE",
            "event_timestamp": (base_time + timedelta(minutes=10)).isoformat() + "Z",
            "source_sequence": 2,  # LATE / OUT-OF-ORDER! Must not regress state from SHIPPED to CONFIRMED
            "order_id": "ORD-103",
            "customer_id": "CUST-003",
            "order_status": "CONFIRMED",
            "total_amount": 250.00,
            "currency": "USD",
            "shipping_address": "789 Pine St, Seattle, WA",
            "item_count": 5,
        },

        # Duplicate event check: exact duplicate of evt_order_101_1
        {
            "event_id": "evt_order_101_1",  # DUPLICATE!
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=1)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-101",
            "customer_id": "CUST-001",
            "order_status": "PENDING",
            "total_amount": 120.50,
            "currency": "USD",
            "shipping_address": "123 Market St, San Francisco, CA",
            "item_count": 2,
        },

        # Entity 4: Currency and address update
        {
            "event_id": "evt_order_104_1",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=4)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-104",
            "customer_id": "CUST-004",
            "order_status": "PENDING",
            "total_amount": 310.00,
            "currency": "EUR",
            "shipping_address": "10 Rue de Rivoli, Paris, France",
            "item_count": 3,
        },
        {
            "event_id": "evt_order_104_2",
            "event_type": "UPDATE",
            "event_timestamp": (base_time + timedelta(minutes=12)).isoformat() + "Z",
            "source_sequence": 2,
            "order_id": "ORD-104",
            "customer_id": "CUST-004",
            "order_status": "CONFIRMED",
            "total_amount": 325.00,  # Price updated
            "currency": "EUR",
            "shipping_address": "12 Rue de Rivoli, Paris, France",  # Address updated
            "item_count": 3,
        },
    ]
    return events


def generate_edge_cases_dataset() -> List[Dict[str, Any]]:
    """Generates a dataset specifically designed to trigger and test every required edge case."""
    base_time = datetime(2026, 10, 30, 11, 0, 0)
    events = [
        # Normal setup record for state transition test
        {
            "event_id": "edge_valid_deliv_1",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=1)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-EDGE-01",
            "customer_id": "CUST-EDGE",
            "order_status": "DELIVERED",
            "total_amount": 100.0,
            "currency": "USD",
            "shipping_address": "Edge Test St",
            "item_count": 1,
        },
        # 1. Edge Case: Invalid Status Transition (Terminal DELIVERED -> PENDING)
        {
            "event_id": "edge_invalid_transition",
            "event_type": "UPDATE",
            "event_timestamp": (base_time + timedelta(minutes=2)).isoformat() + "Z",
            "source_sequence": 2,
            "order_id": "ORD-EDGE-01",
            "customer_id": "CUST-EDGE",
            "order_status": "PENDING",
            "total_amount": 100.0,
            "currency": "USD",
            "shipping_address": "Edge Test St",
            "item_count": 1,
        },
        # 2. Edge Case: Missing Required Field (customer_id is None)
        {
            "event_id": "edge_missing_field",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=3)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-EDGE-02",
            "customer_id": None,  # INVALID!
            "order_status": "PENDING",
            "total_amount": 50.0,
            "currency": "USD",
            "shipping_address": "Missing Field St",
            "item_count": 1,
        },
        # 3. Edge Case: Negative Amount
        {
            "event_id": "edge_negative_amount",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=4)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-EDGE-03",
            "customer_id": "CUST-EDGE",
            "order_status": "PENDING",
            "total_amount": -89.99,  # INVALID!
            "currency": "USD",
            "shipping_address": "Negative St",
            "item_count": 1,
        },
        # 4. Edge Case: Malformed Timestamp
        {
            "event_id": "edge_bad_timestamp",
            "event_type": "INSERT",
            "event_timestamp": "not-a-valid-timestamp-string",  # INVALID!
            "source_sequence": 1,
            "order_id": "ORD-EDGE-04",
            "customer_id": "CUST-EDGE",
            "order_status": "PENDING",
            "total_amount": 25.0,
            "currency": "USD",
            "shipping_address": "Timestamp St",
            "item_count": 1,
        },
        # 5. Edge Case: Invalid Sequence (sequence <= 0)
        {
            "event_id": "edge_bad_sequence",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=5)).isoformat() + "Z",
            "source_sequence": -1,  # INVALID!
            "order_id": "ORD-EDGE-05",
            "customer_id": "CUST-EDGE",
            "order_status": "PENDING",
            "total_amount": 45.0,
            "currency": "USD",
            "shipping_address": "Sequence St",
            "item_count": 1,
        },
        # 6. Edge Case: Invalid Event Type
        {
            "event_id": "edge_bad_event_type",
            "event_type": "UPSERT",  # INVALID! Only INSERT, UPDATE, DELETE allowed
            "event_timestamp": (base_time + timedelta(minutes=6)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-EDGE-06",
            "customer_id": "CUST-EDGE",
            "order_status": "PENDING",
            "total_amount": 75.0,
            "currency": "USD",
            "shipping_address": "Event Type St",
            "item_count": 1,
        },
        # 7. Edge Case: Unexpected Schema Column Drift
        {
            "event_id": "edge_unexpected_column",
            "event_type": "INSERT",
            "event_timestamp": (base_time + timedelta(minutes=7)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-EDGE-07",
            "customer_id": "CUST-EDGE",
            "order_status": "PENDING",
            "total_amount": 90.0,
            "currency": "USD",
            "shipping_address": "Drift St",
            "item_count": 1,
            "malicious_extra_field": "hacker_payload",  # INVALID SCHEMA DRIFT!
        },
        # 8. Edge Case: DELETE on non-existent entity
        {
            "event_id": "edge_delete_ghost",
            "event_type": "DELETE",  # INVALID REFERENCE KEY!
            "event_timestamp": (base_time + timedelta(minutes=8)).isoformat() + "Z",
            "source_sequence": 1,
            "order_id": "ORD-NONEXISTENT-999",
            "customer_id": "CUST-UNKNOWN",
            "order_status": "CANCELLED",
            "total_amount": 0.0,
            "currency": "USD",
            "shipping_address": "Ghost St",
            "item_count": 0,
        },
    ]
    return events


def generate_scalable_dataset(count: int = 100000, seed: int = 42) -> List[Dict[str, Any]]:
    """
    Generates a large-scale reproducible synthetic dataset with realistic distribution.
    Parameters:
      count: Total number of records to generate (e.g. 100,000)
      seed: Random seed for deterministic reproducibility
    """
    rng = random.Random(seed)
    base_time = datetime(2026, 10, 30, 0, 0, 0)
    currencies = ["USD", "EUR", "GBP", "CAD", "AUD"]

    # We will simulate order lifecycles across a pool of orders
    num_orders = max(count // 3, 100)
    orders_state: Dict[str, Dict[str, Any]] = {}
    events: List[Dict[str, Any]] = []

    # Pre-generate order IDs
    order_ids = [f"ORD-{i:07d}" for i in range(1, num_orders + 1)]

    while len(events) < count:
        i = len(events)
        row_id = i + 1
        current_time = base_time + timedelta(seconds=i * 2)

        # 1. Edge case injection (~2% rate)
        rand_val = rng.random()
        if rand_val < 0.005:
            # Negative amount
            events.append({
                "event_id": f"evt_stress_{row_id}",
                "event_type": "INSERT",
                "event_timestamp": current_time.isoformat() + "Z",
                "source_sequence": 1,
                "order_id": f"ORD-ANOMALY-{row_id}",
                "customer_id": f"CUST-{rng.randint(1, 1000):05d}",
                "order_status": "PENDING",
                "total_amount": -round(rng.uniform(10.0, 500.0), 2),
                "currency": rng.choice(currencies),
                "shipping_address": "Anomaly St",
                "item_count": 1,
            })
            continue

        elif rand_val < 0.01:
            # Missing customer_id
            events.append({
                "event_id": f"evt_stress_{row_id}",
                "event_type": "INSERT",
                "event_timestamp": current_time.isoformat() + "Z",
                "source_sequence": 1,
                "order_id": f"ORD-ANOMALY-{row_id}",
                "customer_id": "",
                "order_status": "PENDING",
                "total_amount": round(rng.uniform(10.0, 500.0), 2),
                "currency": rng.choice(currencies),
                "shipping_address": "Missing Customer St",
                "item_count": 1,
            })
            continue

        elif rand_val < 0.015:
            # Duplicate event injection (re-deliver a recently generated valid event)
            if events and "event_id" in events[-1]:
                dup_event = dict(events[-1])
                events.append(dup_event)
                continue

        # 2. Pick an order
        order_id = rng.choice(order_ids)

        if order_id not in orders_state or orders_state[order_id]["is_deleted"]:
            # INSERT / RE-INSERT
            seq = orders_state[order_id]["seq"] + 1 if order_id in orders_state else 1
            cust_id = f"CUST-{rng.randint(1, 10000):05d}"
            status = "PENDING"
            amount = round(rng.uniform(15.0, 1500.0), 2)
            curr = rng.choice(currencies)
            addr = f"{rng.randint(100, 999)} Main St, City {rng.randint(1, 50)}"
            cnt = rng.randint(1, 8)
            orders_state[order_id] = {
                "seq": seq,
                "customer_id": cust_id,
                "status": status,
                "amount": amount,
                "currency": curr,
                "address": addr,
                "item_count": cnt,
                "is_deleted": False,
            }
            events.append({
                "event_id": f"evt_stress_{row_id}",
                "event_type": "INSERT",
                "event_timestamp": current_time.isoformat() + "Z",
                "source_sequence": seq,
                "order_id": order_id,
                "customer_id": cust_id,
                "order_status": status,
                "total_amount": amount,
                "currency": curr,
                "shipping_address": addr,
                "item_count": cnt,
            })
        else:
            state = orders_state[order_id]

            # Out-of-order check (~2% rate)
            is_out_of_order = rng.random() < 0.02

            if is_out_of_order and state["seq"] > 1:
                # Late arriving event with sequence 1 or 2
                late_seq = rng.randint(1, state["seq"])
                events.append({
                    "event_id": f"evt_stress_{row_id}",
                    "event_type": "UPDATE",
                    "event_timestamp": (current_time - timedelta(minutes=30)).isoformat() + "Z",
                    "source_sequence": late_seq,
                    "order_id": order_id,
                    "customer_id": state["customer_id"],
                    "order_status": "CONFIRMED",
                    "total_amount": state["amount"],
                    "currency": state["currency"],
                    "shipping_address": state["address"],
                    "item_count": state["item_count"],
                })
                continue

            state["seq"] += 1
            curr_st = state["status"]

            # Progress lifecycle
            if curr_st == "PENDING":
                next_st = rng.choice(["CONFIRMED", "CANCELLED"])
            elif curr_st == "CONFIRMED":
                next_st = rng.choice(["SHIPPED", "CANCELLED"])
            elif curr_st == "SHIPPED":
                next_st = "DELIVERED"
            else:
                next_st = curr_st

            state["status"] = next_st
            ev_type = "DELETE" if next_st == "CANCELLED" and rng.random() < 0.5 else "UPDATE"
            if ev_type == "DELETE":
                state["is_deleted"] = True

            events.append({
                "event_id": f"evt_stress_{row_id}",
                "event_type": ev_type,
                "event_timestamp": current_time.isoformat() + "Z",
                "source_sequence": state["seq"],
                "order_id": order_id,
                "customer_id": state["customer_id"],
                "order_status": next_st,
                "total_amount": state["amount"],
                "currency": state["currency"],
                "shipping_address": state["address"],
                "item_count": state["item_count"],
            })

    return events


def save_as_json(data: List[Dict[str, Any]], output_path: Path):
    """Saves records to formatted JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    print(f"Saved {len(data):,} JSON records to: {output_path}")


def save_as_jsonl(data: List[Dict[str, Any]], output_path: Path):
    """Saves records to streaming JSONL file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
    print(f"Saved {len(data):,} JSONL records to: {output_path}")


def save_as_csv(data: List[Dict[str, Any]], output_path: Path):
    """Saves records to CSV file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    if not data:
        return
    # Collect union of all keys preserving order
    fieldnames = []
    for item in data:
        for k in item.keys():
            if k not in fieldnames:
                fieldnames.append(k)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    print(f"Saved {len(data):,} CSV records to: {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Synthetic CDC Event Data Generator")
    parser.add_argument("--count", type=int, default=1000, help="Number of records for scalable dataset")
    parser.add_argument("--seed", type=int, default=42, help="Deterministic random seed")
    parser.add_argument("--format", choices=["json", "jsonl", "csv", "all"], default="all")
    args = parser.parse_args()

    project_root = Path(__file__).resolve().parent.parent

    # 1. Generate Starter Samples
    starter = generate_starter_dataset()
    save_as_json(starter, project_root / "data" / "sample" / "starter_sample.json")
    save_as_csv(starter, project_root / "data" / "sample" / "starter_sample.csv")

    # 2. Generate Edge Cases Sample
    edge_cases = generate_edge_cases_dataset()
    save_as_json(edge_cases, project_root / "data" / "sample" / "edge_cases_sample.json")
    save_as_csv(edge_cases, project_root / "data" / "sample" / "edge_cases_sample.csv")

    # 3. Generate Scalable Dataset
    print(f"Generating scalable dataset with {args.count:,} records (seed={args.seed})...")
    large_data = generate_scalable_dataset(count=args.count, seed=args.seed)
    
    if args.format in ("jsonl", "all"):
        save_as_jsonl(large_data, project_root / "data" / "large" / f"cdc_events_{args.count}.jsonl")
    if args.format in ("csv", "all") and args.count <= 100000:
        save_as_csv(large_data, project_root / "data" / "large" / f"cdc_events_{args.count}.csv")

    print("Synthetic data generation finished successfully!")


if __name__ == "__main__":
    main()
