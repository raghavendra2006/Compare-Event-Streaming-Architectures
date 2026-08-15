"""
Latency Analyzer — Statistical Analysis of E2E Latency Data
=============================================================
Reads latency CSV files from benchmark runs and computes:
  - p50 (median), p95, p99 percentile latencies
  - Mean, min, max
  - Throughput estimates

Outputs results as JSON and formatted tables.
"""

import argparse
import csv
import json
import os
import sys
from pathlib import Path


def read_latency_csv(csv_path: str) -> list:
    """Read latency samples from a CSV file."""
    samples = []
    try:
        with open(csv_path, "r") as f:
            reader = csv.DictReader(f)
            for row in reader:
                try:
                    samples.append(int(row["latency_ns"]))
                except (ValueError, KeyError):
                    continue
    except FileNotFoundError:
        print(f"  ⚠ File not found: {csv_path}")
    return samples


def compute_percentiles(samples: list) -> dict:
    """Compute percentile statistics from latency samples."""
    if not samples:
        return {}

    sorted_samples = sorted(samples)
    n = len(sorted_samples)

    def percentile(p):
        idx = int(n * p / 100)
        return sorted_samples[min(idx, n - 1)]

    return {
        "sample_count": n,
        "min_ns": sorted_samples[0],
        "min_ms": round(sorted_samples[0] / 1_000_000, 3),
        "max_ns": sorted_samples[-1],
        "max_ms": round(sorted_samples[-1] / 1_000_000, 3),
        "mean_ns": int(sum(sorted_samples) / n),
        "mean_ms": round(sum(sorted_samples) / n / 1_000_000, 3),
        "p50_ns": percentile(50),
        "p50_ms": round(percentile(50) / 1_000_000, 3),
        "p95_ns": percentile(95),
        "p95_ms": round(percentile(95) / 1_000_000, 3),
        "p99_ns": percentile(99),
        "p99_ms": round(percentile(99) / 1_000_000, 3),
    }


def print_table(title: str, results: dict):
    """Print a formatted comparison table."""
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")
    print(f"  {'Metric':<25} {'Value':>15}")
    print(f"  {'-' * 25} {'-' * 15}")
    for key, value in results.items():
        if key.endswith("_ms"):
            print(f"  {key:<25} {value:>12.3f} ms")
        elif key.endswith("_ns"):
            print(f"  {key:<25} {value:>15,} ns")
        else:
            print(f"  {key:<25} {value:>15}")
    print(f"{'=' * 60}")


def main():
    parser = argparse.ArgumentParser(description="Analyze latency benchmark results")
    parser.add_argument(
        "--results-dir",
        default="benchmarks/results",
        help="Directory containing latency CSV files",
    )
    args = parser.parse_args()

    results_dir = Path(args.results_dir)
    if not results_dir.exists():
        print(f"Results directory not found: {results_dir}")
        sys.exit(1)

    summary = {
        "kafka": {},
        "rabbitmq": {},
    }

    # Process Kafka latency files
    print("\n" + "=" * 60)
    print("  KAFKA LATENCY ANALYSIS")
    print("=" * 60)

    kafka_all_samples = []
    for csv_file in sorted(results_dir.glob("latency_kafka_*.csv")):
        group_name = csv_file.stem.replace("latency_kafka_", "")
        samples = read_latency_csv(str(csv_file))
        kafka_all_samples.extend(samples)

        if samples:
            stats = compute_percentiles(samples)
            print_table(f"Kafka — {group_name}", stats)
            summary["kafka"][group_name] = stats

    if kafka_all_samples:
        kafka_aggregate = compute_percentiles(kafka_all_samples)
        print_table("Kafka — AGGREGATE (all consumer groups)", kafka_aggregate)
        summary["kafka"]["aggregate"] = kafka_aggregate

    # Process RabbitMQ latency files
    print("\n" + "=" * 60)
    print("  RABBITMQ LATENCY ANALYSIS")
    print("=" * 60)

    rabbitmq_all_samples = []
    for csv_file in sorted(results_dir.glob("latency_rabbitmq_*.csv")):
        queue_name = csv_file.stem.replace("latency_rabbitmq_", "")
        samples = read_latency_csv(str(csv_file))
        rabbitmq_all_samples.extend(samples)

        if samples:
            stats = compute_percentiles(samples)
            print_table(f"RabbitMQ — {queue_name}", stats)
            summary["rabbitmq"][queue_name] = stats

    if rabbitmq_all_samples:
        rabbitmq_aggregate = compute_percentiles(rabbitmq_all_samples)
        print_table("RabbitMQ — AGGREGATE (all queues)", rabbitmq_aggregate)
        summary["rabbitmq"]["aggregate"] = rabbitmq_aggregate

    # Comparison table
    if kafka_all_samples and rabbitmq_all_samples:
        ka = summary["kafka"]["aggregate"]
        ra = summary["rabbitmq"]["aggregate"]

        print(f"\n{'=' * 70}")
        print("  HEAD-TO-HEAD COMPARISON")
        print(f"{'=' * 70}")
        print(f"  {'Metric':<20} {'Kafka':>15} {'RabbitMQ':>15} {'Winner':>12}")
        print(f"  {'-' * 20} {'-' * 15} {'-' * 15} {'-' * 12}")

        for metric in ["p50_ms", "p95_ms", "p99_ms", "mean_ms"]:
            k_val = ka.get(metric, 0)
            r_val = ra.get(metric, 0)
            winner = "Kafka" if k_val <= r_val else "RabbitMQ"
            print(f"  {metric:<20} {k_val:>12.3f} ms {r_val:>12.3f} ms {winner:>12}")

        print(f"  {'sample_count':<20} {ka.get('sample_count', 0):>15} {ra.get('sample_count', 0):>15}")
        print(f"{'=' * 70}")

    # Save summary
    summary_path = results_dir / "latency_summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\n✓ Summary saved to: {summary_path}")


if __name__ == "__main__":
    main()
