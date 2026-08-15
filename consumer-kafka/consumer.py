"""
Kafka Order Consumer — Manual Offset Commit Consumer
=====================================================
Subscribes to the 'orders' topic with a configurable consumer group ID.
Implements manual offset commits (enable.auto.commit=false) to guarantee
at-least-once delivery semantics.

Writes latency samples to /data/latency_{group_id}.csv for benchmarking.
"""

import csv
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, KafkaException

# =============================================================================
# Configuration
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
ORDER_TOPIC = os.getenv("ORDER_TOPIC_NAME", "orders")
GROUP_ID = os.getenv("GROUP_ID", "default-cg")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

# Logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(f"kafka-consumer-{GROUP_ID}")

# Graceful shutdown
shutdown_flag = False


def handle_signal(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global shutdown_flag
    logger.info("Shutdown signal received (signal=%d). Closing consumer...", signum)
    shutdown_flag = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# =============================================================================
# Health Check
# =============================================================================

def mark_healthy():
    """Create health check marker file."""
    Path("/tmp/consumer_healthy").touch()


# =============================================================================
# Latency Tracking
# =============================================================================

class LatencyTracker:
    """Tracks end-to-end latency and writes samples to CSV."""

    def __init__(self, group_id: str, output_dir: str = "/data"):
        self.group_id = group_id
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / f"latency_kafka_{group_id}.csv"
        self.samples = []
        self.total_messages = 0
        self.start_time = time.time()

        # Initialize CSV
        with open(self.csv_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["message_num", "order_id", "latency_ns", "latency_ms", "timestamp"])

        logger.info("Latency tracker initialized: %s", self.csv_path)

    def record(self, order_id: str, produced_at_ns: int):
        """Record a latency sample."""
        received_at_ns = time.time_ns()
        latency_ns = received_at_ns - produced_at_ns
        latency_ms = latency_ns / 1_000_000
        self.total_messages += 1

        self.samples.append(latency_ns)

        # Write to CSV
        with open(self.csv_path, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow([
                self.total_messages,
                order_id,
                latency_ns,
                f"{latency_ms:.3f}",
                int(time.time() * 1000),
            ])

        return latency_ms

    def get_stats(self) -> dict:
        """Calculate percentile statistics from collected samples."""
        if not self.samples:
            return {}

        sorted_samples = sorted(self.samples)
        n = len(sorted_samples)

        def percentile(p):
            idx = int(n * p / 100)
            return sorted_samples[min(idx, n - 1)]

        elapsed = time.time() - self.start_time
        return {
            "total_messages": n,
            "elapsed_seconds": round(elapsed, 2),
            "throughput_mps": round(n / max(elapsed, 0.001), 2),
            "p50_ns": percentile(50),
            "p50_ms": round(percentile(50) / 1_000_000, 3),
            "p95_ns": percentile(95),
            "p95_ms": round(percentile(95) / 1_000_000, 3),
            "p99_ns": percentile(99),
            "p99_ms": round(percentile(99) / 1_000_000, 3),
            "min_ns": sorted_samples[0],
            "max_ns": sorted_samples[-1],
        }


# =============================================================================
# Business Logic
# =============================================================================

def process_order(order: dict, group_id: str) -> bool:
    """
    Simulate business logic processing for the order.
    Returns True if processing succeeded, False otherwise.
    """
    order_id = order.get("order_id", "unknown")
    amount = order.get("amount", 0)

    if group_id == "inventory-cg":
        # Simulate inventory check
        logger.debug("[inventory] Processing order %s: checking stock for %s",
                      order_id, order.get("product_id"))
    elif group_id == "notification-cg":
        # Simulate notification dispatch
        logger.debug("[notification] Sending confirmation for order %s to user %s",
                      order_id, order.get("user_id"))
    elif group_id == "analytics-cg":
        # Simulate analytics aggregation
        logger.debug("[analytics] Recording order %s: amount=$%.2f", order_id, amount)

    # All processing succeeds for Kafka (no DLX concept here)
    return True


# =============================================================================
# Main Consumer Loop
# =============================================================================

def run_consumer():
    """Main consumer loop with manual offset commit."""
    consumer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "group.id": GROUP_ID,
        "client.id": f"{GROUP_ID}-client",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,           # CRITICAL: manual commits only
        "max.poll.interval.ms": 300000,
        "session.timeout.ms": 30000,
        "fetch.min.bytes": 1,
        "fetch.wait.max.ms": 100,
    }

    consumer = None
    tracker = LatencyTracker(GROUP_ID)

    # Retry connection
    for attempt in range(1, 31):
        try:
            consumer = Consumer(consumer_conf)
            consumer.subscribe([ORDER_TOPIC])
            logger.info(
                "Consumer [%s] subscribed to topic '%s' (servers: %s)",
                GROUP_ID, ORDER_TOPIC, KAFKA_BOOTSTRAP_SERVERS,
            )
            break
        except KafkaException as e:
            logger.warning("Connection attempt %d failed: %s", attempt, e)
            time.sleep(3)
    else:
        logger.error("Failed to connect after 30 attempts. Exiting.")
        sys.exit(1)

    mark_healthy()
    logger.info("=== Kafka Consumer [%s] started ===", GROUP_ID)

    messages_since_commit = 0
    commit_interval = 50  # Commit every N messages
    last_stats_log = time.time()
    stats_log_interval = 30  # Log stats every 30 seconds

    try:
        while not shutdown_flag:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    logger.debug(
                        "End of partition: %s [%d] @ offset %d",
                        msg.topic(), msg.partition(), msg.offset(),
                    )
                    continue
                else:
                    logger.error("Consumer error: %s", msg.error())
                    continue

            # Deserialize message
            try:
                order = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error(
                    "Failed to deserialize message at offset %d: %s",
                    msg.offset(), e,
                )
                # Commit offset to skip bad message
                consumer.commit(asynchronous=False)
                continue

            # Process the order
            success = process_order(order, GROUP_ID)

            # Track latency
            produced_at = order.get("produced_at")
            if produced_at:
                latency_ms = tracker.record(order["order_id"], produced_at)
                logger.debug(
                    "[%s] order=%s partition=%d offset=%d latency=%.2fms",
                    GROUP_ID, order["order_id"][:8], msg.partition(),
                    msg.offset(), latency_ms,
                )

            # Manual offset commit — ONLY after successful processing
            if success:
                messages_since_commit += 1
                if messages_since_commit >= commit_interval:
                    try:
                        consumer.commit(asynchronous=False)
                        logger.debug(
                            "[%s] Committed offsets after %d messages",
                            GROUP_ID, messages_since_commit,
                        )
                        messages_since_commit = 0
                    except KafkaException as e:
                        logger.error("Offset commit failed: %s", e)

            # Periodic stats logging
            now = time.time()
            if now - last_stats_log >= stats_log_interval:
                stats = tracker.get_stats()
                if stats:
                    logger.info(
                        "[%s] Stats: total=%d throughput=%.1f msg/s "
                        "p50=%.2fms p95=%.2fms p99=%.2fms",
                        GROUP_ID, stats["total_messages"],
                        stats["throughput_mps"],
                        stats["p50_ms"], stats["p95_ms"], stats["p99_ms"],
                    )
                last_stats_log = now

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    finally:
        # Final commit before shutdown
        if messages_since_commit > 0:
            try:
                consumer.commit(asynchronous=False)
                logger.info("[%s] Final offset commit (%d messages).", GROUP_ID, messages_since_commit)
            except KafkaException as e:
                logger.error("Final commit failed: %s", e)

        # Log final statistics
        stats = tracker.get_stats()
        if stats:
            logger.info(
                "=== Final Stats [%s] ===\n"
                "  Total Messages: %d\n"
                "  Throughput:     %.1f msg/s\n"
                "  Latency p50:    %.2f ms\n"
                "  Latency p95:    %.2f ms\n"
                "  Latency p99:    %.2f ms\n"
                "  Min Latency:    %.2f ms\n"
                "  Max Latency:    %.2f ms",
                GROUP_ID, stats["total_messages"], stats["throughput_mps"],
                stats["p50_ms"], stats["p95_ms"], stats["p99_ms"],
                stats["min_ns"] / 1_000_000, stats["max_ns"] / 1_000_000,
            )

            # Write summary JSON
            summary_path = Path("/data") / f"stats_kafka_{GROUP_ID}.json"
            with open(summary_path, "w") as f:
                json.dump(stats, f, indent=2)
            logger.info("Stats written to %s", summary_path)

        consumer.close()
        logger.info("Consumer [%s] shut down.", GROUP_ID)


if __name__ == "__main__":
    run_consumer()
