"""
RabbitMQ Order Consumer — Queue Consumer with DLX Support
==========================================================
Consumes from a configurable queue bound to the 'order-exchange' direct
exchange with routing key 'order.created'.

Implements Dead Letter Exchange (DLX) logic:
  - Messages with amount < 0 are NACKed with requeue=false
  - RabbitMQ routes them to dlx-exchange -> failed-orders-q

Writes latency samples to /data/latency_{queue_name}.csv for benchmarking.
"""

import csv
import json
import logging
import os
import signal
import sys
import time
from pathlib import Path

import pika

# =============================================================================
# Configuration
# =============================================================================

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
QUEUE_NAME = os.getenv("QUEUE_NAME", "inventory-q")
ORDER_EXCHANGE = os.getenv("ORDER_EXCHANGE_NAME", "order-exchange")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "100"))

# Logging
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger(f"rabbitmq-consumer-{QUEUE_NAME}")

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

    def __init__(self, queue_name: str, output_dir: str = "/data"):
        self.queue_name = queue_name
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.csv_path = self.output_dir / f"latency_rabbitmq_{queue_name}.csv"
        self.samples = []
        self.total_messages = 0
        self.total_nacked = 0
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

    def record_nack(self):
        """Record a NACKed message."""
        self.total_nacked += 1

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
            "total_nacked": self.total_nacked,
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
# RabbitMQ Infrastructure Setup
# =============================================================================

def setup_infrastructure(channel):
    """
    Declare all exchanges, queues, and bindings.
    This is idempotent — safe to call on every startup.
    """
    # --- Dead Letter Exchange and Queue ---
    channel.exchange_declare(
        exchange="dlx-exchange",
        exchange_type="fanout",
        durable=True,
    )
    channel.queue_declare(
        queue="failed-orders-q",
        durable=True,
    )
    channel.queue_bind(
        queue="failed-orders-q",
        exchange="dlx-exchange",
    )
    logger.info("DLX infrastructure declared: dlx-exchange -> failed-orders-q")

    # --- Main Exchange ---
    channel.exchange_declare(
        exchange=ORDER_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    # --- Queues with DLX argument ---
    queue_args = {
        "x-dead-letter-exchange": "dlx-exchange",
    }
    for q_name in ["inventory-q", "notification-q", "analytics-q"]:
        channel.queue_declare(
            queue=q_name,
            durable=True,
            arguments=queue_args,
        )
        channel.queue_bind(
            queue=q_name,
            exchange=ORDER_EXCHANGE,
            routing_key="order.created",
        )
        logger.info("Queue '%s' bound to '%s' with routing key 'order.created'", q_name, ORDER_EXCHANGE)


# =============================================================================
# Business Logic
# =============================================================================

def process_order(order: dict, queue_name: str) -> bool:
    """
    Simulate business logic processing.

    Returns True if processing succeeded.
    Returns False if processing failed (triggers DLX via NACK).

    DLX Trigger: Messages with amount < 0 are considered invalid.
    """
    order_id = order.get("order_id", "unknown")
    amount = order.get("amount", 0)

    # --- DLX Logic: Reject orders with negative amounts ---
    if amount < 0:
        logger.warning(
            "[%s] REJECTED order %s: negative amount $%.2f → routing to DLX",
            queue_name, order_id[:8], amount,
        )
        return False

    # Normal processing
    if queue_name == "inventory-q":
        logger.debug("[inventory] Processing order %s: checking stock for %s",
                      order_id, order.get("product_id"))
    elif queue_name == "notification-q":
        logger.debug("[notification] Sending confirmation for order %s to user %s",
                      order_id, order.get("user_id"))
    elif queue_name == "analytics-q":
        logger.debug("[analytics] Recording order %s: amount=$%.2f", order_id, amount)

    return True


# =============================================================================
# Message Callback
# =============================================================================

def make_on_message_callback(tracker: LatencyTracker, queue_name: str):
    """Create a message callback closure with access to the tracker."""
    last_stats_time = [time.time()]
    stats_interval = 30  # seconds

    def on_message(channel, method, properties, body):
        """Callback invoked for each message delivered from the queue."""
        try:
            order = json.loads(body.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as e:
            logger.error("Failed to deserialize message: %s", e)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            tracker.record_nack()
            return

        # Track latency
        produced_at = order.get("produced_at")
        if produced_at:
            latency_ms = tracker.record(order.get("order_id", "unknown"), produced_at)
            logger.debug(
                "[%s] order=%s latency=%.2fms",
                queue_name, order.get("order_id", "unknown")[:8], latency_ms,
            )

        # Process the order
        success = process_order(order, queue_name)

        if success:
            # ACK — message processed successfully, remove from queue
            channel.basic_ack(delivery_tag=method.delivery_tag)
        else:
            # NACK with requeue=False — routes to DLX (failed-orders-q)
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
            tracker.record_nack()

        # Periodic stats logging
        now = time.time()
        if now - last_stats_time[0] >= stats_interval:
            stats = tracker.get_stats()
            if stats:
                logger.info(
                    "[%s] Stats: total=%d nacked=%d throughput=%.1f msg/s "
                    "p50=%.2fms p95=%.2fms p99=%.2fms",
                    queue_name, stats["total_messages"], stats["total_nacked"],
                    stats["throughput_mps"],
                    stats["p50_ms"], stats["p95_ms"], stats["p99_ms"],
                )
            last_stats_time[0] = now

    return on_message


# =============================================================================
# Main Consumer Loop
# =============================================================================

def run_consumer():
    """Main consumer loop with connection retry and DLX support."""
    tracker = LatencyTracker(QUEUE_NAME)

    connection = None
    channel = None

    # Retry connection
    for attempt in range(1, 31):
        try:
            params = pika.URLParameters(RABBITMQ_URL)
            params.heartbeat = 600
            params.blocked_connection_timeout = 300
            connection = pika.BlockingConnection(params)
            channel = connection.channel()
            logger.info("Connected to RabbitMQ at %s", RABBITMQ_URL)
            break
        except pika.exceptions.AMQPConnectionError as e:
            logger.warning("Connection attempt %d failed: %s", attempt, e)
            time.sleep(3)
    else:
        logger.error("Failed to connect after 30 attempts. Exiting.")
        sys.exit(1)

    # Setup infrastructure (idempotent)
    setup_infrastructure(channel)

    # Set prefetch count for flow control
    channel.basic_qos(prefetch_count=PREFETCH_COUNT)

    # Create callback
    callback = make_on_message_callback(tracker, QUEUE_NAME)

    # Start consuming
    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback,
        auto_ack=False,  # Manual ACK/NACK
    )

    mark_healthy()
    logger.info(
        "=== RabbitMQ Consumer [%s] started (prefetch=%d) ===",
        QUEUE_NAME, PREFETCH_COUNT,
    )

    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    except pika.exceptions.AMQPConnectionError as e:
        logger.error("Connection lost: %s", e)
    finally:
        # Log final statistics
        stats = tracker.get_stats()
        if stats:
            logger.info(
                "=== Final Stats [%s] ===\n"
                "  Total Messages: %d\n"
                "  Total NACKed:   %d\n"
                "  Throughput:     %.1f msg/s\n"
                "  Latency p50:    %.2f ms\n"
                "  Latency p95:    %.2f ms\n"
                "  Latency p99:    %.2f ms\n"
                "  Min Latency:    %.2f ms\n"
                "  Max Latency:    %.2f ms",
                QUEUE_NAME, stats["total_messages"], stats["total_nacked"],
                stats["throughput_mps"],
                stats["p50_ms"], stats["p95_ms"], stats["p99_ms"],
                stats["min_ns"] / 1_000_000, stats["max_ns"] / 1_000_000,
            )

            # Write summary JSON
            summary_path = Path("/data") / f"stats_rabbitmq_{QUEUE_NAME}.json"
            with open(summary_path, "w") as f:
                json.dump(stats, f, indent=2)
            logger.info("Stats written to %s", summary_path)

        try:
            if channel and channel.is_open:
                channel.stop_consuming()
            if connection and not connection.is_closed:
                connection.close()
        except Exception:
            pass

        logger.info("Consumer [%s] shut down.", QUEUE_NAME)


if __name__ == "__main__":
    run_consumer()
