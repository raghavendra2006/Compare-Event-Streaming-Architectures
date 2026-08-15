"""
Order Producer — Dual-Pipeline Event Publisher
================================================
Publishes JSON order events simultaneously to:
  - Apache Kafka (topic: orders, key: order_id)
  - RabbitMQ (exchange: order-exchange, routing_key: order.created)

Each message includes a nanosecond-precision timestamp for end-to-end
latency benchmarking.
"""

import json
import logging
import os
import random
import signal
import string
import sys
import time
import uuid
from pathlib import Path

from confluent_kafka import Producer as KafkaProducer
import pika

# =============================================================================
# Configuration
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
ORDER_TOPIC = os.getenv("ORDER_TOPIC_NAME", "orders")
ORDER_EXCHANGE = os.getenv("ORDER_EXCHANGE_NAME", "order-exchange")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
PRODUCER_MODE = os.getenv("PRODUCER_MODE", "continuous")
MESSAGES_PER_BATCH = int(os.getenv("MESSAGES_PER_BATCH", "100"))
BATCH_INTERVAL = float(os.getenv("BATCH_INTERVAL_SECONDS", "2"))
BENCHMARK_MESSAGE_COUNT = int(os.getenv("BENCHMARK_MESSAGE_COUNT", "5000"))

# Logging setup
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("producer")

# Graceful shutdown
shutdown_flag = False


def handle_signal(signum, frame):
    """Handle SIGINT/SIGTERM for graceful shutdown."""
    global shutdown_flag
    logger.info("Shutdown signal received (signal=%d). Finishing current batch...", signum)
    shutdown_flag = True


signal.signal(signal.SIGINT, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)


# =============================================================================
# Order Generator
# =============================================================================

PRODUCT_IDS = [f"PROD-{i:04d}" for i in range(1, 51)]
USER_IDS = [f"USR-{i:05d}" for i in range(1, 201)]


def generate_order(inject_failure: bool = False) -> dict:
    """
    Generate a realistic order event.

    Args:
        inject_failure: If True, set amount to a negative value to trigger
                        DLX routing in RabbitMQ consumers.
    """
    amount = round(random.uniform(9.99, 999.99), 2)
    if inject_failure:
        amount = round(random.uniform(-100.0, -1.0), 2)

    return {
        "order_id": str(uuid.uuid4()),
        "user_id": random.choice(USER_IDS),
        "product_id": random.choice(PRODUCT_IDS),
        "amount": amount,
        "timestamp": int(time.time() * 1000),          # milliseconds
        "produced_at": time.time_ns(),                   # nanoseconds (for latency)
    }


# =============================================================================
# Kafka Producer
# =============================================================================

def create_kafka_producer() -> KafkaProducer:
    """Create a Kafka producer with optimized settings."""
    conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id": "order-producer",
        "acks": "all",
        "retries": 5,
        "retry.backoff.ms": 500,
        "batch.size": 16384,
        "linger.ms": 10,
        "compression.type": "lz4",
        "enable.idempotence": True,
    }
    logger.info("Connecting to Kafka at %s...", KAFKA_BOOTSTRAP_SERVERS)
    return KafkaProducer(conf)


def kafka_delivery_callback(err, msg):
    """Callback invoked on successful or failed delivery."""
    if err is not None:
        logger.error("Kafka delivery failed: %s", err)
    else:
        logger.debug(
            "Kafka delivered: topic=%s partition=%d offset=%d key=%s",
            msg.topic(), msg.partition(), msg.offset(),
            msg.key().decode("utf-8") if msg.key() else "None",
        )


def publish_to_kafka(producer: KafkaProducer, order: dict):
    """Publish a single order to Kafka."""
    key = order["order_id"]
    value = json.dumps(order)
    producer.produce(
        topic=ORDER_TOPIC,
        key=key.encode("utf-8"),
        value=value.encode("utf-8"),
        callback=kafka_delivery_callback,
    )


# =============================================================================
# RabbitMQ Producer
# =============================================================================

def create_rabbitmq_connection():
    """Create a RabbitMQ connection and channel with exchange/queue setup."""
    logger.info("Connecting to RabbitMQ at %s...", RABBITMQ_URL)
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 600
    params.blocked_connection_timeout = 300
    connection = pika.BlockingConnection(params)
    channel = connection.channel()

    # --- Declare Dead Letter Exchange and Queue ---
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
    logger.info("DLX infrastructure ready: dlx-exchange -> failed-orders-q")

    # --- Declare Main Exchange ---
    channel.exchange_declare(
        exchange=ORDER_EXCHANGE,
        exchange_type="direct",
        durable=True,
    )

    # --- Declare Queues with DLX argument ---
    queue_args = {
        "x-dead-letter-exchange": "dlx-exchange",
    }
    for queue_name in ["inventory-q", "notification-q", "analytics-q"]:
        channel.queue_declare(
            queue=queue_name,
            durable=True,
            arguments=queue_args,
        )
        channel.queue_bind(
            queue=queue_name,
            exchange=ORDER_EXCHANGE,
            routing_key="order.created",
        )
        logger.info("Queue %s bound to %s with routing key 'order.created'", queue_name, ORDER_EXCHANGE)

    return connection, channel


def publish_to_rabbitmq(channel, order: dict):
    """Publish a single order to RabbitMQ."""
    body = json.dumps(order)
    channel.basic_publish(
        exchange=ORDER_EXCHANGE,
        routing_key="order.created",
        body=body.encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=2,  # persistent
            content_type="application/json",
        ),
    )


# =============================================================================
# Health Check
# =============================================================================

def mark_healthy():
    """Create health check marker file."""
    Path("/tmp/producer_healthy").touch()


# =============================================================================
# Main Loop
# =============================================================================

def run_producer():
    """Main producer loop: dual-publish to Kafka and RabbitMQ."""
    # Connect to both brokers with retry logic
    kafka_producer = None
    rmq_connection = None
    rmq_channel = None

    for attempt in range(1, 31):
        try:
            if kafka_producer is None:
                kafka_producer = create_kafka_producer()
                logger.info("Kafka producer connected.")
        except Exception as e:
            logger.warning("Kafka connection attempt %d failed: %s", attempt, e)

        try:
            if rmq_connection is None or rmq_connection.is_closed:
                rmq_connection, rmq_channel = create_rabbitmq_connection()
                logger.info("RabbitMQ producer connected.")
        except Exception as e:
            logger.warning("RabbitMQ connection attempt %d failed: %s", attempt, e)

        if kafka_producer and rmq_connection and not rmq_connection.is_closed:
            break

        logger.info("Waiting 3s before retry (attempt %d/30)...", attempt)
        time.sleep(3)
    else:
        logger.error("Failed to connect to brokers after 30 attempts. Exiting.")
        sys.exit(1)

    mark_healthy()
    logger.info("=== Order Producer started (mode=%s) ===", PRODUCER_MODE)

    total_sent = 0
    failure_rate = 0.05  # 5% of messages will have negative amounts for DLX testing

    try:
        while not shutdown_flag:
            batch_start = time.time()

            for i in range(MESSAGES_PER_BATCH):
                if shutdown_flag:
                    break

                # Inject ~5% failures for DLX demonstration
                inject_failure = random.random() < failure_rate
                order = generate_order(inject_failure=inject_failure)

                # Publish to Kafka
                try:
                    publish_to_kafka(kafka_producer, order)
                except Exception as e:
                    logger.error("Kafka publish error: %s", e)

                # Publish to RabbitMQ
                try:
                    publish_to_rabbitmq(rmq_channel, order)
                except pika.exceptions.AMQPConnectionError:
                    logger.warning("RabbitMQ connection lost. Reconnecting...")
                    try:
                        rmq_connection, rmq_channel = create_rabbitmq_connection()
                        publish_to_rabbitmq(rmq_channel, order)
                    except Exception as e:
                        logger.error("RabbitMQ reconnect failed: %s", e)
                except Exception as e:
                    logger.error("RabbitMQ publish error: %s", e)

                total_sent += 1

            # Flush Kafka batch
            kafka_producer.flush(timeout=10)

            batch_elapsed = time.time() - batch_start
            logger.info(
                "Batch complete: %d messages sent (total: %d) in %.2fs | %.0f msg/s",
                MESSAGES_PER_BATCH, total_sent, batch_elapsed,
                MESSAGES_PER_BATCH / max(batch_elapsed, 0.001),
            )

            # Sleep between batches
            if not shutdown_flag and PRODUCER_MODE == "continuous":
                time.sleep(BATCH_INTERVAL)
            elif PRODUCER_MODE == "benchmark" and total_sent >= BENCHMARK_MESSAGE_COUNT:
                logger.info("Benchmark target reached (%d messages). Stopping.", total_sent)
                break

    except KeyboardInterrupt:
        logger.info("KeyboardInterrupt received.")
    finally:
        logger.info("Shutting down producer. Total messages sent: %d", total_sent)
        if kafka_producer:
            kafka_producer.flush(timeout=30)
        if rmq_connection and not rmq_connection.is_closed:
            rmq_connection.close()
        logger.info("Producer shutdown complete.")


if __name__ == "__main__":
    run_producer()
