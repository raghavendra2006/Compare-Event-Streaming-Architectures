#!/bin/bash
# =============================================================================
# End-to-End Latency Benchmark — Kafka vs RabbitMQ
# =============================================================================
# Collects latency data from the running consumers and generates
# a statistical summary (p50, p95, p99).
#
# Prerequisites: docker-compose up (all services running)
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

MESSAGE_COUNT="${BENCHMARK_MESSAGE_COUNT:-5000}"

echo "=============================================="
echo " Latency Benchmark Suite"
echo "=============================================="
echo " Target Messages: ${MESSAGE_COUNT}"
echo "=============================================="

# =============================================================================
# Step 1: Configure producer for benchmark mode
# =============================================================================

echo ""
echo "[1/4] Configuring producer for latency benchmark..."

# Stop current producer and restart in benchmark mode
docker-compose stop producer 2>/dev/null || true
docker-compose run -d \
    -e PRODUCER_MODE=benchmark \
    -e MESSAGES_PER_BATCH=500 \
    -e BATCH_INTERVAL_SECONDS=0.1 \
    -e BENCHMARK_MESSAGE_COUNT="${MESSAGE_COUNT}" \
    --name benchmark-producer \
    producer 2>/dev/null || true

echo "  → Benchmark producer started (target: ${MESSAGE_COUNT} messages)"

# =============================================================================
# Step 2: Wait for messages to be produced and consumed
# =============================================================================

echo ""
echo "[2/4] Waiting for messages to be produced and consumed..."

# Wait for producer to finish
echo "  Waiting for producer to complete..."
for i in $(seq 1 120); do
    if ! docker ps --format '{{.Names}}' | grep -q "benchmark-producer"; then
        echo "  → Producer finished after ${i}s"
        break
    fi
    sleep 1
done

# Allow consumers to catch up
echo "  Allowing 10s for consumers to catch up..."
sleep 10

# =============================================================================
# Step 3: Collect latency data from containers
# =============================================================================

echo ""
echo "[3/4] Collecting latency data from containers..."

# Copy latency CSVs from the shared volume
for SERVICE in kafka-consumer-inventory kafka-consumer-notification kafka-consumer-analytics; do
    GROUP=$(echo "${SERVICE}" | sed 's/kafka-consumer-//')
    docker cp "${SERVICE}:/data/latency_kafka_${GROUP}-cg.csv" \
        "${RESULTS_DIR}/latency_kafka_${GROUP}-cg.csv" 2>/dev/null || \
        echo "  ⚠ Could not copy latency data from ${SERVICE}"
done

for SERVICE in rabbitmq-consumer-inventory rabbitmq-consumer-notification rabbitmq-consumer-analytics; do
    QUEUE=$(echo "${SERVICE}" | sed 's/rabbitmq-consumer-//')
    docker cp "${SERVICE}:/data/latency_rabbitmq_${QUEUE}-q.csv" \
        "${RESULTS_DIR}/latency_rabbitmq_${QUEUE}-q.csv" 2>/dev/null || \
        echo "  ⚠ Could not copy latency data from ${SERVICE}"
done

echo "  → Latency CSVs collected to ${RESULTS_DIR}/"

# =============================================================================
# Step 4: Analyze results
# =============================================================================

echo ""
echo "[4/4] Analyzing latency data..."

python3 "${SCRIPT_DIR}/analyze_latency.py" --results-dir "${RESULTS_DIR}"

# Restart normal producer
docker rm -f benchmark-producer 2>/dev/null || true
docker-compose start producer 2>/dev/null || true

echo ""
echo "=============================================="
echo " Latency benchmark complete!"
echo " Results: ${RESULTS_DIR}/latency_summary.json"
echo "=============================================="
