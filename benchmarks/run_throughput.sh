#!/bin/bash
# =============================================================================
# Throughput Benchmark — Kafka vs RabbitMQ
# =============================================================================
# Runs automated throughput tests for 1KB, 5KB, and 10KB message sizes
# using native performance testing tools.
#
# Results are saved to benchmarks/results/
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
RABBITMQ_HOST="${RABBITMQ_HOST:-localhost}"
NUM_RECORDS=100000
TOPIC="benchmark-throughput"

echo "=============================================="
echo " Throughput Benchmark Suite"
echo "=============================================="
echo " Kafka:    ${KAFKA_BOOTSTRAP}"
echo " RabbitMQ: ${RABBITMQ_HOST}"
echo " Records:  ${NUM_RECORDS}"
echo "=============================================="

# =============================================================================
# Kafka Throughput Tests
# =============================================================================

echo ""
echo "==============================="
echo " KAFKA THROUGHPUT TESTS"
echo "==============================="

# Create benchmark topic
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "${TOPIC}" \
    --partitions 3 \
    --replication-factor 1 2>/dev/null || true

for MSG_SIZE in 1024 5120 10240; do
    SIZE_LABEL="$((MSG_SIZE / 1024))KB"
    echo ""
    echo "--- Kafka: ${SIZE_LABEL} messages (${NUM_RECORDS} records) ---"

    docker exec kafka kafka-producer-perf-test \
        --topic "${TOPIC}" \
        --num-records "${NUM_RECORDS}" \
        --record-size "${MSG_SIZE}" \
        --throughput -1 \
        --producer-props \
            bootstrap.servers=kafka:9092 \
            batch.size=16384 \
            linger.ms=5 \
            compression.type=lz4 \
        2>&1 | tee "${RESULTS_DIR}/kafka_throughput_${SIZE_LABEL}.txt"

    echo "  → Saved to kafka_throughput_${SIZE_LABEL}.txt"
done

# Test with different batch sizes for 1KB
echo ""
echo "--- Kafka: Batch Size Comparison (1KB messages) ---"
for BATCH_SIZE in 8192 16384 65536 131072; do
    BATCH_LABEL="batch_${BATCH_SIZE}"
    echo "  Testing batch.size=${BATCH_SIZE}..."

    docker exec kafka kafka-producer-perf-test \
        --topic "${TOPIC}" \
        --num-records "${NUM_RECORDS}" \
        --record-size 1024 \
        --throughput -1 \
        --producer-props \
            bootstrap.servers=kafka:9092 \
            batch.size="${BATCH_SIZE}" \
            linger.ms=10 \
        2>&1 | tee "${RESULTS_DIR}/kafka_throughput_${BATCH_LABEL}.txt"
done

# =============================================================================
# RabbitMQ Throughput Tests
# =============================================================================

echo ""
echo "==============================="
echo " RABBITMQ THROUGHPUT TESTS"
echo "==============================="

for MSG_SIZE in 1024 5120 10240; do
    SIZE_LABEL="$((MSG_SIZE / 1024))KB"
    echo ""
    echo "--- RabbitMQ: ${SIZE_LABEL} messages (${NUM_RECORDS} records) ---"

    docker run --rm --network streaming-network \
        pivotalrabbitmq/perf-test:latest \
        --uri "amqp://guest:guest@rabbitmq:5672" \
        --producers 1 \
        --consumers 1 \
        --size "${MSG_SIZE}" \
        --pmessages "${NUM_RECORDS}" \
        --cmessages "${NUM_RECORDS}" \
        --auto-delete false \
        --time 60 \
        --confirm 100 \
        --qos "${MSG_SIZE}" \
        2>&1 | tee "${RESULTS_DIR}/rabbitmq_throughput_${SIZE_LABEL}.txt"

    echo "  → Saved to rabbitmq_throughput_${SIZE_LABEL}.txt"
done

# Test with different prefetch counts for 1KB
echo ""
echo "--- RabbitMQ: Prefetch Count Comparison (1KB messages) ---"
for PREFETCH in 1 50 100 500; do
    PREFETCH_LABEL="prefetch_${PREFETCH}"
    echo "  Testing prefetch=${PREFETCH}..."

    docker run --rm --network streaming-network \
        pivotalrabbitmq/perf-test:latest \
        --uri "amqp://guest:guest@rabbitmq:5672" \
        --producers 1 \
        --consumers 1 \
        --size 1024 \
        --pmessages "${NUM_RECORDS}" \
        --cmessages "${NUM_RECORDS}" \
        --qos "${PREFETCH}" \
        --time 60 \
        2>&1 | tee "${RESULTS_DIR}/rabbitmq_throughput_${PREFETCH_LABEL}.txt"
done

# =============================================================================
# Cleanup
# =============================================================================

echo ""
echo "==============================="
echo " Cleanup"
echo "==============================="
docker exec kafka kafka-topics --bootstrap-server kafka:9092 \
    --delete --topic "${TOPIC}" 2>/dev/null || true
echo "✓ Benchmark topic deleted."

echo ""
echo "=============================================="
echo " Throughput benchmarks complete!"
echo " Results saved to: ${RESULTS_DIR}/"
echo "=============================================="
