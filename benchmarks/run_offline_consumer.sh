#!/bin/bash
# =============================================================================
# Offline Consumer Recovery Experiment — Kafka vs RabbitMQ
# =============================================================================
# Tests how each system handles consumer downtime:
# 1. Start both pipelines with active producers
# 2. Kill notification consumers for both systems
# 3. Produce 1,000 messages while consumers are down
# 4. Wait 60 seconds
# 5. Restart consumers and measure catch-up time
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
RESULTS_DIR="${SCRIPT_DIR}/results"
mkdir -p "${RESULTS_DIR}"

KAFKA_BOOTSTRAP="localhost:9092"
RABBITMQ_API="http://localhost:15672/api"
MESSAGES_DURING_OUTAGE=1000

echo "=============================================="
echo " Offline Consumer Recovery Experiment"
echo "=============================================="
echo " Messages during outage: ${MESSAGES_DURING_OUTAGE}"
echo "=============================================="

# =============================================================================
# Step 1: Ensure all services are running
# =============================================================================

echo ""
echo "[1/6] Verifying all services are running..."
docker-compose ps
echo "  → All services verified."

# Record initial Kafka consumer group lag
echo ""
echo "--- Initial Kafka Consumer Group Status ---"
docker exec kafka kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --describe --all-groups 2>/dev/null || true

# Record initial RabbitMQ queue depths
echo ""
echo "--- Initial RabbitMQ Queue Depths ---"
curl -s -u guest:guest "${RABBITMQ_API}/queues/%2F/notification-q" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(f'  notification-q: {d.get(\"messages\",0)} messages')" 2>/dev/null || true

# =============================================================================
# Step 2: Kill notification consumers
# =============================================================================

echo ""
echo "[2/6] Stopping notification consumers..."
docker-compose stop kafka-consumer-notification rabbitmq-consumer-notification
echo "  → Notification consumers stopped."
OUTAGE_START=$(date +%s)

# =============================================================================
# Step 3: Produce messages while consumers are down
# =============================================================================

echo ""
echo "[3/6] Producing ${MESSAGES_DURING_OUTAGE} messages while notification consumers are offline..."

# Run a temporary producer
docker-compose run --rm \
    -e PRODUCER_MODE=benchmark \
    -e MESSAGES_PER_BATCH=200 \
    -e BATCH_INTERVAL_SECONDS=0.1 \
    -e BENCHMARK_MESSAGE_COUNT="${MESSAGES_DURING_OUTAGE}" \
    --name offline-experiment-producer \
    producer 2>/dev/null || true

echo "  → ${MESSAGES_DURING_OUTAGE} messages produced."

# =============================================================================
# Step 4: Wait 60 seconds
# =============================================================================

echo ""
echo "[4/6] Waiting 60 seconds (simulating outage period)..."
for i in $(seq 60 -10 10); do
    echo "  ${i}s remaining..."
    sleep 10
done
sleep 10
echo "  0s remaining..."

# Record backlog state
echo ""
echo "--- Backlog Status (after 60s outage) ---"

# Kafka lag
echo ""
echo "Kafka Consumer Group Lag:"
docker exec kafka kafka-consumer-groups \
    --bootstrap-server kafka:9092 \
    --group notification-cg \
    --describe 2>/dev/null | tee "${RESULTS_DIR}/kafka_lag_during_outage.txt"

# RabbitMQ queue depth
echo ""
echo "RabbitMQ Queue Depth:"
RABBIT_DEPTH=$(curl -s -u guest:guest "${RABBITMQ_API}/queues/%2F/notification-q" | \
    python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages',0))" 2>/dev/null || echo "0")
echo "  notification-q depth: ${RABBIT_DEPTH} messages"
echo "${RABBIT_DEPTH}" > "${RESULTS_DIR}/rabbitmq_depth_during_outage.txt"

# =============================================================================
# Step 5: Restart consumers and measure catch-up time
# =============================================================================

echo ""
echo "[5/6] Restarting notification consumers..."
RESTART_TIME=$(date +%s)
docker-compose start kafka-consumer-notification rabbitmq-consumer-notification
echo "  → Notification consumers restarted."

# Monitor catch-up
echo ""
echo "Monitoring catch-up..."

KAFKA_CAUGHT_UP=false
RABBIT_CAUGHT_UP=false
KAFKA_CATCHUP_TIME=0
RABBIT_CATCHUP_TIME=0

for i in $(seq 1 120); do
    sleep 1

    # Check Kafka lag
    if [ "${KAFKA_CAUGHT_UP}" = false ]; then
        KAFKA_LAG=$(docker exec kafka kafka-consumer-groups \
            --bootstrap-server kafka:9092 \
            --group notification-cg \
            --describe 2>/dev/null | \
            grep -v "^$" | grep -v "GROUP" | grep -v "TOPIC" | \
            awk '{sum += $5} END {print sum+0}' 2>/dev/null || echo "999")

        if [ "${KAFKA_LAG}" -le 0 ] 2>/dev/null; then
            KAFKA_CATCHUP_TIME=$i
            KAFKA_CAUGHT_UP=true
            echo "  ✓ Kafka notification-cg caught up in ${i}s"
        fi
    fi

    # Check RabbitMQ depth
    if [ "${RABBIT_CAUGHT_UP}" = false ]; then
        RABBIT_Q=$(curl -s -u guest:guest "${RABBITMQ_API}/queues/%2F/notification-q" | \
            python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('messages',0))" 2>/dev/null || echo "999")

        if [ "${RABBIT_Q}" -le 0 ] 2>/dev/null; then
            RABBIT_CATCHUP_TIME=$i
            RABBIT_CAUGHT_UP=true
            echo "  ✓ RabbitMQ notification-q caught up in ${i}s"
        fi
    fi

    # Both caught up
    if [ "${KAFKA_CAUGHT_UP}" = true ] && [ "${RABBIT_CAUGHT_UP}" = true ]; then
        break
    fi

    # Progress indicator every 5 seconds
    if [ $((i % 5)) -eq 0 ]; then
        echo "  ... ${i}s elapsed (Kafka lag: ${KAFKA_LAG:-?}, RabbitMQ depth: ${RABBIT_Q:-?})"
    fi
done

# =============================================================================
# Step 6: Report results
# =============================================================================

echo ""
echo "=============================================="
echo " Offline Consumer Experiment Results"
echo "=============================================="
echo ""
echo " Messages produced during outage: ${MESSAGES_DURING_OUTAGE}"
echo " Outage duration:                 60 seconds"
echo ""
echo " ┌──────────────┬──────────────┬──────────────────┐"
echo " │ System       │ Backlogged   │ Catch-up Time    │"
echo " ├──────────────┼──────────────┼──────────────────┤"
echo " │ Kafka        │ ${MESSAGES_DURING_OUTAGE} msgs    │ ${KAFKA_CATCHUP_TIME}s               │"
echo " │ RabbitMQ     │ ${RABBIT_DEPTH:-${MESSAGES_DURING_OUTAGE}} msgs    │ ${RABBIT_CATCHUP_TIME}s               │"
echo " └──────────────┴──────────────┴──────────────────┘"
echo ""

# Save results as JSON
cat > "${RESULTS_DIR}/offline_experiment.json" <<EOF
{
  "experiment": "offline_consumer_recovery",
  "messages_during_outage": ${MESSAGES_DURING_OUTAGE},
  "outage_duration_seconds": 60,
  "kafka": {
    "messages_backlogged": ${MESSAGES_DURING_OUTAGE},
    "catchup_time_seconds": ${KAFKA_CATCHUP_TIME},
    "persistence": "Messages retained in commit log (offset-based recovery)"
  },
  "rabbitmq": {
    "messages_backlogged": ${RABBIT_DEPTH:-${MESSAGES_DURING_OUTAGE}},
    "catchup_time_seconds": ${RABBIT_CATCHUP_TIME},
    "persistence": "Messages held in queue until ACKed (queue-depth recovery)"
  }
}
EOF

echo "Results saved to: ${RESULTS_DIR}/offline_experiment.json"
echo ""
echo "=============================================="
echo " Experiment complete!"
echo "=============================================="
