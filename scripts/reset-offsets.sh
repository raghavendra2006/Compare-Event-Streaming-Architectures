#!/bin/bash
# =============================================================================
# Reset Kafka Consumer Group Offsets
# =============================================================================
# Resets the analytics-cg consumer group offset back by 100 records.
# If current offset < 100, resets to the beginning.
#
# Usage: ./scripts/reset-offsets.sh [GROUP_ID] [SHIFT_BY]
# Defaults: GROUP_ID=analytics-cg, SHIFT_BY=-100
# =============================================================================

set -euo pipefail

GROUP_ID="${1:-analytics-cg}"
SHIFT_BY="${2:--100}"
KAFKA_BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
TOPIC="${ORDER_TOPIC_NAME:-orders}"

echo "=============================================="
echo " Kafka Offset Reset Tool"
echo "=============================================="
echo " Bootstrap:  ${KAFKA_BOOTSTRAP}"
echo " Topic:      ${TOPIC}"
echo " Group:      ${GROUP_ID}"
echo " Shift By:   ${SHIFT_BY}"
echo "=============================================="

# Step 1: Show current offsets before reset
echo ""
echo "[1/3] Current consumer group offsets:"
docker exec kafka kafka-consumer-groups \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
    --group "${GROUP_ID}" \
    --describe 2>/dev/null || echo "  (group may not have committed offsets yet)"

# Step 2: Stop consumers in the group (they must be stopped for reset)
echo ""
echo "[2/3] Resetting offsets (shift by ${SHIFT_BY})..."
echo "       NOTE: Consumers in group '${GROUP_ID}' must be stopped first."

docker exec kafka kafka-consumer-groups \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
    --group "${GROUP_ID}" \
    --topic "${TOPIC}" \
    --reset-offsets \
    --shift-by "${SHIFT_BY}" \
    --execute

# Step 3: Verify the new offsets
echo ""
echo "[3/3] Offsets after reset:"
docker exec kafka kafka-consumer-groups \
    --bootstrap-server "${KAFKA_BOOTSTRAP}" \
    --group "${GROUP_ID}" \
    --describe 2>/dev/null

echo ""
echo "✓ Offset reset complete. Restart the consumer to re-process messages."
