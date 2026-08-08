#!/usr/bin/env bash
# One command that answers "why is nothing flowing?".
#
# Walks the pipeline in order — connector tasks, topics, message counts,
# registered schemas, consumer logs — so the first empty stage is the one to
# investigate.
set -uo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
REGISTRY_URL="${REGISTRY_URL:-http://localhost:8081}"
COMPOSE="${COMPOSE:-docker compose}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

section() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

kafka_topics() {
  $COMPOSE exec -T kafka kafka-topics --bootstrap-server kafka:29092 --list 2>/dev/null | tr -d '\r'
}

section "Connector and task states"
# A connector reports RUNNING even when every one of its tasks has died, so the
# task states are the ones that actually matter here.
for name in $(curl -fsS "${CONNECT_URL}/connectors" 2>/dev/null | jq -r '.[]' | sort); do
  curl -fsS "${CONNECT_URL}/connectors/${name}/status" | jq -r '
    "\(.name)
   connector: \(.connector.state)
   tasks:     \(if (.tasks | length) == 0 then "NONE RUNNING" else ([.tasks[] | .state] | join(", ")) end)",
    (.tasks[]? | select(.trace != null)
      | "   trace:\n" + ((.trace | split("\n")[0:12] | map("     " + .) | join("\n"))))'
done

section "Topics"
kafka_topics | sort | sed 's/^/  /'

section "Messages per topic"
for topic in $(kafka_topics | grep -E '^(business|tech)\.' | sort); do
  offsets=$($COMPOSE exec -T kafka sh -c \
    "kafka-get-offsets --bootstrap-server kafka:29092 --topic '${topic}' 2>/dev/null \
     || kafka-run-class kafka.tools.GetOffsetShell --bootstrap-server kafka:29092 --topic '${topic}' 2>/dev/null" \
    | tr -d '\r')
  count=$(printf '%s\n' "${offsets}" | awk -F: '{sum += $3} END {print sum + 0}')
  printf '  %-46s %s\n' "${topic}" "${count}"
done

section "Schema Registry subjects"
curl -fsS "${REGISTRY_URL}/subjects" 2>/dev/null | jq -r '.[]' | sort | sed 's/^/  /'

section "Outbox rows written (source of truth)"
for pair in "customer-db:customer:customerdb" "order-db:orders:ordersdb"; do
  IFS=: read -r service user db <<<"${pair}"
  printf '  %s:\n' "${service}"
  $COMPOSE exec -T "${service}" psql -U "${user}" -d "${db}" -At -c \
    "SELECT '    ' || channel || ' ' || event_type || ' x' || count(*)
       FROM outbox GROUP BY channel, event_type ORDER BY 1" 2>/dev/null \
    || echo "    (query failed)"
done

section "order-service log"
$COMPOSE logs --tail 30 --no-log-prefix order-service 2>/dev/null

section "customer-service log"
$COMPOSE logs --tail 30 --no-log-prefix customer-service 2>/dev/null
