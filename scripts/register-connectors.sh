#!/usr/bin/env bash
# Registers every connector in connectors/ with Kafka Connect.
#
# The connector name is taken from the filename, and the config is PUT to
# /connectors/<name>/config, which creates the connector or updates it in place.
# Re-running the script is therefore safe.
set -euo pipefail

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

echo "Waiting for Kafka Connect at ${CONNECT_URL}"
for attempt in $(seq 1 60); do
  if curl -fsS "${CONNECT_URL}/connectors" >/dev/null 2>&1; then
    break
  fi
  if [ "${attempt}" -eq 60 ]; then
    echo "Kafka Connect did not become ready in time" >&2
    exit 1
  fi
  sleep 3
done
echo "Kafka Connect is up"

for config in "${ROOT}"/connectors/*.json; do
  name="$(basename "${config}" .json)"
  echo "Registering ${name}"
  curl -fsS -X PUT \
    -H 'Content-Type: application/json' \
    --data @"${config}" \
    "${CONNECT_URL}/connectors/${name}/config" >/dev/null
done

echo
echo "Connector status:"
for config in "${ROOT}"/connectors/*.json; do
  name="$(basename "${config}" .json)"
  # Give each connector a moment to move out of its initial state.
  sleep 2
  printf '  %-32s ' "${name}"
  curl -fsS "${CONNECT_URL}/connectors/${name}/status" \
    | sed -n 's/.*"connector":{"state":"\([A-Z]*\)".*/\1/p' \
    | head -1
done
