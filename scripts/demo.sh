#!/usr/bin/env bash
# Walks the full event loop and prints what each step proves.
#
#   customer created  ─► business.customer.events ─► order-service view
#   order placed      ─► business.order.events    ─► spend + tier change
#   tier change       ─► business.customer.events ─► discount applied
#   customer blocked  ─► business.customer.events ─► next order refused
set -euo pipefail

CUSTOMER_URL="${CUSTOMER_URL:-http://localhost:8091}"
ORDER_URL="${ORDER_URL:-http://localhost:8092}"

command -v jq >/dev/null || { echo "jq is required" >&2; exit 1; }

step() { printf '\n\033[1m== %s\033[0m\n' "$1"; }

# Polls until the predicate succeeds, so the demo tracks real propagation
# rather than a fixed sleep that is either flaky or needlessly slow.
await() {
  local description="$1" timeout="$2"; shift 2
  printf '   waiting: %s ' "${description}"
  for _ in $(seq 1 "${timeout}"); do
    if "$@" >/dev/null 2>&1; then
      printf 'ok\n'
      return 0
    fi
    printf '.'
    sleep 1
  done
  printf '\n   timed out after %ss\n' "${timeout}" >&2
  return 1
}

view_has() {
  curl -fsS "${ORDER_URL}/customer-view/$1" | jq -e --arg v "$3" ".$2 == \$v"
}

step "Creating a customer"
CUSTOMER=$(curl -fsS -X POST "${CUSTOMER_URL}/customers" \
  -H 'content-type: application/json' \
  -d '{"email":"ada+'"$RANDOM"'@example.com","name":"Ada Lovelace"}')
CUSTOMER_ID=$(jq -r .id <<<"${CUSTOMER}")
echo "${CUSTOMER}" | jq -c '{id, email, tier, status}'

step "Waiting for CustomerCreated to reach order-service"
# Nothing called the order service. The row appears only because the outbox
# connector published the event and order-service projected it.
await "customer_view populated" 60 view_has "${CUSTOMER_ID}" status ACTIVE
curl -fsS "${ORDER_URL}/customer-view/${CUSTOMER_ID}" | jq -c .

step "Placing an order worth 600.00 (crosses the GOLD threshold)"
ORDER=$(curl -fsS -X POST "${ORDER_URL}/orders" \
  -H 'content-type: application/json' \
  -d '{"customer_id":"'"${CUSTOMER_ID}"'","items":[{"sku":"laptop-stand","quantity":1,"unit_price_cents":60000}]}')
jq -c '{id, status, subtotal_cents, discount_cents, total_cents, customer_tier}' <<<"${ORDER}"
echo "   no discount yet: the customer was still STANDARD when priced"

step "Waiting for OrderCreated to raise lifetime spend and change the tier"
await "customer promoted to GOLD" 60 \
  bash -c "curl -fsS '${CUSTOMER_URL}/customers/${CUSTOMER_ID}' | jq -e '.tier == \"GOLD\"'"
curl -fsS "${CUSTOMER_URL}/customers/${CUSTOMER_ID}" | jq -c '{tier, lifetime_spend_cents}'

step "Waiting for CustomerTierChanged to travel back to order-service"
await "discount_bps updated to 500" 60 \
  bash -c "curl -fsS '${ORDER_URL}/customer-view/${CUSTOMER_ID}' | jq -e '.discount_bps == 500'"

step "Placing a second order — the discount now applies"
curl -fsS -X POST "${ORDER_URL}/orders" \
  -H 'content-type: application/json' \
  -d '{"customer_id":"'"${CUSTOMER_ID}"'","items":[{"sku":"laptop-stand","quantity":1,"unit_price_cents":60000}]}' \
  | jq -c '{subtotal_cents, discount_cents, total_cents, customer_tier}'

step "Blocking the customer"
curl -fsS -X POST "${CUSTOMER_URL}/customers/${CUSTOMER_ID}/block" \
  -H 'content-type: application/json' -d '{"reason":"payment dispute"}' | jq -c '{id, status}'

step "Waiting for CustomerBlocked to reach order-service"
await "customer_view marked BLOCKED" 60 view_has "${CUSTOMER_ID}" status BLOCKED

step "A third order is now refused (403)"
code=$(curl -sS -o /tmp/eda-demo-blocked.json -w '%{http_code}' -X POST "${ORDER_URL}/orders" \
  -H 'content-type: application/json' \
  -d '{"customer_id":"'"${CUSTOMER_ID}"'","items":[{"sku":"laptop-stand","quantity":1,"unit_price_cents":60000}]}')
echo "   HTTP ${code}: $(jq -r .error /tmp/eda-demo-blocked.json)"
[ "${code}" = "403" ] || { echo "expected 403" >&2; exit 1; }

printf '\n\033[1mDone.\033[0m Customer %s\n' "${CUSTOMER_ID}"
echo "Inspect the streams at http://localhost:8080 (topics business.* and tech.*)"
