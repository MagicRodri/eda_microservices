# eda_microservices

An event-driven architecture you can run on a laptop, built to make one
distinction concrete: **technical events are not business events**, and the
outbox pattern is what lets you publish both without ever lying about your
database.

Two Go services, each owning its own PostgreSQL database, exchange events
through Kafka. Neither ever calls the other over HTTP.

| Repository                                                              | Role                                        |
| ----------------------------------------------------------------------- | ------------------------------------------- |
| [customer-service](https://github.com/MagicRodri/customer-service)      | Customer accounts, tiers, blocking          |
| [order-service](https://github.com/MagicRodri/order-service)            | Orders, pricing, authorisation              |
| this repo                                                                | Infrastructure, connectors, orchestration   |

## Quick start

```bash
git clone https://github.com/MagicRodri/eda_microservices
cd eda_microservices

make bootstrap    # fetch the two service repos into services/
make up           # build and start Kafka, Schema Registry, Connect, 2x Postgres, both services
make connectors   # register the four Debezium connectors
make demo         # walk the whole event loop and assert each hop
```

Then open <http://localhost:8080> to browse topics, schemas and connector state.

| Endpoint          | URL                     |
| ----------------- | ----------------------- |
| Kafka UI          | http://localhost:8080   |
| Schema Registry   | http://localhost:8081   |
| Kafka Connect     | http://localhost:8083   |
| customer-service  | http://localhost:8091   |
| order-service     | http://localhost:8092   |

## The two kinds of events

Both streams come out of the same Postgres WAL, through the same Kafka Connect
worker, serialised as Avro against the same registry. What differs is the
contract they carry.

### Technical events — `tech.*`

Raw change-data-capture. One Debezium connector per service tails its
`customers` / `orders` table and publishes the full Debezium envelope
(`before`, `after`, `op`, `source`) to `tech.customer.public.customers` and
`tech.order.public.orders`.

The shape of these events *is* the physical table. Rename a column and every
consumer breaks. They are the right tool for audit, analytics, search indexing
and replication — infrastructure concerns that legitimately want the raw row.

Each service consumes only **its own** technical topic, into a
`technical_audit_log` table. That is deliberate: it demonstrates the stream
without ever letting a business decision depend on another service's schema.

### Business events — `business.*`

Explicit, versioned domain facts, published to `business.customer.events` and
`business.order.events`:

| Event                 | Owner            | Meaning                              |
| --------------------- | ---------------- | ------------------------------------ |
| `CustomerCreated`     | customer-service | An account was opened                |
| `CustomerBlocked`     | customer-service | The customer may no longer order     |
| `CustomerUnblocked`   | customer-service | The restriction was lifted           |
| `CustomerTierChanged` | customer-service | Spend moved the customer to a tier   |
| `OrderCreated`        | order-service    | An order was accepted                |
| `OrderCancelled`      | order-service    | An order was cancelled               |

Their contracts live with their owner, in each repository's `schemas/*.avsc`.
This is the only surface the services are allowed to couple to.

## Why an outbox

Writing to Postgres and publishing to Kafka are two systems. Do both directly
and one can succeed while the other fails:

```
tx.Commit()          ✅ order is in the database
producer.Send(event) ❌ process dies
                     ⇒ an order nobody downstream will ever hear about
```

Retrying inside the transaction does not fix it — it just moves the window. A
distributed transaction across Postgres and Kafka would fix it, at a cost
nobody wants to pay.

The outbox removes the second system from the write path entirely. The state
change and the event describing it are inserted **in the same transaction**:

```go
err := a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
    if err := tx.InsertOrder(ctx, order); err != nil {
        return err
    }
    return tx.AppendOutbox(ctx, store.OutboxRecord{ /* OrderCreated */ })
})
```

Either both rows land or neither does. Publication happens afterwards, out of
band: Debezium tails the WAL and forwards committed outbox rows to Kafka. An
event that committed will reach Kafka eventually, even if the service dies the
instant after `COMMIT`.

That is the crucial property: **the same transaction that produces the technical
event also produces the business event.** The CDC row on `orders` and the outbox
row describing it share a commit LSN. They cannot disagree.

### The outbox table

```sql
CREATE TABLE outbox (
    id             UUID PRIMARY KEY,
    aggregate_type TEXT  NOT NULL,   -- routes the topic: 'customer' | 'order'
    aggregate_id   TEXT  NOT NULL,   -- becomes the Kafka message key
    event_type     TEXT  NOT NULL,   -- copied into the eventType header
    payload        JSONB NOT NULL,   -- expanded into the Avro value
    trace_id       TEXT  NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

`aggregate_id` becoming the message key matters: every event for one customer
lands on the same partition, so their order is preserved end to end.

### Routing

Debezium's `EventRouter` SMT unwraps the CDC envelope and rewrites the topic
from the row's own data:

```json
"transforms.outbox.route.by.field": "aggregate_type",
"transforms.outbox.route.topic.replacement": "business.${routedByValue}.events",
"transforms.outbox.table.expand.json.payload": "true"
```

`expand.json.payload` is what turns the JSONB column into a real Avro record
rather than a string, so the registry ends up holding a proper schema for each
business topic instead of `{"payload": "string"}`.

## At-least-once, and what it costs

The WAL is replayed from the last committed offset, so a crash means
redelivery. Consumers are therefore idempotent by construction: every payload
carries an `event_id`, and handlers insert it into `processed_events` inside
the very transaction that applies the effect.

```go
fresh, err := tx.MarkProcessed(ctx, eventID, msg.Topic)
if !fresh {
    return nil          // already applied; the transaction rolls back to a no-op
}
// ... apply the effect, in this same transaction
```

Dedup and effect commit together, so a redelivery cannot double-count a spend.

## The loop

```
   POST /customers
        │
        ▼
  ┌─────────────────┐   customers + outbox        ┌──────────────┐
  │ customer-service│──────── one tx ────────────►│ customer-db  │
  └─────────────────┘                             └──────┬───────┘
        ▲                                                │ WAL
        │                                    ┌───────────┴────────────┐
        │                                    ▼                        ▼
        │                        tech.customer.public.customers  outbox (routed)
        │                                    │                        │
        │                                    │                        ▼
        │                                    │           business.customer.events
        │                                    │                        │
        │                              (audit only)                   ▼
        │                                                   ┌──────────────────┐
        │                                                   │  order-service   │
        │                                                   │  customer_view   │
        │                                                   └────────┬─────────┘
        │                                                            │ POST /orders
        │                                                            ▼
        │                                                    orders + outbox (one tx)
        │                                                            │
        └──────────── business.order.events ◄────────────────────────┘
                       OrderCreated / OrderCancelled
```

1. A customer is created. `CustomerCreated` reaches order-service, which
   inserts a row in `customer_view`.
2. An order is placed. order-service reads `customer_view` — **not** the
   customer service — to check the customer is not blocked and to apply the
   tier discount.
3. `OrderCreated` reaches customer-service, which adds the order total to
   `lifetime_spend_cents`. Crossing 50 000 cents promotes the customer to GOLD
   and emits `CustomerTierChanged` **in the same transaction**.
4. `CustomerTierChanged` travels back and updates `customer_view.discount_bps`.
   The next order is priced 5% lower.
5. Blocking the customer emits `CustomerBlocked`; the next order returns 403.

`make demo` performs exactly this and polls for each hop, so the propagation is
visible rather than asserted.

## Avro and the Schema Registry

Kafka Connect is configured with `io.confluent.connect.avro.AvroConverter` on
both key and value. Messages carry a 5-byte Confluent header (magic byte plus
schema ID) followed by the Avro body; the schema itself lives in the registry.

Consumers decode with the **writer's** schema, fetched by that ID:

```go
id, body, _ := header.DecodeID(msg)
schema, _ := d.schemaByID(ctx, id)     // cached after the first fetch
avro.Unmarshal(schema, body, &out)
```

Decoding with the writer's schema rather than a compiled-in one is what lets a
producer add a field without breaking existing consumers — the unknown field
lands in the map and is ignored. The registry is set to `BACKWARD`
compatibility, so an incompatible change is rejected at publish time instead of
at 3am.

Each repository's `schemas/*.avsc` is the human-readable contract for what it
publishes, and its unit tests assert every one of them parses.

## Connectors

Four connectors, two per database, registered by `make connectors`:

| Connector                       | Table              | Publishes to                    |
| ------------------------------- | ------------------ | ------------------------------- |
| `customer-technical-connector`  | `public.customers` | `tech.customer.public.customers`|
| `customer-outbox-connector`     | `public.outbox`    | `business.customer.events`      |
| `order-technical-connector`     | `public.orders`    | `tech.order.public.orders`      |
| `order-outbox-connector`        | `public.outbox`    | `business.order.events`         |

Two connectors on one database need separate replication slots and
publications, which is why each config names its own `slot.name` and
`publication.name`. `publication.autocreate.mode: filtered` keeps each
publication scoped to its own table.

Configs are PUT to `/connectors/<name>/config`, so `make connectors` is
idempotent — re-run it after editing a file.

## Operational notes

- **Ordering** holds per partition, and the key is `aggregate_id`, so events
  for one customer never overtake each other. Across aggregates there is no
  ordering guarantee, which is why block and tier events update disjoint
  columns of `customer_view`.
- **Staleness is the trade-off.** `POST /orders` reads a replica that may be
  seconds behind. A customer blocked a moment ago may get one more order
  through. `GET /customer-view/{id}` exists to make that lag visible.
- **Replication slots retain WAL.** If Connect is down while the databases take
  writes, `pg_wal` grows. `heartbeat.interval.ms` is set to 10s so idle slots
  still advance; in production, monitor slot lag.
- **The outbox table only grows.** Nothing here prunes it. In production, delete
  rows older than your retention window once the connector's confirmed flush
  LSN has passed them.
- **`services/` is not vendored.** The two service repos are fetched by
  `make bootstrap`, which registers them as git submodules when the working
  tree is a git repository. If you prefer to pin them explicitly, commit the
  resulting gitlinks.

## Repository layout

```
.
├── docker-compose.yml     # Kafka (KRaft), Schema Registry, Connect, 2x Postgres, UI
├── go.work                # both service modules, for editor tooling
├── connectors/            # one JSON config per Debezium connector
├── scripts/
│   ├── bootstrap.sh       # fetch the service repos into services/
│   ├── register-connectors.sh
│   └── demo.sh            # end-to-end walkthrough with assertions
└── services/              # customer-service, order-service (fetched, not vendored)
```

## Requirements

Docker with Compose v2, Go 1.26+ to run the tests outside containers, and `jq`
and `curl` for the demo script.
