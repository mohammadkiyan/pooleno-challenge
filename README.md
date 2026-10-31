# pooleno-order-processing

Accepts limit orders over HTTP, commits each one to PostgreSQL before answering `202 Accepted`, then publishes it through an Outbox to Kafka and matches it in price-time order. A retry cannot create a second order, a crash cannot lose a confirmed one, and a redelivered event cannot repeat a trade.

**Status: Phase 2 of 4, awaiting review.** Phase 2 adds the ordered publisher, the pure matching engine, and the consumer that commits trades and restores the book. External notifications arrive in Phase 3, and health endpoints and lag visibility in Phase 4.

**Contents:** [Features](#features) · [Getting started](#getting-started) · [API](#api) · [Configuration](#configuration) · [Tests](#tests) · [Layout](#layout) · [Known limitations](#known-limitations)

## Features

- **Atomic acceptance:** the order, idempotency receipt, symbol sequence and Outbox event commit in one transaction. The `202` is sent only after that commit.
- **Safe retries:** the same key and the same meaning replay the exact original response, even after the order has moved on. A changed request gets `422`.
- **Exact amounts:** decimal strings become integer units without passing through a float. Excess precision is rejected, never rounded.
- **Ordered publication:** each symbol's events reach Kafka in acceptance order. A failed event holds back its own symbol and no other, and acceptance keeps working while Kafka is down.
- **Price-time matching:** makers trade best price first, oldest first within a price, at the maker's price. The engine is pure code with no dependency on Fastify, PostgreSQL or Kafka.
- **Durable matching:** trades, order changes, progress and the Inbox commit in one transaction. The book changes only after that commit, and the offset only after the book.
- **Restore from the database:** any failure discards the book and rebuilds it from committed rows, so memory and the database cannot drift apart for long.

## Getting started

You need Docker with Compose, Node.js 26 or newer, and free local ports `8080`, `55432` and `59092`.

```bash
npm ci
docker compose up --build -d --wait
npm run demo
```

The demo first shows the acceptance contract: it accepts an order, retries it with different decimal spelling, reuses the key for a different order, sends too much precision, and reads the order back.

It then runs the agreed fixture on `ETH-USD`. S1 sells 2 at 100, S2 sells 3 at 100, S3 sells 1 at 99, and B1 buys 4 at 100. B1 trades 1 at 99 with S3, 2 at 100 with S1 and 1 at 100 with S2, which leaves S2 with 2 and first at its price. A later sell at 100 queues behind S2.

These results assume an empty `ETH-USD` book, because orders already resting there take part in the match. If you have placed other orders that could trade, run `docker compose down -v` first to start clean.

To prove that the results survive a crash, kill every process and run the demo again:

```bash
docker compose kill -s SIGKILL api publisher matching postgres kafka
docker compose up -d --wait
npm run demo
```

The demo uses fixed keys, so the second run prints the same order IDs and the same three trades, with no new executions.

To see acceptance outlast a Kafka outage:

```bash
docker compose stop kafka
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: outage-demo-order-0001' \
  -d '{"symbol":"ETH-USD","side":"SELL","type":"LIMIT","price":"150.00","quantity":"1"}'
docker compose start kafka
```

The `POST` still answers `202`. Once Kafka is back, the publisher sends the event and `GET` reports the order as `OPEN`.

### Run on the host

Each process runs in its own terminal, after stopping its container and preparing the dependencies:

```bash
docker compose stop api publisher matching
docker compose up -d --wait postgres kafka
cp .env.example .env
npm run migrate
npm run kafka:init

npm run dev
npm run dev:publisher
npm run dev:matching
```

`npm run build` followed by `npm start` runs the compiled API instead.

To stop everything, run `docker compose down`. Add `-v` to delete the database and Kafka's data as well.

## API

### `POST /v1/orders`

```bash
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: order-2026-10-31-0001' \
  -d '{"symbol":"BTC-USD","side":"BUY","type":"LIMIT","price":"100.00","quantity":"2.500"}'
```

```http
HTTP/1.1 202 Accepted
location: /v1/orders/fbe6a61c-bbdd-4346-886d-af24e9de3847
content-type: application/json; charset=utf-8

{"orderId":"fbe6a61c-bbdd-4346-886d-af24e9de3847","status":"QUEUED","statusUrl":"/v1/orders/fbe6a61c-bbdd-4346-886d-af24e9de3847"}
```

`Idempotency-Key` is required: 16 to 128 characters of `A-Za-z0-9_-`. Generate one key per order and reuse it for every retry of that order. `price` and `quantity` must be JSON strings.

| Symbol | Price tick | Maximum price | Quantity step | Maximum quantity |
|---|---|---|---|---|
| `BTC-USD` | 0.01 | 10,000,000.00 | 0.00000001 | 10,000.00000000 |
| `ETH-USD` | 0.05 | 1,000,000.00 | 0.0010 | 100,000.0000 |

### `GET /v1/orders/{orderId}`

Reports the current state, with the order's trades in execution order. Replace the ID with the `orderId` from your `POST` response:

```bash
curl http://127.0.0.1:8080/v1/orders/fbe6a61c-bbdd-4346-886d-af24e9de3847
```

```json
{
  "orderId": "fbe6a61c-bbdd-4346-886d-af24e9de3847",
  "symbol": "ETH-USD",
  "side": "BUY",
  "type": "LIMIT",
  "status": "PARTIALLY_FILLED",
  "price": "100.00",
  "originalQuantity": "4.0000",
  "executedQuantity": "1.0000",
  "remainingQuantity": "3.0000",
  "acceptanceSequence": "2",
  "acceptedAt": "2026-10-31T09:47:27.367Z",
  "updatedAt": "2026-10-31T09:47:27.412Z",
  "trades": [
    {
      "tradeId": "0b3f7c3e-5a43-4a53-9d3b-6f1d1f6a2c10",
      "role": "TAKER",
      "price": "99.00",
      "quantity": "1.0000",
      "executedAt": "2026-10-31T09:47:27.412Z"
    }
  ]
}
```

| `status` | Meaning |
|---|---|
| `QUEUED` | Accepted and durable, not yet matched |
| `OPEN` | In the book with nothing executed |
| `PARTIALLY_FILLED` | In the book with part of it executed |
| `FILLED` | Fully executed |

A trade's `role` is `MAKER` if the order was resting and `TAKER` if it arrived and crossed. The counterparty's order is not shown.

### Errors

Every error has the body `{"code": "...", "message": "..."}` and never carries acceptance data.

| Status | `code` | Meaning | What the client should do |
|---|---|---|---|
| 400 | `INVALID_REQUEST` | Missing, malformed or repeated key; invalid body; malformed `orderId` | Fix the request |
| 400 | `UNSUPPORTED_SYMBOL` | No market for this symbol | Fix the request |
| 400 | `INVALID_PRICE`, `INVALID_QUANTITY` | Not a plain decimal, zero, too precise, off-increment or too large | Fix the request |
| 404 | `ORDER_NOT_FOUND`, `NOT_FOUND` | Unknown order or route | Fix the request |
| 409 | `IDEMPOTENCY_REQUEST_IN_FLIGHT` | The first request with this key is still running | Retry with the same key after `Retry-After` |
| 413, 415 | `INVALID_REQUEST` | Body over 16 KiB, or an unsupported content type | Fix the request |
| 422 | `IDEMPOTENCY_KEY_REUSED` | This key was used for a different order | Use a new key for a new order |
| 503 | `SERVICE_UNAVAILABLE` | The outcome could not be confirmed | Retry with the same key after `Retry-After` |

## Configuration

Compose sets its own values. Running on the host reads `.env`, copied from [.env.example](.env.example).

| Variable | Used by | Description | Default |
|---|---|---|---|
| `DATABASE_URL` | API, publisher, matching, `migrate` | PostgreSQL connection string | Required |
| `LOG_LEVEL` | API, publisher, matching, `kafka:init` | pino log level | `info` |
| `HOST` | API | Listen address | `127.0.0.1` |
| `PORT` | API | Listen port; `0` picks a free one | `8080` |
| `ACCEPTANCE_LOCK_TIMEOUT_MS` | API | Longest wait for a lock during acceptance, at least 1 | `1000` |
| `KAFKA_BROKERS` | Publisher, matching, `kafka:init` | Comma-separated `host:port` list | `127.0.0.1:59092` |
| `KAFKA_TOPIC` | Publisher, matching, `kafka:init` | Topic for accepted orders | `orders.accepted` |
| `OUTBOX_POLL_INTERVAL_MS` | Publisher | Pause when nothing is due | `200` |
| `OUTBOX_RETRY_BASE_MS` | Publisher | Delay after the first failed send, doubling on each attempt | `500` |
| `OUTBOX_RETRY_CAP_MS` | Publisher | Longest delay between attempts | `30000` |
| `MATCHING_GROUP_ID` | Matching | Kafka consumer group | `matching-v1` |
| `MATCHING_SESSION_TIMEOUT_MS` | Matching | Kafka session timeout, at least 6000 | `10000` |
| `MATCHING_RESTART_DELAY_MS` | Matching | Pause before restoring after a failure | `1000` |

Decisions are recorded in [ADR 0001](docs/adr/0001-phase-1-tooling.md) (versions, markets, demo scope, time budgets) and [ADR 0002](docs/adr/0002-phase-2-messaging-and-matching.md) (Kafka client and broker, tree, topic, ownership, the matching transaction, operator procedures).

## Tests

```bash
docker compose up -d --wait postgres kafka
npm run check
```

`npm run check` runs the typecheck, the linter and all tests. `npm run test:unit` needs neither database nor Kafka. Other tests use real PostgreSQL, recreating a separate `pooleno_test` database from the migrations on each run, and real Kafka, with a new topic for each test. Set `TEST_DATABASE_URL` or `TEST_KAFKA_BROKERS` to use other servers.

The engine is also compared with a simple sorted-list reference matcher over 15,000 seeded random orders.

Production code has no test hooks. Each failure is injected from outside:

### Acceptance

| Failure | How it is injected | Result proven |
|---|---|---|
| Crash after commit, before the response | A preload that sends `SIGKILL` to the process when it writes the response | The retry returns the original `orderId`, with one order |
| Crash in an open transaction | The test holds the symbol counter to park the request, then sends `SIGKILL` to the API | Nothing is kept, and the retry is accepted with no sequence gap |
| Crash after the response | `SIGKILL` between requests | `GET` and the replay match after a restart |
| Write failure between order and Outbox | A database trigger raises on the Outbox insert | `503`, no partial rows, and the counter does not advance |
| Competing request for a key | An open transaction holds the key | The waiter replays after commit, takes over after rollback, or gets `409` on timeout |
| Competing request for a symbol | An open transaction holds the counter row | `503` and nothing written |
| Duplicate and concurrent requests | 25 identical and 50 distinct requests at once | One order for the identical requests; sequences 1 to 50 for the distinct ones |
| Database unreachable | A pool pointed at a closed port | `503` for `POST` and `GET`, with no claim of acceptance |

### Publishing

| Failure | How it is injected | Result proven |
|---|---|---|
| Kafka offline | The publisher points at a closed port | Orders are accepted, attempts are durable, later events wait, and each symbol then publishes in order with its stored payload |
| Crash after the send, before marking | The test holds the event's row to park the publisher after Kafka acknowledged, then sends `SIGKILL` | The same event is sent again, byte for byte, with one trade |
| Database failure after the send | A database trigger raises on marking | No failure is recorded, the publisher stops, and the same event is resent |
| Failing symbol | A fake producer refuses one symbol | Its later events wait, other symbols continue, and the delay doubles up to the cap |

### Matching

| Failure | How it is injected | Result proven |
|---|---|---|
| Crash after the commit, before the offset | A preload that sends `SIGKILL` to the process as it commits the offset | The redelivery is a no-op, with one trade and the correct remainder in the restored book |
| Whole topic replayed | A new consumer group reads from the beginning | No new trades, and later orders queue behind the restored maker |
| Old event after newer ones | The test applies a handled event again | No-op, and the original FIFO stands |
| Sequence gap or unknown old sequence | The test applies an event out of order | Blocked with an error, and nothing changes |
| Database failure in the transaction | A database trigger raises on the trade insert | Everything rolls back, and the book is unchanged |
| Commit succeeds, reply lost | A pool whose `COMMIT` commits and then reports failure | The book is unchanged, the restore picks up the committed result, and the retry is a no-op |
| Book cannot apply a committed match | A spy that makes the book throw | The restore is correct, and the retry is a no-op |
| Book disagrees with the database | The test changes a maker's row | The transaction aborts with nothing written |
| `SIGTERM` during a transaction | The test holds the progress row, then sends `SIGTERM` and releases it | The transaction finishes, the exit is clean, no offset passes durable work, and the restart is a no-op |
| Rebalance during a transaction | A rival consumer joins the group while the transaction is parked | No offset is committed afterwards, and the redelivery is a no-op |
| Malformed message | The test produces one directly to the topic | Later events wait with no state change, until an operator moves the offset past it |
| Lost Kafka event | The test marks an event as published without sending it, with a later event behind it in the topic | Matching blocks rather than skips. The operator procedure republishes unapplied events from the Outbox, and matching resumes in order with no repeated trade |

### Ownership

| Failure | How it is injected | Result proven |
|---|---|---|
| Second process started | A real second publisher or matcher | It waits, does nothing, and takes over after `SIGKILL` of the owner |
| Ownership connection lost | `pg_terminate_backend` on the lock's connection | The role stops and exits with status 1 |

## Layout

```text
src/
  entrypoints/        Process entrypoints: api, publisher, matching, migrate, init-kafka
  app/                API assembly, configuration, errors, signals, single ownership
  infrastructure/     Pool, transactions, session lock, logger, migration runner
  modules/orders/     Routes, acceptance, idempotency, decimals, markets
  modules/messaging/  Event schema, Outbox, publisher, Kafka clients, Inbox, consumer
  modules/matching/   Pure engine and book, plus the transaction and restore around them
migrations/           SQL files, applied in name order
scripts/demo.ts       Repeatable walkthrough of the contract and the matching fixture
tests/                unit, integration (PostgreSQL), e2e (real processes and Kafka)
docs/adr/             Recorded decisions
```

## Known limitations

- **Delivery guarantee:** events are delivered at least once and their effects are idempotent. This is not a claim of exactly-once behaviour for the complete workflow.
- **Single-owner throughput:** one publisher and one matcher do the work, and the matcher handles all partitions in one serial loop. A standby takes over but does not share the load, and a blocked event stops every symbol in the matcher.
- **Retention and recovery:** Kafka keeps events for 30 days. Beyond that, or after losing Kafka's data, an operator republishes unapplied events from the Outbox by following [ADR 0002](docs/adr/0002-phase-2-messaging-and-matching.md#operator-procedures).
- **Local single broker:** one Kafka broker with a replication factor of 1, so an acknowledged event is on one machine only. A real deployment needs more.
- **No backlog limit is enforced:** during a Kafka outage, acceptance is limited only by PostgreSQL's disk, and nothing reports the backlog. Phase 4 adds that visibility.
- **Kafka versions:** the client declares support for Kafka 3.5.0 to 4.2.0. The image is the 4.2.2 patch of that line, and only that version has been run.
- **Demo identity:** there is no authentication. Every request shares the fixed scope `demo`.
- **Immutable market scales:** changing the scale of a market that has orders would misread stored amounts and stop old keys replaying.
- **Keys are kept forever:** by agreed decision, key mappings are never deleted automatically, so the table grows with every order.
- **Simple operations:** there are no health endpoints or lag metrics yet. The Compose check only proves the API's port is open, and the publisher and matcher have no check beyond running. Shutdown waits for in-flight work without its own deadline, and relies on the supervisor's `SIGKILL`, which is safe because durable records drive replay. Phase 4 completes these.
- **Local PostgreSQL:** one instance with no replication and passwordless access on the loopback interface, for local use only.
