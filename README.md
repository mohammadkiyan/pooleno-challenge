# pooleno-order-processing

Accepts limit orders over HTTP, commits each one to PostgreSQL before answering `202 Accepted`, then publishes it through an Outbox to Kafka, matches it in price-time order and notifies an external API. A retry cannot create a second order, a crash cannot lose a confirmed one, a redelivered event cannot repeat a trade, and a lost response cannot repeat a notification's effect.

**Status: Phase 4 of 4, awaiting review.** Phase 4 adds graceful shutdown with a deadline, health endpoints for every role, backlog and lag visibility, and the remaining crash tests. For how it works and why, read the [architecture document](docs/architecture.md).

**Contents:** [Features](#features) · [Getting started](#getting-started) · [API](#api) · [Operations](#operations) · [Configuration](#configuration) · [Tests](#tests) · [Layout](#layout) · [Known limitations](#known-limitations)

## Features

- **Atomic acceptance:** the order, idempotency receipt, symbol sequence and Outbox event commit in one transaction. The `202` is sent only after that commit.
- **Safe retries:** the same key and the same meaning replay the exact original response, even after the order has moved on. A changed request gets `422`.
- **Exact amounts:** decimal strings become integer units without passing through a float. Excess precision is rejected, never rounded.
- **Ordered publication:** each symbol's events reach Kafka in acceptance order. A failed event holds back its own symbol and no other, and acceptance keeps working while Kafka is down.
- **Price-time matching:** makers trade best price first, oldest first within a price, at the maker's price. The engine is pure code with no dependency on Fastify, PostgreSQL or Kafka.
- **Durable matching:** trades, order changes, progress and the Inbox commit in one transaction. The book changes only after that commit, and the offset only after the book.
- **Restore from the database:** any failure discards the book and rebuilds it from committed rows, so memory and the database cannot drift apart for long.
- **Durable notifications:** a separate consumer group turns each accepted order into a job before its offset advances, and a worker delivers it under a stable key. Attempts and due times survive restarts.
- **Independence:** the destination being down delays only notifications. Matching continues, and no order changes because of it.
- **Inspectable failures:** a permanent rejection or an exhausted budget leaves a `FAILED` job for a manual redrive, under the same event and key.
- **Graceful shutdown:** on `SIGTERM`, a role reports not ready, finishes the work in flight and commits only what is durable, all within a deadline shorter than the grace period.
- **Health and visibility:** every role has liveness and readiness endpoints, `GET /ops/backlog` reports the Outbox, matching, notifications and Kafka lag, and logs follow one request across all four roles.

## Getting started

You need Docker with Compose, Node.js 26 or newer, and free local ports `8080`, `8081`, `8082`, `8083`, `55432`, `58080` and `59092`.

```bash
npm ci
docker compose up --build -d --wait
npm run demo
```

If `up` fails with `address already in use`, follow [If a port won't bind](#if-a-port-wont-bind).

The demo first shows the acceptance contract: it accepts an order, retries it with different decimal spelling, reuses the key for a different order, sends too much precision, and reads the order back.

It then runs the agreed fixture on `ETH-USD`. S1 sells 2 at 100, S2 sells 3 at 100, S3 sells 1 at 99, and B1 buys 4 at 100. B1 trades 1 at 99 with S3, 2 at 100 with S1 and 1 at 100 with S2, which leaves S2 with 2 and first at its price. A later sell at 100 queues behind S2. Finally it shows the one effect that B1's notification caused at the mock destination.

These results assume an empty `ETH-USD` book, because orders already resting there take part in the match. If you have placed other orders that could trade, run `docker compose down -v` first to start clean.

To prove that the results survive a crash, kill every process and run the demo again:

```bash
docker compose kill -s SIGKILL api publisher matching external mock-api postgres kafka
docker compose up -d --wait
npm run demo
```

The demo uses fixed keys, so the second run prints the same order IDs, the same three trades and the same single effect, with no new executions. A rerun therefore replays stored results and doesn't show that the matcher is running. The outage steps below do, because they send a new order.

To see acceptance outlast a Kafka outage:

```bash
docker compose stop kafka
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: outage-demo-order-0001' \
  -d '{"symbol":"ETH-USD","side":"SELL","type":"LIMIT","price":"150.00","quantity":"1"}'
curl -s http://127.0.0.1:8081/health/ready
sleep 10
curl -s http://127.0.0.1:8080/ops/backlog
docker compose ps
docker compose start kafka
```

The `POST` still answers `202`, and the publisher answers `503` with `kafka unreachable`. After the wait, the backlog shows the event pending, its symbol blocked and Kafka unavailable, and Compose marks the three Kafka roles `unhealthy` without restarting them.

About 15 s after Kafka starts, `GET /v1/orders/<orderId>` reports the order as `OPEN`. A longer outage can add up to the publisher's 30 s retry cap.

### See a lost response cause no second effect

The mock can commit its effect and then drop the connection, which is the gap that a local Inbox cannot close. This queues that fault, followed by a `503` whose `Retry-After` holds the job long enough to restart the mock in between:

```bash
curl -i http://127.0.0.1:58080/__control/faults \
  -H 'content-type: application/json' \
  -d '{"mode":"lost-response"}'
curl -i http://127.0.0.1:58080/__control/faults \
  -H 'content-type: application/json' \
  -d '{"mode":"status","status":503,"retryAfter":"15"}'
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: lost-response-demo-0001' \
  -d '{"symbol":"ETH-USD","side":"SELL","type":"LIMIT","price":"200.00","quantity":"1"}'
sleep 5
docker compose kill -s SIGKILL mock-api
docker compose up -d --wait mock-api
sleep 15
docker compose logs mock-api | grep '"outcome"' | tail -2
docker compose exec postgres psql -U pooleno -d pooleno -c "
  SELECT target_key, status, attempts FROM external_jobs ORDER BY created_at DESC LIMIT 1;"
curl -s http://127.0.0.1:58080/__control/effects | grep -o 'external-notify:[0-9a-f-]*' | sort | uniq -c
```

The job is `SUCCEEDED` after three attempts: the lost response, the `503`, and the replay. The log shows `applied` and then, after the restart, `replayed` for one key, and the last command counts every key once.

### See a failed job redriven

A permanent rejection fails the job at once and leaves it for an operator. Replace the event ID with the one the query prints:

```bash
curl -i http://127.0.0.1:58080/__control/faults \
  -H 'content-type: application/json' \
  -d '{"mode":"status","status":400}'
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: redrive-demo-order-0001' \
  -d '{"symbol":"ETH-USD","side":"SELL","type":"LIMIT","price":"210.00","quantity":"1"}'
sleep 3
docker compose exec postgres psql -U pooleno -d pooleno -c "
  SELECT event_id, status, last_error FROM external_jobs WHERE status = 'FAILED';"
docker compose run --rm --no-deps external node dist/entrypoints/redrive-external.js <eventId>
```

The job is `SUCCEEDED` moments later, under the same key. Other procedures are in [ADR 0003](docs/adr/0003-phase-3-external-notifications.md#operator-procedures).

### Run on the host

Each process runs in its own terminal, after stopping its container and preparing the dependencies:

```bash
docker compose stop api publisher matching external mock-api
docker compose up -d --wait postgres kafka
cp .env.example .env
npm run migrate
npm run kafka:init

npm run dev
npm run dev:publisher
npm run dev:matching
npm run dev:external
npm run dev:mock-api
```

`npm run build` followed by `npm start` runs the compiled API instead, and `npm run external:redrive -- <eventId>` redrives a failed job from the host.

The host's mock keeps its effects in `mock-api.sqlite`, apart from the container's volume. Orders that a Compose run has already notified are therefore missing from it, and `npm run demo` fails waiting for B1's effect. To start clean, run `docker compose down -v` before this list, which also deletes the database and Kafka's data.

To stop everything, run `docker compose down`. Add `-v` to delete the database, Kafka's data and the container mock's effects as well. The host's `mock-api.sqlite*` files stay until you delete them.

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

## Operations

### Health

Every role answers `GET /health/live` and `GET /health/ready`. Liveness means the process runs. Readiness means the role can do its work now, and a failure gives a fixed reason:

```bash
curl -i http://127.0.0.1:8082/health/ready
```

```http
HTTP/1.1 503 Service Unavailable
content-type: application/json

{"status":"not_ready","reason":"no partitions assigned"}
```

| Role | Port | Ready when |
|---|---|---|
| API | `8080` | PostgreSQL answers through the pool. Kafka isn't needed to accept orders |
| Publisher | `8081` | It owns the role, and Kafka answers |
| Matching | `8082` | It owns the role, its books are restored, and it has partitions |
| External | `8083` | It owns the role, and it has partitions. The destination being down doesn't count |

### Backlog

`GET /ops/backlog` reports counts and ages only. It answers even with Kafka down, and then reports `"kafka": {"available": false}`. This is its answer 10 s into the outage in [Getting started](#getting-started):

```bash
curl -s http://127.0.0.1:8080/ops/backlog
```

```json
{
  "outbox": {
    "pending": 1,
    "oldestPendingAgeMs": 10054,
    "retrying": 1,
    "totalAttempts": 4,
    "blockedSymbols": [
      { "symbol": "ETH-USD", "acceptanceSequence": "6", "attempts": 4, "nextAttemptInMs": 712 }
    ]
  },
  "matching": {
    "lag": 1,
    "symbols": [{ "symbol": "ETH-USD", "lag": 1, "oldestUnappliedAgeMs": 10053 }]
  },
  "external": { "pending": 0, "retrying": 0, "failed": 0, "oldestPendingAgeMs": null, "quarantined": 0 },
  "kafka": { "available": false }
}
```

With Kafka up, `kafka` is `{"available": true, "groups": [...]}`, with the lag of each group and partition. The [architecture document](docs/architecture.md#visibility) explains each field.

`/health/*` and `/ops/*` have no authentication, so an ingress must not route them.

### Logs

Each role writes JSON lines that carry `role`, `requestId`, `orderId`, `eventId`, `symbol` and `acceptanceSequence`. This follows the most recently matched order across all four roles:

```bash
REQUEST_ID=$(docker compose logs --no-log-prefix matching | grep -o '"requestId":"[^"]*"' | tail -1)
docker compose logs --no-log-prefix api publisher matching external | grep -F "$REQUEST_ID"
```

### Stopping

`docker compose stop` sends `SIGTERM`. A role drains and exits with status 0, normally within a second. Past `SHUTDOWN_TIMEOUT_MS` it exits with status 1, which is safe because durable records drive replay. A second signal ends it at once. The [architecture document](docs/architecture.md#shutdown) has the measurements.

### If a port won't bind

`docker compose up` can fail with `failed to bind host port ... address already in use` when nothing is listening on that port. `55432`, `58080` and `59092` are inside Linux's range for outgoing connections, `32768` to `60999`. A client that recently used one as its source port keeps it in `TIME_WAIT` for 60 s, and one test run left 339 such entries on loopback.

Wait until `ss` lists no entry for the port in the error, then replace the containers:

```bash
ss -tan 'sport = :58080'
docker compose down
docker compose up -d --wait
```

`down` keeps the named volumes, so your data stays.

Don't rerun `up` without `down`, even after waiting. It starts the failed container with no network. `matching` then restarts in a loop, and `mock-api` is worse: `up` exits 0 and reports it `healthy`, because its check uses loopback, while nothing can reach it.

## Configuration

Compose sets its own values. Running on the host reads `.env`, copied from [.env.example](.env.example).

| Variable | Used by | Description | Default |
|---|---|---|---|
| `DATABASE_URL` | API, publisher, matching, external, `migrate`, `external:redrive` | PostgreSQL connection string | Required |
| `LOG_LEVEL` | All | pino log level | `info` |
| `SHUTDOWN_TIMEOUT_MS` | API, publisher, matching, external | Longest wait for work in flight after a signal. Keep it below the supervisor's grace period | `8000` |
| `HEALTH_HOST` | Publisher, matching, external | Listen address of the health server | `127.0.0.1` |
| `HEALTH_PORT` | Publisher, matching, external | Health port; `0` picks a free one | `8081`, `8082`, `8083` |
| `HOST` | API | Listen address | `127.0.0.1` |
| `PORT` | API | Listen port; `0` picks a free one | `8080` |
| `ACCEPTANCE_LOCK_TIMEOUT_MS` | API | Longest wait for a lock during acceptance, at least 1 | `1000` |
| `KAFKA_BROKERS` | API (only to report lag), publisher, matching, external, `kafka:init` | Comma-separated `host:port` list | `127.0.0.1:59092` |
| `KAFKA_TOPIC` | API (only to report lag), publisher, matching, external, `kafka:init` | Topic for accepted orders | `orders.accepted` |
| `OUTBOX_POLL_INTERVAL_MS` | Publisher | Pause when nothing is due | `200` |
| `OUTBOX_RETRY_BASE_MS` | Publisher | Delay after the first failed send, doubling on each attempt | `500` |
| `OUTBOX_RETRY_CAP_MS` | Publisher | Longest delay between attempts | `30000` |
| `MATCHING_GROUP_ID` | API (only to report lag), matching | Kafka consumer group | `matching-v1` |
| `MATCHING_SESSION_TIMEOUT_MS` | Matching | Kafka session timeout, at least 6000 | `10000` |
| `MATCHING_RESTART_DELAY_MS` | Matching | Pause before restoring after a failure | `1000` |
| `EXTERNAL_GROUP_ID` | API (only to report lag), external | Kafka consumer group | `external-notification-v1` |
| `EXTERNAL_SESSION_TIMEOUT_MS` | External | Kafka session timeout, at least 6000 | `10000` |
| `EXTERNAL_RESTART_DELAY_MS` | External | Pause after a failed session or delivery pass | `1000` |
| `EXTERNAL_API_URL` | External | Where notifications go, as an absolute URL | `http://127.0.0.1:58080/v1/notifications` |
| `EXTERNAL_TIMEOUT_MS` | External | Limit on one attempt | `5000` |
| `EXTERNAL_POLL_INTERVAL_MS` | External | Pause when no job is due | `500` |
| `EXTERNAL_RETRY_BASE_MS` | External | Delay after the first failed attempt, doubling on each attempt | `1000` |
| `EXTERNAL_RETRY_CAP_MS` | External | Longest delay between attempts, and the limit on `Retry-After` | `300000` |
| `EXTERNAL_RETRY_JITTER_PERCENT` | External | Largest share of a delay removed at random, 0 to 100 | `50` |
| `EXTERNAL_MAX_ATTEMPTS` | External | Attempts before a job fails, which lasts about a day by default | `400` |
| `MOCK_API_DATABASE` | Mock API | SQLite file that holds its effects | Required |
| `MOCK_API_HOST` | Mock API | Listen address | `127.0.0.1` |
| `MOCK_API_PORT` | Mock API | Listen port | `58080` |

Decisions are recorded in [ADR 0001](docs/adr/0001-phase-1-tooling.md) (versions, markets, demo scope, time budgets), [ADR 0002](docs/adr/0002-phase-2-messaging-and-matching.md) (Kafka client and broker, tree, topic, ownership, the matching transaction, operator procedures), [ADR 0003](docs/adr/0003-phase-3-external-notifications.md) (job states, HTTP client, retry budget, failure classes, quarantine policy, the mock, the idempotency and independence reviews) and [ADR 0004](docs/adr/0004-phase-4-evidence-and-operations.md) (shutdown deadline, health, the backlog limit, the backlog report).

## Tests

```bash
docker compose up -d --wait postgres kafka
npm run check
```

`npm run check` runs the typecheck, the linter and all tests. `npm run test:unit` needs neither database nor Kafka. Other tests use real PostgreSQL, recreating a separate `pooleno_test` database from the migrations on each run, and real Kafka, with a new topic for each test. Set `TEST_DATABASE_URL` or `TEST_KAFKA_BROKERS` to use other servers.

The engine is also compared with a simple sorted-list reference matcher over 15,000 seeded random orders.

**Result on 2026-10-31:** the typecheck and linter are clean, and all 359 tests in 17 files pass in 144 s, on Node.js 26.7.0 with the pinned PostgreSQL and Kafka images.

Production code has no test hooks. Each failure is injected from outside, and you can use the same methods by hand:

- **Kill a real process:** `docker compose kill -s SIGKILL <service>`.
- **Stop or hang a dependency:** `docker compose stop kafka`, or `docker compose pause kafka` for a broker that accepts connections and never answers.
- **Make the destination misbehave:** `POST /__control/faults` on the mock, as in [the lost-response walkthrough](#see-a-lost-response-cause-no-second-effect).
- **Die at an exact point:** run a role on the host with a preload from `tests/`, which kills the process just before a chosen call. After `cp .env.example .env` and `docker compose stop matching`, the following dies as it commits the offset that you name: `CRASH_BEFORE_COMMITTING_OFFSET=<offset> node --env-file=.env --import tsx --import ./tests/e2e/crash-before-offset-commit.preload.ts src/entrypoints/matching.ts`. After the demo, name 1 more than the largest `endOffset` in `GET /ops/backlog`, then send an `ETH-USD` order. The process exits with status 137, and `docker compose start matching` replays the event without applying it twice.
- **Park a transaction:** hold a row in `psql` with `SELECT ... FOR UPDATE`, as the tests do with the progress and Inbox rows, then kill or signal the role.
- **Fail a write:** add a database trigger that raises on the table.

The tables list each failure and its result.

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
| Crash in an open transaction | The test holds the progress row to park the transaction, then sends `SIGKILL` | Nothing is kept, no offset moves, and the restart applies the event once |
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

### External notifications

| Failure | How it is injected | Result proven |
|---|---|---|
| Remote effect commits, response lost | The mock commits and resets the connection, and is then killed and restarted | The retry uses the same key, the mock replays, and there is one effect |
| Remote effect commits, then a timeout | The mock commits and never answers | The retry uses the same key, with one effect |
| Crash during the call | `SIGKILL` of the worker while the mock withholds its answer | The next owner reclaims the job, with one effect |
| Crash after the answer, before recording it | A preload that sends `SIGKILL` just before the result is written | The job is still due with no attempt used, and the retry is replayed, with one effect |
| Destination outage | The mock is not running | Matching continues, and attempts and due times survive a restart of the role |
| Matching blocked | A malformed event stops matching | Valid events behind it are still notified, because the groups are separate |
| Kafka unreachable | The role points at a closed port | Existing jobs are still delivered |
| Retries exhausted | The mock is not running, with a small budget | The job is `FAILED`, and the order is unchanged |
| Crash inside the handoff | The test holds the Inbox row to park the transaction, then sends `SIGKILL` | No job and no offset are kept, and the restart delivers once |
| Crash between the handoff and the offset | A preload that sends `SIGKILL` as the offset is committed | The redelivery creates no second job |
| `SIGTERM` or a rebalance during the handoff | The test holds the Inbox row, then signals or joins the group | The transaction finishes, and no offset passes durable work |
| Database failure in the handoff | A database trigger raises on the insert | The offset holds until the job is stored |
| Malformed message | Invalid JSON, a wrong shape and a tombstone on the topic | Each is quarantined, then the valid event behind them is delivered |
| Quarantine write fails | A database trigger raises on the insert | The offset holds until the message is recorded |
| `Retry-After` | The mock answers 503 with the header | The next attempt waits at least that long, up to the cap |
| Conflicting body | A different body under the same key | `422`, a `FAILED` job, and the earlier effect is untouched |
| Permanent status | The mock answers 400 | The job is `FAILED` at once, and the redrive command delivers it under the same key |
| `SIGTERM` during an attempt | The mock withholds its answer | The attempt finishes within its timeout, and the process exits cleanly |

### Ownership

| Failure | How it is injected | Result proven |
|---|---|---|
| Second process started | A real second publisher, matcher or external role | It waits, does nothing, and takes over after `SIGKILL` of the owner |
| Ownership connection lost | `pg_terminate_backend` on the lock's connection | The role stops and exits with status 1 |

### Lifecycle, health and visibility

| Situation | How it is produced | Result proven |
|---|---|---|
| `SIGTERM` during work | The test parks a transaction, then signals the role | Readiness turns false while liveness still answers, and the work finishes |
| Work outlasts the deadline | A transaction stays parked past `SHUTDOWN_TIMEOUT_MS` | Exit status 1 at the deadline, and the abandoned work is replayed once |
| Second signal | Two signals during a stuck shutdown | The role exits at once |
| `SIGTERM` while joining a group | The role is signalled during a join | The deadline bounds it |
| API request in flight | The test parks a request, then signals the API | That request finishes, and new connections are refused |
| Standby | A second real process | Alive and not ready, and ready once it takes over |
| Kafka unreachable | The role points at a closed port | Alive and not ready, and the API stays ready |
| Destination unreachable | The role points at a closed port | The external role stays ready |
| Health port taken | A process already holds the port | The role exits with status 1 |
| PostgreSQL unreachable | A pool pointed at a closed port | The API is alive and answers `503` on readiness, with no connection details |
| Kafka down or hung, for lag | A closed port, and a server that never answers | `GET /ops/backlog` answers `200` within its limit, with `"available": false` |
| One request across all roles | A full flow through real processes | Each role logs the same `requestId`, `orderId`, `eventId`, `symbol` and `acceptanceSequence`, and no log holds the key |

## Layout

```text
src/
  entrypoints/        Process entrypoints: api, publisher, matching, external, redrive-external,
                      migrate, init-kafka
  app/                API assembly, configuration, errors, signals, health server, single ownership
  infrastructure/     Pool, transactions, session lock, logger, migration runner
  modules/orders/     Routes, acceptance, idempotency, decimals, markets
  modules/messaging/  Event schema, Outbox, publisher, Kafka clients, Inbox, consumer
  modules/matching/   Pure engine and book, plus the transaction and restore around them
  modules/external/   Job handoff, worker, HTTP client and job storage
  modules/operations/ Health routes, backlog report and Kafka lag
mock-api/             Stand-in destination with durable idempotency, sharing nothing with src/
migrations/           SQL files, applied in name order
scripts/demo.ts       Repeatable walkthrough of the contract, the fixture and a notification
tests/                unit, integration (PostgreSQL), e2e (real processes and Kafka)
docs/architecture.md  How it works: 202, complexity, transactions, offsets, scale, evidence map
docs/adr/             Recorded decisions
```

## Known limitations

- **Delivery guarantee:** events are delivered at least once and their effects are idempotent. This is not a claim of exactly-once behaviour for the complete workflow.
- **Single-owner throughput:** one publisher, one matcher and one notification worker do the work, and the matcher and the worker are serial. A standby takes over but does not share the load, a blocked event stops every symbol in the matcher, and an unresponsive destination costs the worker a full timeout per due job. In one local run on a 12-core machine, the publisher sent about 230 events per second and the worker delivered about 135 jobs per second.
- **Notifications:** they are not ordered among themselves, can arrive before the order is matched, and rely on a real destination remembering keys for longer than the retry budget. First start notifies every order still retained in the topic. See [ADR 0003](docs/adr/0003-phase-3-external-notifications.md#consequences).
- **Mock destination:** `/__control` has no authentication, and `mock-api` is for local use only. A real destination is not part of this project.
- **Retention and recovery:** Kafka keeps events for 30 days. Beyond that, or after losing Kafka's data, an operator republishes unapplied events from the Outbox by following [ADR 0002](docs/adr/0002-phase-2-messaging-and-matching.md#operator-procedures).
- **Local single broker:** one Kafka broker with a replication factor of 1, so an acknowledged event is on one machine only. A real deployment needs more.
- **Backlog limit:** during a Kafka outage, acceptance is limited only by PostgreSQL's disk, at about 1.4 KiB per order. Nothing refuses orders because of backlog. `GET /ops/backlog` reports its size, and [ADR 0004](docs/adr/0004-phase-4-evidence-and-operations.md#decision-the-backlog-limit) gives the reasons.
- **No alerting:** the backlog report and error logs are the signal, and a deployment connects them to its own alerting. There is no metrics endpoint.
- **Kafka versions:** the client declares support for Kafka 3.5.0 to 4.2.0. The image is the 4.2.2 patch of that line, and only that version has been run.
- **Demo identity:** there is no authentication, on `/v1`, `/health` or `/ops`. Every request shares the fixed scope `demo`.
- **Immutable market scales:** changing the scale of a market that has orders would misread stored amounts and stop old keys replaying.
- **Keys are kept forever:** by agreed decision, key mappings are never deleted automatically, so the table grows with every order.
- **Restart after a crash:** a consumer role waits up to one Kafka session timeout for its dead predecessor to expire. A clean stop has no such wait.
- **One Compose instance per role:** published health ports conflict with `docker compose up --scale`. A standby needs its own `HEALTH_PORT`.
- **Host ports in the ephemeral range:** `55432`, `58080` and `59092` can fail to bind for up to 60 s after local traffic. Recovery is in [If a port won't bind](#if-a-port-wont-bind).
- **Local PostgreSQL:** one instance with no replication and passwordless access on the loopback interface, for local use only.
- **Deferred features:** market orders, cancel and amend, wallets, balances and settlement, full authentication, Debezium or CDC, multiple concurrent owners of one order book, distributed matching failover, and Kubernetes manifests. The [architecture document](docs/architecture.md#deployment-tradeoffs) discusses deployment without manifests.
