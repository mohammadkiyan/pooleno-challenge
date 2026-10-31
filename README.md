# pooleno-order-processing

Accepts limit orders over HTTP and commits each one to PostgreSQL before answering `202 Accepted`, so a retry cannot create a second order and a crash cannot lose a confirmed one.

**Status: Phase 1 of 4, awaiting review.** Phase 1 covers the path from `POST` through the transaction, `202`, `GET` and a restart. Kafka, matching and external notifications start in later phases, so accepted orders stay `QUEUED` and their Outbox events stay pending.

**Contents:** [Features](#features) · [Getting started](#getting-started) · [API](#api) · [Configuration](#configuration) · [Tests](#tests) · [Layout](#layout) · [Known limitations](#known-limitations)

## Features

- **Atomic acceptance:** the order, idempotency receipt, symbol sequence and Outbox event commit in one transaction. The `202` is sent only after that commit.
- **Safe retries:** the same key and the same meaning replay the exact original response, even after the order has moved on. A changed request gets `422`.
- **Exact amounts:** decimal strings become integer units without passing through a float. Excess precision is rejected, never rounded.
- **Commit-ordered sequences:** a row lock on each symbol's counter is held until commit, so sequence order is commit order and a rollback leaves no gap.
- **Bounded waits:** a request blocked on another transaction gives up after a time limit and asks the client to retry with the same key.

## Getting started

You need Docker with Compose, Node.js 26 or newer, and free local ports `8080` and `55432`.

```bash
npm ci
docker compose up --build -d --wait
npm run demo
```

The demo accepts an order, retries it with different decimal spelling, reuses the key for a different order, sends too much precision, and reads the order back.

To prove that the order survives a crash, kill both processes and run the demo again:

```bash
docker compose kill -s SIGKILL api postgres
docker compose up -d --wait
npm run demo
```

The demo uses a fixed key, so the second run prints the same `orderId`, and the database still holds one order.

### Run the API on the host

```bash
docker compose stop api
docker compose up -d --wait postgres
cp .env.example .env
npm run migrate
npm run dev
```

`npm run build` followed by `npm start` runs the compiled version instead.

To stop everything, run `docker compose down`. Add `-v` to delete the database as well.

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

Reports the current state. Replace the ID with the `orderId` from your `POST` response:

```bash
curl http://127.0.0.1:8080/v1/orders/fbe6a61c-bbdd-4346-886d-af24e9de3847
```

```json
{
  "orderId": "fbe6a61c-bbdd-4346-886d-af24e9de3847",
  "symbol": "BTC-USD",
  "side": "BUY",
  "type": "LIMIT",
  "status": "QUEUED",
  "price": "100.00",
  "originalQuantity": "2.50000000",
  "executedQuantity": "0.00000000",
  "remainingQuantity": "2.50000000",
  "acceptanceSequence": "2",
  "acceptedAt": "2026-10-31T09:47:27.367Z",
  "updatedAt": "2026-10-31T09:47:27.367Z"
}
```

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

| Variable | Description | Default |
|---|---|---|
| `DATABASE_URL` | PostgreSQL connection string | Required |
| `HOST` | Listen address | `127.0.0.1` |
| `PORT` | Listen port; `0` picks a free one | `8080` |
| `LOG_LEVEL` | pino log level | `info` |
| `ACCEPTANCE_LOCK_TIMEOUT_MS` | Longest wait for a lock during acceptance, at least 1 | `1000` |

Chosen packages, markets, the demo scope and time budgets are recorded in [ADR 0001](docs/adr/0001-phase-1-tooling.md).

## Tests

```bash
docker compose up -d --wait postgres
npm run check
```

`npm run check` runs the typecheck, the linter and all tests. `npm run test:unit` needs no database. Database tests use real PostgreSQL and recreate a separate `pooleno_test` database from the migrations on each run. Set `TEST_DATABASE_URL` to use another server.

Production code has no test hooks. Each failure is injected from outside:

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

## Layout

```text
src/
  entrypoints/        Process entrypoints: api, migrate
  app/                API assembly, configuration, errors, signal handling
  infrastructure/     Pool, transaction helper, logger, migration runner
  modules/orders/     Routes, acceptance, idempotency, decimals, markets
  modules/messaging/  OrderAccepted v1 schema and Outbox insert
migrations/           SQL files, applied in name order
scripts/demo.ts       Repeatable walkthrough of the contract
tests/                unit, integration (PostgreSQL), e2e (real processes)
docs/adr/             Recorded decisions
```

## Known limitations

- **Acceptance only:** nothing reads the Outbox yet, so orders stay `QUEUED`. The guarantees here cover HTTP acceptance. This is not a claim of exactly-once behaviour for the complete workflow, and later phases deliver events at least once.
- **Demo identity:** there is no authentication. Every request shares the fixed scope `demo`.
- **No trades in `GET`:** there are no trades before matching exists. The field arrives with matching in Phase 2, which is an additive change.
- **Immutable market scales:** changing the scale of a market that has orders would misread stored amounts and stop old keys replaying.
- **Keys are kept forever:** by agreed decision, key mappings are never deleted automatically, so the table grows with every order.
- **Simple operations:** there are no health endpoints or in-flight draining with a deadline yet. The Compose check only proves the port is open. Phase 4 adds these along with backlog metrics.
- **Local PostgreSQL:** one instance with no replication and passwordless access on the loopback interface, for local use only.
