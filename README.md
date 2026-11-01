# pooleno-order-processing

Accepts limit orders over HTTP, commits each one to PostgreSQL before answering `202 Accepted`, publishes it through an Outbox to Kafka, matches it in price-time order and notifies an external API. A retry cannot create a second order, a crash cannot lose a confirmed one, a redelivered event cannot repeat a trade, and a lost response cannot repeat a notification's effect.

## Run everything with one command

You need Docker with the Compose plugin, and nothing else, not even Node.js:

```bash
./review.sh
```

It takes about 5 minutes, plus image downloads the first time. Starting from nothing, it does the following, stops at the first failure and tells you where to look:

1. Builds the images.
2. Runs the typecheck, the linter and all tests, against real PostgreSQL and real Kafka.
3. Starts the whole stack.
4. Walks through the acceptance contract, a price-time matching fixture and the external notification, checking every answer.
5. Proves the crash guarantees with real `SIGKILL`s: it kills the publisher, gets a `202` for an order, then kills every process including PostgreSQL and Kafka. After the restart, that order has matched and notified, the rebuilt book kept its queue and remainders, and no effect repeated.
6. Leaves the stack running for you to try.

A passing run ends like this:

```text
Crash proof: after the restart
  ok  After the restart the saved order was published and matched, not lost
  ok  The rebuilt book kept the queue and the remainder: A finished first, then B
  ok  Maker A has one trade from each taker, none repeated
  ok  The saved order caused exactly one effect at the destination
  ok  No notification key was applied twice

Run the walkthrough again, with the same keys      ok (1s)
  ok  The same orders, trades and effect as before the crash, and nothing new

ALL CHECKS PASSED in 4m 12s
...
  Unit tests:                184 passed
  Integration and e2e tests: 199 passed
```

| Command | What it does |
|---|---|
| `./review.sh` | Everything above |
| `./review.sh --skip-tests` | The same without the typecheck, linter and tests, in about a minute after the first build |
| `./review.sh down` | Stops the stack and deletes its data |

`./review.sh` first deletes this Compose project's containers and volumes, and touches nothing else. If a port is taken, it names the port and the variable that moves it, such as `HOST_PORT_API=9080 ./review.sh`.

## Try it yourself

With the stack running, place an order, then read it back using the `orderId` from the answer:

```bash
curl -i http://127.0.0.1:8080/v1/orders \
  -H 'content-type: application/json' \
  -H 'idempotency-key: my-first-order-0001' \
  -d '{"symbol":"ETH-USD","side":"BUY","type":"LIMIT","price":"100.00","quantity":"1.5"}'

curl -s http://127.0.0.1:8080/v1/orders/<orderId>
```

The walkthrough leaves sell orders resting at 100 on `ETH-USD`, so this one trades straight away. Send it again with the same key to see the same answer replayed.

| To see | Run |
|---|---|
| What is queued, retrying or stuck | `curl -s http://127.0.0.1:8080/ops/backlog` |
| What the mock destination received | `curl -s http://127.0.0.1:18080/__control/effects` |
| Whether a role is ready (8081, 8082, 8083) | `curl -i http://127.0.0.1:8082/health/ready` |
| A Kafka outage, a lost response, a failed job | [Walkthroughs in Operations](docs/operations.md#see-it-fail) |

## Where each requirement is

| Assignment | Code | Explanation and proof |
|---|---|---|
| Receive and validate orders | [modules/orders](src/modules/orders) | [API](docs/api.md), [acceptance.test.ts](tests/integration/acceptance.test.ts), [decimal.test.ts](tests/unit/decimal.test.ts), the walkthrough |
| Price priority, FIFO, partial execution | [modules/matching](src/modules/matching) | [matching.test.ts](tests/unit/matching.test.ts), including 15,000 random orders against a reference matcher, and the walkthrough |
| Data structures and time complexity | [order-book.ts](src/modules/matching/order-book.ts), [price-level.ts](src/modules/matching/price-level.ts) | [Matching: data structures and complexity](docs/architecture.md#matching-data-structures-and-complexity) |
| Publish order events to Kafka | [publish-outbox.ts](src/modules/messaging/publish-outbox.ts) | [outbox.test.ts](tests/integration/outbox.test.ts), [crash-recovery.test.ts](tests/e2e/crash-recovery.test.ts) |
| Consumer calls a mock external API | [modules/external](src/modules/external), [mock-api](mock-api) | [external-retry.test.ts](tests/integration/external-retry.test.ts), the walkthrough |
| Failed calls, retries, duplicates, consumer failures | [notification-worker.ts](src/modules/external/notification-worker.ts), [consumer-session.ts](src/modules/messaging/consumer-session.ts) | [Failures and retries](docs/architecture.md#kafka-publishing-offsets-and-guarantees), [failure tables](docs/testing.md#failure-tables) |
| Offsets and processing guarantees | [consumer-session.ts](src/modules/messaging/consumer-session.ts) | [Kafka: publishing, offsets and guarantees](docs/architecture.md#kafka-publishing-offsets-and-guarantees) |
| No event loss after a crash | [accept-order.ts](src/modules/orders/accept-order.ts) | [Preventing event loss](docs/architecture.md#preventing-event-loss-after-a-crash), the crash proof in `./review.sh` |
| Scaling consumers, preserving order | [partition-lanes.ts](src/modules/messaging/partition-lanes.ts) | [Scaling and ordering](docs/architecture.md#scaling-consumers-and-preserving-ordering) |
| Kubernetes, graceful shutdown, health checks | [app](src/app), [modules/operations](src/modules/operations) | [Kubernetes](docs/architecture.md#kubernetes-shutdown-and-health), [lifecycle.test.ts](tests/e2e/lifecycle.test.ts) |

## Documentation

- [Architecture](docs/architecture.md): the short design document, with the architecture, complexity and key decisions. Start here.
- [API](docs/api.md): endpoints, statuses, errors and markets.
- [Operations](docs/operations.md): failure walkthroughs, health, backlog, repairs, running on the host and configuration.
- [Testing](docs/testing.md): how to run the tests, the results, and every injected failure with what it proves.
- [ADRs](docs/adr): the reasoning and evidence behind [tooling and budgets](docs/adr/0001-tooling-and-budgets.md), [messaging and matching](docs/adr/0002-messaging-and-matching.md), [external notifications](docs/adr/0003-external-notifications.md), [shutdown, health and visibility](docs/adr/0004-shutdown-health-and-visibility.md), and [NestJS on Fastify](docs/adr/0005-nestjs-on-fastify.md).

[extras/](extras) holds an optional illustrated scenario viewer. Nothing here depends on it, and you can review the project without it.

## Develop on the host

With Node.js 26 or newer:

```bash
npm ci
docker compose up -d --wait postgres kafka
npm run check
```

[Run on the host](docs/operations.md#run-on-the-host) has each role's command.

## Layout

```text
src/
  entrypoints/        Process entrypoints: api, publisher, matching, external, redrive-external,
                      migrate, init-kafka
  app/                Nest API assembly, role runner, configuration, errors, signals, health server,
                      single ownership
  infrastructure/     Pool module, transactions, session lock, logger, migration runner
  modules/orders/     Controller, acceptance, idempotency, decimals, markets
  modules/messaging/  Publisher role, event schema, Outbox, Kafka clients, Inbox, consumer
  modules/matching/   Matching role, pure engine and book, the transaction and restore around them
  modules/external/   External role, job handoff, worker, HTTP client and job storage
  modules/operations/ Controller for health and backlog, backlog report and Kafka lag
mock-api/             Stand-in destination with durable idempotency, sharing nothing with src/
migrations/           SQL files, applied in name order
scripts/              The walkthrough and the crash proof that ./review.sh runs
tests/                unit, integration (PostgreSQL), e2e (real processes and Kafka)
docs/                 Design document, API, operations, testing and ADRs
```

## Key limitations

- **Delivery guarantee:** at least once, with idempotent effects. The complete workflow is not exactly once.
- **Throughput:** the publisher, matching and external roles each have one active owner. A standby takes over but doesn't share the load.
- **Local setup:** one Kafka broker, one PostgreSQL and no authentication, so this stack is for local use only.
- **Out of scope:** market orders, cancel and amend, wallets and settlement, and Kubernetes manifests.

The [full list](docs/architecture.md#limits) is in the design document.
