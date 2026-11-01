# ADR 0001: Tooling, markets and budgets

- **Status:** accepted
- **Date:** 2026-10-31

## Context

The repository started empty, so there were no existing conventions to reuse. The stack is Node.js and TypeScript, Fastify and TypeBox, PostgreSQL, Kafka and Docker Compose, with direct SQL through `pg`, SQL migrations, Vitest, tsx and npm. This stack leaves these choices open:

- runtime, image and library versions
- market precision and limits
- demo identity scope
- retry and time budgets

This ADR settles them. [ADR 0002](0002-messaging-and-matching.md) settles the Kafka client and the balanced tree. [ADR 0005](0005-nestjs-on-fastify.md) later put NestJS on top of Fastify, which changed the rows marked below.

## Decision: versions

Versions are exact in `package.json`, locked in `package-lock.json`, and pinned by tag in the container images. Evidence is the registry state on 2026-10-31, from `npm view`, the Node.js release index and Docker Hub.

| Component | Pinned | Released | Reason |
|---|---|---|---|
| Node.js | `26.13.0-alpine3.24` | 2026-10-28, LTS | Current LTS line |
| PostgreSQL | `18.6-alpine3.24` | On Docker Hub | Newest stable major; 19 is still `19rc1` |
| `fastify` | 5.12.7 | 2026-10-13 | HTTP server under Nest, and the mock API's server (changed by ADR 0005) |
| `typebox` | 1.4.3 | 2026-10-22 | Request and response schemas |
| `pg` | 8.23.1 | 2026-09-30 | Direct SQL, with no ORM |
| `pino` | 10.4.0 | 2026-10-02 | Fastify's own logger, and Nest's through an adapter (changed by ADR 0005) |
| `typescript` | 6.0.3 | see below | Newest version the linter supports |
| `typescript-eslint` | 8.72.1 | 2026-10-29 | Type-aware strict rules |
| `eslint` | 10.14.0 | 2026-10-30 | Lint command |
| `vitest` | 5.0.5 | 2026-10-19 | Test runner |
| `tsx` | 4.23.15 | 2026-09-20 | Runs TypeScript in development |

`@types/pg` 8.23.1 and `@types/node` 26.6.6 follow their runtime packages.

### Compatibility checks

- **TypeScript:** 7.0.2 is the newest release, but `typescript-eslint` 8.72.1 declares the peer range `>=4.8.4 <6.1.0`. Type-aware linting takes priority, so the project uses 6.0.3.
- **Fastify and pino:** Fastify 5.12.7 requires `^9.14.0 || ^10.1.0`. 10.4.0 satisfies it.
- **TypeBox:** handlers use its `Static` types directly, since ADR 0005 removed the type provider. Validation runs in Fastify's own Ajv.
- **Vitest and Node.js:** Vitest 5 supports Node.js `>=26`. The test suite ran on Node.js 26.7.0 on the host, and the API ran on 26.13.0 in its image.
- **PostgreSQL 18:** the official image keeps data under `/var/lib/postgresql`, so the named volume mounts there. Data surviving `SIGKILL` and a restart confirms the path.
- **Production audit:** `npm audit --omit=dev` reported 0 vulnerabilities when the dependencies were installed.

## Decision: validation behaviour

Fastify's Ajv defaults coerce types and remove unknown properties. A JSON number would become a string after already passing through a float, and an unknown property would vanish before the fingerprint. Both defaults are off, and tests cover the rejections.

`orderId` uses a lowercase UUID pattern instead of `format: 'uuid'`, which also accepts a `urn:uuid:` prefix that PostgreSQL rejects.

## Decision: market configuration

Markets live in `src/modules/orders/markets.ts`. The values are demonstration values.

| Symbol | Price scale | Price tick | Maximum price | Quantity scale | Quantity step | Maximum quantity |
|---|---|---|---|---|---|---|
| `BTC-USD` | 2 | 0.01 | 10,000,000.00 | 8 | 0.00000001 | 10,000.00000000 |
| `ETH-USD` | 2 | 0.05 | 1,000,000.00 | 4 | 0.0010 | 100,000.0000 |

- **Rounding policy:** none. Excess precision is rejected. Trailing zeros carry no precision, so `100`, `100.0` and `100.000` are the same value.
- **Immutable scales:** a market's scale must not change once it has orders. Stored units would be read at the wrong scale, and old keys would stop replaying.

## Decision: demo identity scope

Every request uses the fixed scope `demo`. It is a server constant and is never read from the request, so a client cannot claim another identity. Authentication is deferred, and a real deployment would derive the scope from the authenticated account.

## Decision: request limits and time budgets

| Budget | Value | Purpose |
|---|---|---|
| `lock_timeout` in acceptance | 1 s, set by `ACCEPTANCE_LOCK_TIMEOUT_MS` | Bounds the wait on a key or symbol counter |
| `statement_timeout` | 5 s (matching: 60 s, set by `MATCHING_STATEMENT_TIMEOUT_MS`) | Bounds any single statement. Matching writes all of one order's fills in a single update and a single insert, so it gets a higher limit |
| `idle_in_transaction_session_timeout` | 10 s | Frees locks held by a stalled client |
| Connection timeout | 2 s | Fails fast when the pool or database is unavailable |
| Pool size | 10 | Keeps the number of concurrent transactions small |
| `Retry-After` on 409 and 503 | 1 s | Tells clients when to retry |
| Body limit | 16 KiB | An order is far smaller |
| `Idempotency-Key` | 16 to 128 characters of `A-Za-z0-9_-` | Excludes the comma, so a repeated header is rejected |

A lock timeout on the key is a `409`: the same request is still running. A lock timeout on the symbol counter is a `503`: a different request is holding the symbol. The second is not a `409`, because telling that client its own request is in flight would be false.

The `409` and `422` responses are this API's explicit contract. The IETF `Idempotency-Key` header specification is still a draft.

## Consequences

- Upgrading to TypeScript 7 waits for a compatible `typescript-eslint` release.
- Any unexpected error returns `503` with advice to retry the same key. That is correct for an ambiguous commit, because the retry replays or accepts. It also means a programming error looks like an outage to the client, so server logs record those failures at error level.
