# ADR 0002: Phase 2 messaging and matching choices

- **Status:** accepted; Phase 2 was reviewed and approved on 2026-10-31
- **Date:** 2026-10-31

## Context

The handoff fixes the design of this phase: an ordered Outbox publisher with one owner, topic `orders.accepted` keyed by symbol, group `matching-v1`, a pure matching engine, and one transaction for trades, order changes, progress and Inbox. [ADR 0001](0001-phase-1-tooling.md) left two choices to this phase, the Kafka client and the balanced tree. This ADR records those, and the decisions the handoff left to the implementation.

## Decision: versions

Evidence is the registry state on 2026-10-31, from `npm view` and Docker Hub.

| Component | Pinned | Released | Reason |
|---|---|---|---|
| `@platformatic/kafka` | 2.14.0 | 2026-10-07 | See below |
| `sorted-btree` | 2.1.0 | 2025-12-11 | See below |
| `apache/kafka` | `4.2.2` | 2026-09-28 | Current patch of the newest line the client supports |

### Kafka client

| Candidate | Why it was or was not chosen |
|---|---|
| `@platformatic/kafka` 2.14.0 | **Chosen.** Maintained, Apache-2.0, pure JavaScript with no install scripts. Supports Node.js 26 and Kafka 3.5.0 to 4.2.0, idempotent producers and manual offset commits |
| `kafkajs` 2.2.4 | Last published on 2023-02-27 |
| `@confluentinc/kafka-javascript` 1.11.0 | Native module whose prebuilt binaries cover Node.js 18 to 24 only |
| `node-rdkafka` 3.6.1 | Native module that needs a build step |

Kafka 4.3.2 and 4.4.0 exist, but are outside the client's declared range, so the image stays on the 4.2 line.

These client behaviours were read from its source and are depended on deliberately:

- **Producer `retries`:** an idempotent producer forces `acks=ALL` and one request in flight, but also retries forever when `retries` is unset or at most 1. The publisher sets 3, so that a failed attempt reaches the code that records it durably.
- **Consumer defaults:** `autocommit` defaults to on and the fallback position to `latest`. Both would break the required behaviour, so both are set explicitly.
- **`message.commit()` is not used.** It queues the offset, which can then be flushed after the group has rejoined. `consumer.commit()` with `retries: 0` surfaces the failure to the code instead.
- **Closing a stream still delivers what is buffered,** so the loop checks a flag before handling each message.

### Balanced tree

`sorted-btree` 2.1.0 holds each side's price levels. It is MIT-licensed, has no dependencies, and takes a comparator, which `bigint` prices need.

| Candidate | Last published | Why it was or was not chosen |
|---|---|---|
| `sorted-btree` 2.1.0 | 2025-12-11 | **Chosen.** The most recently published of the candidates |
| `avl` 2.0.0 | 2025-07-21 | Would also work. Published less recently |
| `js-sdsl` 4.4.2 | 2024-02-20 | A whole container library at 1.1 MB unpacked, where only one sorted map is needed |
| `bintrees` 1.0.2 | 2017-08-05 | No release for years |
| `functional-red-black-tree` 1.0.1 | 2014-09-29 | No release for years |

The package is CommonJS with no named export, so it is imported the way Node.js and TypeScript's `nodenext` mode both see it. Vitest's default interop presents it differently, so `deps.interopDefault` is off in [vitest.config.ts](../../vitest.config.ts) and the tests load it as production does.

## Decision: topic

| Setting | Value | Reason |
|---|---|---|
| Partitions | 3 | Fixed for this version: changing it would move symbols between partitions and break their order across the change |
| Replication factor | 1 | The local stack has one broker. A real deployment needs more |
| Retention | 30 days | The recovery horizon, explained below |
| Auto-creation | Off | `kafka-init` creates the topic deliberately and refuses a wrong partition count |
| Partitioner | Pinned to `compatibilityPartitioner` | A changed client default must not move a symbol |

**Recovery horizon:** if matching stays down for longer than retention, unapplied events can be gone from Kafka. They are still in the Outbox, and the sequence guard blocks rather than skips, so an operator republishes them. See [Operator procedures](#operator-procedures).

## Decision: single ownership

Each of the publisher and the matcher holds a PostgreSQL session advisory lock on its own connection for as long as it works. A standby polls for the lock and does no work until it has it.

- The lock is released only after the role has returned, so a successor starts after the old process has stopped sending.
- Losing the lock's connection aborts the role, and the process exits with status 1. Compose then restarts it, and it queues for the lock again.
- The publisher also checks the connection before each pass, which catches a connection that has gone quiet without reporting an error.

This is enforced ownership for a single-owner design, not a proven distributed fencing protocol. A process that is frozen and then resumes could overlap with its successor for a moment, and two properties limit the damage:

- **Publisher:** an event is sent only after its predecessor is marked published. An overlapping sender can only repeat an event that Kafka already acknowledged, never invert the first occurrences, and the Inbox makes the repeat a no-op.
- **Matcher:** every change happens under the symbol's progress row lock, with the sequence guard, a check that the book matches the database, and compare-and-set updates on each order. A stale matcher fails these checks and restores.

## Decision: publishing

- Each pass takes the earliest unpublished event of each symbol, and sends it only if it is due. A failed or not-due head therefore holds back that symbol and no other.
- A failed send records `attempts`, `last_error` and `next_attempt_at`. The delay doubles from `OUTBOX_RETRY_BASE_MS` up to `OUTBOX_RETRY_CAP_MS`.
- `markPublished` is outside the `try` around the send. A database failure after an acknowledged send therefore cannot be recorded as a failed send. It stops the publisher, and the successor resends the same stored payload.
- `outbox_events.payload` is sent as stored, so a resend is identical.

**Backlog limit:** the handoff asks for a documented limit on acceptance during a Kafka outage. The limit is the disk available to PostgreSQL, and nothing refuses orders because of backlog. [ADR 0004](0004-phase-4-evidence-and-operations.md#decision-the-backlog-limit) revisits this with measurements and keeps it.

## Decision: the matching transaction

One event runs these steps in order, in one transaction:

1. Claim the Inbox row. If it already exists, the event is a no-op.
2. Lock the symbol's progress row.
3. Require `acceptanceSequence = last_applied_seq + 1`, or block with an error.
4. Require that the book is at `last_applied_seq`, or stop and restore.
5. Compute the match without changing the book.
6. Persist the taker, makers, trades and progress. Each order update is a compare-and-set on its expected remaining quantity, so a book that disagrees with the database aborts the transaction.
7. Commit.

Only then is the match applied to the book, and only then is the offset committed. A failure at any point ends the session, and the next one restores from the database.

- **Fixed Inbox name:** the Inbox uses the name `matching-v1` regardless of `MATCHING_GROUP_ID`, so a new Kafka group replaying the topic still recognises handled events.
- **No foreign key from `trades.event_id` to the Outbox,** so that Outbox retention can be added later without touching trades.
- **New constraint:** `orders_filled_iff_nothing_remains` makes `FILLED` and a zero remainder mutually dependent in the database.

## Decision: offsets and group membership

- Offsets are committed explicitly, per partition and one message at a time, after the transaction and the book update.
- A rebalance, a rejoin or a shutdown ends the session and suppresses any further commit. The redelivery that follows is a no-op through the Inbox. An uncommitted durable result is safe, and a commit made after losing a partition is not.
- A session that ends for any reason discards its book. The next session restores from the database, so a book never outlives the group membership it was built under.

**One lane per partition:** one process consumes all partitions, and each partition handles its messages one at a time while the others proceed. A poison event or a gap holds its own partition, restoring the affected book and retrying without committing, so the symbols on other partitions continue. The block the handoff requires still applies to everything behind the event on that partition. The one-active-matcher lock is unchanged. The session waits for every lane before it ends, so no transaction overlaps the next session.

**Bounded lanes:** each lane buffers at most `MATCHING_LANE_CAPACITY` messages. The client delivers all partitions through one stream and can pause only all of it, so a full lane holds back every partition until it has room. Dropping and refetching the excess would keep the others moving, but it relies on internals of the client. With the default of 10,000, the other partitions stall only after that many messages wait behind one stuck or slow event.

## Decision: trades in `GET`

`GET /v1/orders/{id}` gains a `trades` array: `tradeId`, `role` (`MAKER` or `TAKER`), `price`, `quantity` and `executedAt`. It omits the counterparty's order ID, which one client has no reason to learn about another's order. This is additive to the Phase 1 response.

## Operator procedures

Both block matching on purpose, and the error log names the position or sequence involved.

**A malformed event.** The event can never succeed, so the group must move past it.

```bash
docker compose stop matching
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --group matching-v1 \
  --topic orders.accepted:<partition> --reset-offsets --to-offset <offset + 1> --execute
docker compose start matching
```

Skipping is safe only for a message that is not a real event. If an accepted order was damaged, the next valid event for that symbol hits the sequence guard, which leads to the next procedure.

**Lost or missing events.** The Outbox still holds every accepted event, so the publisher can send those again. Retention can remove an older event while newer ones remain, and the client then moves an expired position to the oldest record it still has. Matching blocks there on the sequence guard, ahead of anything republished, so the group must move to the end of the topic first. Everything unapplied is then sent again behind that position, in order, and the publisher must be stopped throughout so that no new event lands ahead of its predecessors.

```bash
docker compose stop matching publisher
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --group matching-v1 \
  --topic orders.accepted --reset-offsets --to-latest --execute
docker compose exec postgres psql -U pooleno -d pooleno -c "
  UPDATE outbox_events e
     SET published_at = NULL, next_attempt_at = now(), attempts = 0
   WHERE e.published_at IS NOT NULL
     AND e.acceptance_seq > COALESCE(
       (SELECT p.last_applied_seq FROM matching_progress p WHERE p.symbol = e.symbol), 0);"
docker compose start publisher matching
```

If Kafka's data is gone altogether, run `docker compose run --rm kafka-init` before the reset, which needs the topic to exist.

Both repairs are covered by end-to-end tests, which make the same offset changes through the admin API. These commands were also run against the Compose stack, including a run with Kafka's volume deleted.

## Consequences

- Throughput is limited to one matcher, with one lane per partition. Database waits overlap across lanes, but matching calculations share one JavaScript thread, so a long calculation delays every lane and there is no CPU parallelism. This is the accepted design for version 1.
- Duplicate delivery is expected and absorbed. Nothing here claims exactly-once behaviour across HTTP, PostgreSQL and Kafka, and the guarantee is at-least-once delivery with idempotent effects.
- After a crash, restart takes up to the Kafka session timeout, because the group waits for the dead member to expire.
- An event that keeps failing holds its partition and retries every `MATCHING_RESTART_DELAY_MS`, logging an error each time, until an operator acts. Readiness reports `a partition is held on a failing event`, and `GET /ops/backlog` reports the lag. Nothing alerts on it ([ADR 0004](0004-phase-4-evidence-and-operations.md#limits-of-the-figures)). A failure shared by all lanes, such as a database outage, holds them all.
- One large order delays the symbols on its partition, because they share a lane. Other partitions continue: in one run through the real role, with a 400,000-maker BTC order on partition 0, an ETH order on partition 2 finished at about 1.6 s and the BTC order at about 15.5 s. Each statement must finish within `MATCHING_STATEMENT_TIMEOUT_MS`, or the order retries and holds its partition until the limit is raised. A timed-out order must not be skipped, because it is a real accepted order. One run each on a development machine, through the real role with the 60 s default: 400,000 makers took about 16 s, 1,000,000 about 42 s and 2,000,000 about 101 s, all including startup. At the former 5 s limit, 400,000 makers never completed. The limit was not reached in these runs, so the largest order it allows is unknown, and no cap on makers per order exists.
- Applying `002_matching.sql` to a database with data takes short write-blocking locks to build indexes, and the new check validates existing rows. Orders from Phase 1 are all `QUEUED` with a remainder, so they pass.
- `fetch` in Node.js 26.7.0 (undici 8.9.0) stalled for 2.5 to 30 seconds when it reused an idle keep-alive connection. The same requests took milliseconds through a raw socket or `http.Agent`, and `fetch` stalled against a bare `node:http` server too, so the API is not the cause. The demo and the end-to-end tests send `connection: close`, which avoids it. The cause inside `fetch` was not traced, so Phase 3's outbound calls should not depend on its connection reuse without checking this first.
