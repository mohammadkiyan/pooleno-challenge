# Architecture

This document explains how an accepted order moves through the system, what each step costs, what happens when a process dies between two steps, and how far the design scales. Commands and sample calls are in the [README](../README.md). The choices behind each part are in the [ADRs](adr/).

The original assignment does not mandate asynchronous matching, `202`, PostgreSQL or request idempotency. These are intentional design choices, made so that a crash at any point loses nothing a client was told about.

## Overview

```text
client ── POST ──▶ API ── one transaction ──▶ PostgreSQL
                                              order · receipt · counter · Outbox event
                                                 │
                                       publisher (one owner)
                                                 │ key = symbol
                                                 ▼
                                      Kafka: orders.accepted
                                 ┌───────────────┴────────────────┐
                        group matching-v1            group external-notification-v1
                       matching (one owner)              external (one owner)
                        book in memory                    Inbox + job, then worker
                                 │                                │ key = external-notify:<eventId>
                                 ▼                                ▼
                    PostgreSQL: trades, orders,            destination API
                         progress, Inbox
```

PostgreSQL is the source of truth. Kafka carries work between roles, and the in-memory book is a cache of committed rows. Any role can be killed at any moment, because each one resumes from durable records.

## What `202` means

`202 Accepted` means that the order, its idempotency receipt, its symbol sequence and its Outbox event are committed together. It does not mean the order has matched.

- **Accepted orders are never rejected later.** Validation happens before the commit. Infrastructure failure delays an order and never changes its outcome.
- **Clients learn the result by polling.** `GET /v1/orders/{orderId}` reports `QUEUED`, then `OPEN`, `PARTIALLY_FILLED` or `FILLED`.
- **A retry is safe.** The same key and meaning replay the original response. If an answer is lost, the client retries with the same key.
- **Kafka is not on this path.** Acceptance continues while Kafka is down, limited only by the [backlog limit](adr/0004-phase-4-evidence-and-operations.md#decision-the-backlog-limit).

## Data structures and complexity

Let P be the number of price levels on a side, K the makers touched by one order, L the levels emptied by it, and N the resting orders.

| Structure | Implementation | Why |
|---|---|---|
| Price levels on one side | `sorted-btree` B+ tree keyed by `bigint` price, best price first | Ordered walk, plus `O(log P)` insert and delete |
| Orders at one price | Singly linked list in [price-level.ts](../src/modules/matching/price-level.ts) | True `O(1)` append and pop. `Array.shift()` would be `O(n)` |
| Books | `Map` from symbol to `OrderBook` | One independent book per symbol |
| Amounts | `bigint` integer units, `numeric` in PostgreSQL | Exact. No value passes through a float, and the size of a value is bounded by market limits |

These are the costs of the code as written:

| Operation | Cost | Where the cost comes from |
|---|---|---|
| Add an order at an existing price | `O(log P)` | Tree lookup, then `O(1)` append |
| Add an order at a new price | `O(log P)` | Tree lookup and insert |
| Pop the oldest order of a level | `O(1)` | Linked list |
| Compute a match, `matchOrder` | `O(log P + K + L)` | Starts one ordered walk, and changes nothing |
| Apply a match, `applyMatch` | `O(log P + K + L log P)` | Finds the best level once, then again only after emptying one, and deletes each emptied level |
| Restore all books | `O(N log P)` plus reading N rows | One `add` per resting order. A partial index serves the query, and all N rows load into one array first, so restore briefly holds `O(N)` extra memory |
| Memory | `O(N + P)` | One node per order, one tree entry per level |

The best level is not cached between orders, so each `applyMatch` starts with a tree lookup. Within one order it is held until that level empties, so the bound is the one above and not `O((K + 1) log P)`.

The database work grows with K as well. One matching transaction writes the taker, K makers, K trades, one progress row and one Inbox row, then commits, so the database is the likely limit rather than the tree. That's an inference, because the code wasn't profiled.

In a local drain of 5,000 orders that didn't trade (K = 0), matching finished 1.6 s after the publisher. It kept up at about 215 orders per second, so that figure is a floor for one machine, not a capacity claim.

## Transactions

Each step commits durably before the next one starts. No transaction spans PostgreSQL and Kafka.

| Step | What commits together | A crash before it | A crash after it |
|---|---|---|---|
| Acceptance | Order, receipt, counter, Outbox event | Nothing is kept, and the client's retry is accepted with no sequence gap | The retry returns the original `orderId` |
| Publish | Kafka acknowledges, then `published_at` is set, separately | Not sent, or sent and not marked. Either way, the event is sent again | Done |
| Matching | Inbox row, taker, makers, trades, progress | Nothing is kept, and the event is redelivered | The offset is not yet committed, so the event returns and the Inbox makes it a no-op |
| Notification handoff | Inbox row and job | Nothing is kept, and the event is redelivered | The redelivery creates no second job |
| Delivery result | Job status and attempts, compare-and-set | The remote effect may exist with no local record. The retry uses the same key, and the destination replays | Done |

The book changes only after the matching commit, and the offset only after the book. If a commit's reply is lost, the role can't know the result, so it discards the book and restores from what committed. The [test tables](../README.md#tests) cover each row with a real crash.

## Offsets

Offsets are committed by hand, as `offset + 1`, and only after the event's work is durable. A message can therefore be handled twice but never skipped.

- **After a rebalance,** a role commits nothing further for the partitions it lost, and the new assignment starts from the last committed offset.
- **After a crash,** the group waits for the dead member's session to expire before reassigning, up to the role's session timeout of 10 s by default. A clean shutdown leaves the group at once, with no such wait.
- **Each group has its own offsets,** so matching being blocked does not stop notifications, and the reverse.

## Retries

| Failure | Behavior | Limit |
|---|---|---|
| Kafka send fails | The event stays in the Outbox, and its symbol waits. The delay doubles from 500 ms to 30 s | None. It retries for as long as the outage lasts |
| Matching event fails | Only its partition is held. The role restores that event's book, waits 1 s, and retries it without committing. Other partitions continue | None. A bad event holds its partition until an operator acts |
| Matching session fails | A Kafka failure discards every book. The role waits 1 s, restores, and starts again | None |
| Sequence gap, or a malformed message at matching | Holds its partition with an error. Later orders depend on this one, so it can't be skipped | Needs the [operator procedure](adr/0002-phase-2-messaging-and-matching.md#operator-procedures) |
| Malformed message at the external role | Quarantined durably, then skipped. Notifications don't depend on each other | Inspected under the [quarantine procedure](adr/0003-phase-3-external-notifications.md#operator-procedures) |
| Destination fails transiently | The job retries with jitter and honours `Retry-After`. The delay doubles from 1 s to 5 min | 400 attempts, about a day, then `FAILED` |
| Destination rejects permanently | The job is `FAILED` at once, for a manual redrive under the same key | Needs an operator |

## Deduplication

Delivery is at least once, and every effect is idempotent. **The complete workflow across HTTP, PostgreSQL, Kafka and the destination is not exactly once.** Each layer removes the duplicates that it can see:

| Layer | Mechanism | What it prevents |
|---|---|---|
| Client to API | `Idempotency-Key`, unique per scope, plus a fingerprint of the request's meaning | A second order from a retry |
| Publisher to Kafka | Idempotent producer | A duplicate from an internal client retry |
| Publisher crash | Same stored payload and same `eventId` on resend | A different event for one order. Duplicates remain possible, and consumers absorb them |
| Kafka to matching | Inbox row per consumer and event, in the matching transaction | A second trade |
| Old or unknown events | Row lock on progress, `acceptanceSequence = last_applied_seq + 1`, a check that the book matches the database, and compare-and-set on orders | A stale or out-of-order event changing state |
| Kafka to external | Inbox row and unique job per event | A second job |
| Worker to destination | Stable key `external-notify:<eventId>`, and the same bytes on every attempt | A second effect, for as long as the destination remembers the key |

The local Inbox can't close the gap between a remote success and its local record. Only the destination's own idempotency closes it.

## Restore

The matching role keeps no state on disk. When it gains ownership, or after any session ends, it discards every book and rebuilds them:

1. One repeatable-read snapshot reads `matching_progress` and every `OPEN` or `PARTIALLY_FILLED` order, in `acceptance_seq` order.
2. Each order is added to its book, which reproduces FIFO within each price and the correct remainder.
3. The role joins the consumer group and continues from the last committed offset.

`QUEUED` orders are left out because they enter the book through their own events. Events that were already applied return as no-ops, and a book that disagrees with the database aborts the transaction and restores again. A random-workload test checks that a restored book equals the live one.

## Ordering and scale

**Order within a symbol is the invariant.** Acceptance assigns each symbol a gap-free sequence under a row lock. The publisher sends only the earliest unpublished event of each symbol. The Kafka key is the symbol, so all of a symbol's events share one partition. Matching rejects any event that isn't the next in sequence.

**Partition-key ownership bounds consumer scale.** A partition has at most one active consumer in a group, so the topic's 3 partitions allow at most 3 useful consumers per group, and a fourth would sit idle. All of a symbol's events are in one partition, so one symbol is never processed in parallel, however many consumers run. Partitions can't be added later without moving symbols between partitions and breaking their order across the change.

**Parallelism is possible across books only.** A faster single symbol would need a faster matcher, not more of them. The one matcher gives each partition its own sequential lane, because no state crosses books. A slow database write or a failing event therefore holds only its partition.

The lanes have limits. Symbols that share a partition still wait for each other. Lanes overlap only during asynchronous waits, because matching calculations share one JavaScript thread. A database outage holds every lane. Each lane buffers at most `MATCHING_LANE_CAPACITY` messages, and a full one holds back all partitions, because the client delivers them through one stream and can pause only all of it.

**The publisher, matching and external roles each have one active owner**, proved by a PostgreSQL session advisory lock. A standby waits for the lock, and takes over when the owner's connection ends. A consumer group alone isn't enough, because a member that has lost its partition can still be writing to PostgreSQL while its successor starts.

**The API has no owner.** It keeps nothing outside PostgreSQL, and row locks serialize each symbol's counter, so it can run as several replicas. The tests send concurrent requests to one API process, not to several.

**Multiple matchers need more than a second replica.** Rebalance and failover need fencing, or an equivalent proof of ownership, before multiple matchers can be enabled:

- A matcher would load only the books of its assigned partitions, and discard them on revocation.
- Today's checks limit the damage of a brief overlap. The progress row lock, the sequence guard, the check that the book matches the database and compare-and-set on orders make a stale matcher fail and restore. They aren't proof of ownership, and two active matchers on one symbol would keep failing each other rather than share work.
- The transaction would need to check ownership itself, for example an epoch per partition that a new owner advances before it restores.
- Live transfer of a book isn't needed, because restore already rebuilds one from committed rows.

In one local run on a 12-core machine, the API accepted about 460 orders per second, the publisher sent about 230 events per second, and the worker delivered about 135 jobs per second. Each ownership role can grow only through faster hardware today, which is the accepted limit of this version.

## Shutdown

On `SIGTERM` or `SIGINT`, each role does the following:

1. Reports not ready, and stops taking new work: no new HTTP connections, Outbox passes, messages or attempts.
2. Finishes what is in flight: the open request, transaction, send or attempt.
3. Commits offsets only for completed work, leaves the group, disconnects from Kafka, and releases its lock and pool.
4. Exits with status 0.

`SHUTDOWN_TIMEOUT_MS`, 8 s by default, bounds all of this. Past the deadline, the role exits with status 1, and a second signal ends it at once. Both are safe, because a forced exit is the same as a crash, and durable records drive replay. Compose's `stop_grace_period` is 10 s, so a role always stops by itself first.

| Measured with `docker compose stop` | Time | Exit |
|---|---|---|
| Any role, healthy dependencies | 0.3 s | 0 |
| Publisher, Kafka hung, idle | 0.3 s | 0 |
| Matching or external, Kafka hung | 8.3 s | 1, at the deadline |

## Health

Liveness reports that the process runs, and never looks at a dependency. Readiness reports whether the role can do its work now.

| Role | Port | Ready when | A failed check reports |
|---|---|---|---|
| API | 8080 | The pool can lend a connection and run a query | `database unavailable` |
| Publisher | 8081 | It owns the role, and Kafka answers | `waiting for ownership`, `ownership cannot be confirmed`, `kafka unreachable` |
| Matching | 8082 | It owns the role, its books are restored, and it has partitions | `waiting for ownership`, `ownership cannot be confirmed`, `no partitions assigned`, `a partition is held on a failing event` |
| External | 8083 | It owns the role, and it has partitions | `waiting for ownership`, `ownership cannot be confirmed`, `no partitions assigned` |

The ownership roles also report `shutting down`. The API stops listening instead, so a probe gets a refused connection, or Fastify's `503` on a connection that was already open. The destination being down is a delivery condition, so it changes neither check on the external role.

## Visibility

`GET /ops/backlog` on the API reports one consistent snapshot, plus Kafka lag. With Kafka down or hung, it still answers within about 2 s, with `"kafka": {"available": false}`.

| Question | Field |
|---|---|
| How much is waiting to reach Kafka, and for how long? | `outbox.pending`, `outbox.oldestPendingAgeMs` |
| Is sending failing? | `outbox.retrying`, `outbox.totalAttempts` |
| Which symbols are held back? | `outbox.blockedSymbols` |
| How many accepted orders are unmatched, and for how long? | `matching.lag`, `matching.symbols` |
| Are notifications behind or stuck? | `external.pending`, `external.retrying`, `external.oldestPendingAgeMs`, `external.failed` |
| Did a message get set aside? | `external.quarantined` |
| How far is each group behind the topic? | `kafka.groups` |

Matching lag and Kafka lag answer different questions. Matching lag counts every accepted order not yet applied, including those still in the Outbox. Kafka lag counts only what reached the topic, so during an outage it can read zero while matching lag grows.

Structured logs carry `role`, `requestId`, `orderId`, `eventId`, `symbol` and `acceptanceSequence` on every step of an order's path. The `requestId` travels in the event as `correlationId`, so one search follows a request across all four roles. Raw idempotency keys and connection details stay out of logs and answers.

## Deployment tradeoffs

This repository contains no Kubernetes manifests. If you run the roles there, these are the points that matter:

- **Termination:** set `terminationGracePeriodSeconds` above `SHUTDOWN_TIMEOUT_MS`.
- **Rolling the API:** `app.close()` refuses new connections at once, but endpoints take a moment to update. Add a short `preStop` delay so traffic drains first.
- **Liveness and readiness:** map them to `/health/live` and `/health/ready`, and set `HOST` and `HEALTH_HOST` to `0.0.0.0` so the kubelet can reach them. A readiness failure on a dependency outage removes the role from service without restarting it, which is the intent.
- **Ownership roles:** run one replica of the publisher, matching and external, and let Kubernetes restart it. A standby is correctly not ready, so a two-replica Deployment reports one unavailable replica forever, and a rollout may wait on it. The publisher, matcher and external have no client traffic, so they need no Service.
- **Restart time:** a crashed consumer role returns after up to one Kafka session timeout. A clean rollout doesn't wait for it.
- **Exposure:** an ingress must not route `/health/*` or `/ops/*`, because they have no authentication.
- **Kafka:** the local stack has one broker with a replication factor of 1. A real deployment needs more brokers, and matching replication and `min.insync.replicas`.

## Assignment evidence

Each assignment bullet maps to a test, demo or paragraph that shows it working.

| Assignment bullet | Evidence |
|---|---|
| Receive and validate orders | [acceptance.test.ts](../tests/integration/acceptance.test.ts), [decimal.test.ts](../tests/unit/decimal.test.ts), and the contract part of `npm run demo` |
| Price priority, FIFO, partial execution | [matching.test.ts](../tests/unit/matching.test.ts), including non-crossing orders, symmetry, a maker spanning takers, level deletion, maximum units, and a comparison with a reference matcher over 15,000 random orders. Also the fixture part of `npm run demo` |
| Explain data structures and complexity | [Data structures and complexity](#data-structures-and-complexity), including matching and restore |
| Publish acceptance to Kafka | [outbox.test.ts](../tests/integration/outbox.test.ts), and "accepts orders while Kafka is down, then publishes each symbol in order" in [crash-recovery.test.ts](../tests/e2e/crash-recovery.test.ts) |
| Consumer calls a mock API | [external-retry.test.ts](../tests/integration/external-retry.test.ts), [mock-api/](../mock-api), and the notification part of `npm run demo` |
| Failures, retries, duplicates, offsets | [Transactions](#transactions), [Offsets](#offsets), [Retries](#retries), [Deduplication](#deduplication), and every crash test in [the README's tables](../README.md#tests) |
| Crash after saving an order | "recovers when the process dies after commit but before the client hears back" in [crash-recovery.test.ts](../tests/e2e/crash-recovery.test.ts), and the kill-all sequence in the [README](../README.md#getting-started) |
| Scaling and ordering | [Ordering and scale](#ordering-and-scale), and "assigns gap-free, unique sequences to concurrent orders on one symbol" in [acceptance.test.ts](../tests/integration/acceptance.test.ts) |
| Kubernetes reliability | [Shutdown](#shutdown), [Health](#health), [Deployment tradeoffs](#deployment-tradeoffs), and [lifecycle.test.ts](../tests/e2e/lifecycle.test.ts). The handoff asks for no manifests |
| Delivery | [README](../README.md), this document, `npm run check`, and the [ADRs](adr/) |

## Required tests

The handoff lists these failures and the result each must have. Tests that claim a delivery guarantee run against real PostgreSQL and real Kafka.

| Injected failure | Required result | Test |
|---|---|---|
| Concurrent identical `POST`s | One order and one acceptance event | "creates one order when the same request arrives 25 times at once" in [acceptance.test.ts][acceptance] |
| Same key, different meaning | `422`, and the original receipt is unchanged | "rejects a changed payload with 422 and leaves the first request untouched" in [acceptance.test.ts][acceptance] |
| Crash after the acceptance commit, before the response | The retry returns the original receipt | "recovers when the process dies after commit but before the client hears back" in [crash-recovery.test.ts][crash] |
| Failure between the order and Outbox writes | No partial acceptance, and no advanced counter | "leaves nothing behind for a symbol with no counter yet, and the retry succeeds" and "rolls back an advance of an existing counter, leaving no gap" in [acceptance.test.ts][acceptance] |
| Kafka offline | The order stays accepted, and publishes in order on recovery | "accepts orders while Kafka is down, then publishes each symbol in order" in [crash-recovery.test.ts][crash] |
| Send succeeds, crash before `published_at` | The same event resends, with no duplicate effect | "resends the same event after a crash before it is marked, with one effect" in [crash-recovery.test.ts][crash] |
| Matching commit, crash before the offset | No second trade, and the correct remainder | "creates no second trade when it dies between its commit and the offset" in [crash-recovery.test.ts][crash] |
| Failure or uncertain commit after computing in memory | The book stops and restores before the next event | "stays correct when a commit succeeds but its reply is lost" and "stops and restores correctly when RAM cannot apply a committed match" in [matching-recovery.test.ts][matching] |
| Old event after newer ones | A no-op with FIFO kept, and a sequence gap blocks | "keeps the original FIFO when an old event returns after newer ones" and "blocks on a gap and leaves no trace of the event" in [matching-recovery.test.ts][matching] |
| Remote effect commits, response lost | One effect, including across a mock restart | "retries a lost response under one key, with one effect, across a mock restart" in [external-retry.test.ts][external] |
| Destination outage, worker restart | Matching continues, and attempts and due times persist | "keeps matching through an outage, and retries remain durable across a restart" in [external-retry.test.ts][external] |
| Shutdown or partition revoke during work | No offset passes durable work | "finishes its transaction on SIGTERM and commits nothing beyond durable work" and "commits no offset after a rebalance takes its partitions" in [crash-recovery.test.ts][crash] |
| The matching fixture, restart and redelivery | The same three trades, with no others | "commits the fixture: the expected trades, order changes, Inbox and progress" in [matching-recovery.test.ts][matching], "produces the expected trades, and a replay of the whole topic adds none" in [crash-recovery.test.ts][crash], and `npm run demo` |

The handoff also asks for these cases, all covered in [matching.test.ts][engine] unless noted:

| Case | Test |
|---|---|
| Non-crossing orders | "rests orders that do not cross, untouched and open" |
| BUY and SELL symmetry | "is the mirror image for a sell taker: highest bid first, then oldest first" |
| Exact price boundaries | "handles %s", with six cases at, one unit inside and one unit outside the best price |
| A maker spanning takers | "lets one maker span several takers while keeping its place in the queue" |
| Empty level deletion | "removes a price level once it is empty, and can start it again" |
| Maximum units | "stays exact at the largest amounts the database can hold" |
| Equivalent decimals | "treats trailing zeros as the same value, even beyond the scale" in [decimal.test.ts][decimal], and "treats equivalent decimals as one price, and keeps exact units" in [matching-recovery.test.ts][matching] |
| Invalid precision | "rejects precision that it would otherwise have to round" in [decimal.test.ts][decimal] |
| Conservation of quantity, no negative remainder | "agrees on every trade and every book, seed %i", which asserts both after each of 15,000 random orders. The column check on `orders.remaining_units` also refuses a negative value |

[acceptance]: ../tests/integration/acceptance.test.ts
[crash]: ../tests/e2e/crash-recovery.test.ts
[matching]: ../tests/integration/matching-recovery.test.ts
[external]: ../tests/integration/external-retry.test.ts
[engine]: ../tests/unit/matching.test.ts
[decimal]: ../tests/unit/decimal.test.ts

## Limits

Known limitations and deferred features are in the [README](../README.md#known-limitations). The most important one for this design is throughput, because the publisher, matching and external roles each have one owner.
