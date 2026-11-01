# Architecture

This document covers how an order moves through the system, what matching costs, what happens when a process dies, and how the design scales and runs under Kubernetes. Smaller choices and their evidence are in the [ADRs](adr/).

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
                    PostgreSQL: trades, orders,            destination API (mocked)
                         progress, Inbox
```

One codebase has separate modules and a separate entrypoint for each of the four roles. PostgreSQL is the source of truth, Kafka carries work between roles, and the in-memory book is a cache of committed rows. Any role can be killed at any moment, because each one resumes from durable records.

`202 Accepted` means the order, its idempotency receipt, its symbol sequence and its Outbox event are committed together. It doesn't mean the order has matched, and clients poll `GET /v1/orders/{orderId}` for that. Validation happens before the commit, so an accepted order is never rejected later, and infrastructure failure can only delay it. The assignment doesn't require asynchronous matching or `202`. They are what lets a crash at any point lose nothing a client was told about.

## Matching: data structures and complexity

Makers trade best price first, oldest first within a price, at the maker's price, and either side can partly execute. The engine is pure code with no dependency on Fastify, PostgreSQL or Kafka.

| Structure | Implementation | Why |
|---|---|---|
| Price levels on one side | `sorted-btree` B+ tree keyed by `bigint` price, best price first | Ordered walk, plus `O(log P)` insert and delete |
| Orders at one price | Singly linked list in [price-level.ts](../src/modules/matching/price-level.ts) | True `O(1)` append and pop. `Array.shift()` would be `O(n)` |
| Books | `Map` from symbol to `OrderBook` | One independent book per symbol |
| Amounts | `bigint` integer units, `numeric` in PostgreSQL | Exact. No value passes through a float |

P is the number of price levels on a side, K the makers touched by one order, L the levels it empties, and N the resting orders.

| Operation | Cost | Where the cost comes from |
|---|---|---|
| Add an order at an existing price | `O(log P)` | Tree lookup, then `O(1)` append |
| Add an order at a new price | `O(log P)` | Tree lookup and insert |
| Pop the oldest order of a level | `O(1)` | Linked list |
| Compute a match, `matchOrder` | `O(log P + K + L)` | Starts one ordered walk, and changes nothing |
| Apply a match, `applyMatch` | `O(log P + K + L log P)` | Finds the best level once, then again only after emptying one, and deletes each emptied level |
| Restore all books | `O(N log P)` plus reading N rows | One `add` per resting order, from one query in acceptance order |
| Memory | `O(N + P)` | One node per order, one tree entry per level |

The database work grows with K too. One matching transaction writes the taker, K makers, K trades, one progress row and one Inbox row, so the database is the likely limit rather than the tree. That's an inference, because the code wasn't profiled. A 5M-order book restored in about 6 s in one local run.

## Kafka: publishing, offsets and guarantees

**Publishing.** Acceptance writes the event to the Outbox, and one publisher sends it, keyed by symbol, through an idempotent producer. Each pass sends only the earliest unpublished event of each symbol, so a failed event holds back its own symbol and no other. `published_at` is set after Kafka acknowledges.

**Offsets.** Each consumer group keeps its own position, so matching being blocked doesn't stop notifications, and the reverse. Auto-commit is off. A consumer commits `offset + 1` for each message, only after that message's work is durable, so a message can be handled twice but never skipped.

- **After a crash,** the group waits for the dead member's session to expire, up to 10 s by default, then redelivers from the last committed offset.
- **After a rebalance,** a role commits nothing further for the partitions it lost, because a late commit could pass work its successor hasn't done.

**Guarantee.** Delivery is at least once, and every effect is idempotent. The complete workflow across HTTP, PostgreSQL, Kafka and the destination is **not** exactly once. Each layer removes the duplicates it can see:

| Layer | Mechanism | What it prevents |
|---|---|---|
| Client to API | `Idempotency-Key`, plus a fingerprint of the request's meaning | A second order from a retry |
| Publisher to Kafka | Idempotent producer, and the same stored payload on any resend | A duplicate from a client retry, or a different event for one order |
| Kafka to matching | Inbox row per consumer and event, in the matching transaction | A second trade |
| Old or unknown events | Progress row lock, `acceptanceSequence = last_applied_seq + 1`, a check that the book matches the database, and compare-and-set on orders | A stale or out-of-order event changing state |
| Kafka to external | Inbox row and unique job per event | A second job |
| Worker to destination | Stable key `external-notify:<eventId>`, and the same bytes on every attempt | A second effect, for as long as the destination remembers the key |

The local Inbox can't close the gap between a remote success and its local record. Only the destination's own idempotency closes it, and a test loses the response after the remote commit to prove that.

**Failures and retries.**

| Failure | Behavior | Limit |
|---|---|---|
| Kafka send fails | The event stays in the Outbox, and its symbol waits. The delay doubles from 500 ms to 30 s | None. It lasts as long as the outage |
| Matching event fails | Only its partition is held. The role restores that book, waits 1 s and retries without committing | None. It retries until the failure clears or an operator acts |
| Kafka session fails at a consumer | Every book is discarded. The role waits 1 s, restores and rejoins | None |
| Sequence gap at matching | Holds its partition with an error, because later orders depend on the missing one | Needs the [lost-event procedure](operations.md#lost-or-missing-events) |
| Malformed message at a consumer | Saved to the `quarantine` dead-letter table, then skipped. If it was a real order, the sequence guard stops its symbol at the next event | Inspected under [Repairs](operations.md#inspect-failed-jobs-and-quarantined-messages) |
| Destination fails transiently | The job retries with jitter and honours `Retry-After`. The delay doubles from 1 s to 5 min | 400 attempts, about a day, then `FAILED` |
| Destination rejects permanently | The job is `FAILED` at once | Needs a [redrive](operations.md#redrive-failed-jobs) under the same key |

## Preventing event loss after a crash

Writing to the database and then to Kafka would lose the event if the process died in between. Instead, the event is a row in the same transaction as the order, the Transactional Outbox pattern. If the order is committed, so is its event. `./review.sh` shows this directly: it kills the publisher, gets a `202` for an order, kills every process including PostgreSQL and Kafka, and the order still matches and notifies after the restart.

Each step commits durably before the next one starts, and no transaction spans PostgreSQL and Kafka.

| Step | What commits together | A crash before it | A crash after it |
|---|---|---|---|
| Acceptance | Order, receipt, counter, Outbox event | Nothing is kept, and the client's retry is accepted with no sequence gap | The retry returns the original `orderId` |
| Publish | Kafka acknowledges, then `published_at` is set, separately | Not sent, or sent and not marked. Either way, the event is sent again | Done |
| Matching | Inbox row, taker, makers, trades, progress | Nothing is kept, and the event is redelivered | The offset isn't committed yet, so the event returns and the Inbox makes it a no-op |
| Notification handoff | Inbox row and job | Nothing is kept, and the event is redelivered | The redelivery creates no second job |
| Delivery result | Job status and attempts, compare-and-set | The remote effect may exist with no local record. The retry uses the same key, and the destination replays | Done |

The book changes only after the matching commit, and the offset only after the book. Whenever the result is uncertain, such as a lost `COMMIT` reply, the role discards the book and rebuilds it from a repeatable-read snapshot of resting orders in acceptance order, which reproduces FIFO and every remainder. A test checks that a restored book equals the live one after a random workload. [Testing](testing.md#failure-tables) lists each failure with its test.

## Scaling consumers and preserving ordering

**Order within a symbol is the invariant.** Acceptance assigns each symbol a gap-free sequence under a row lock. The publisher sends only the earliest unpublished event of each symbol. The Kafka key is the symbol, so all of its events share a partition. Matching rejects any event that isn't next in sequence.

**Partitions bound consumer scale.** A partition has at most one active consumer in a group, so the topic's 3 partitions allow at most 3 useful consumers per group. One symbol is never processed in parallel, however many consumers run. Adding partitions later would move symbols between them and break their order across the change, so the count is fixed, and a real deployment should start with more.

| Role | Today | How it would scale |
|---|---|---|
| API | Stateless, and row locks serialize each symbol's counter, so it can run several replicas | Add replicas |
| Publisher | One owner | Partition the Outbox by symbol, with one sender per partition |
| Matching | One owner, with one sequential lane per Kafka partition, so a slow or failing symbol holds only its partition | One matcher per partition, loading only assigned books. It needs fencing first, for example an epoch per partition that a new owner advances before restoring and each transaction checks |
| External | One owner, with a serial worker | Competing workers that claim jobs with `FOR UPDATE SKIP LOCKED` and a lease. The stable key already makes a duplicate attempt safe. Not implemented |

A consumer group alone can't prove ownership, because a member that has lost its partition can still be writing to PostgreSQL while its successor starts. So each single-owner role holds a PostgreSQL session advisory lock, and a standby waits for it. Today's checks limit the damage of a brief overlap, but they aren't fencing. Two active matchers would keep failing each other rather than share work. A faster single symbol would need a faster matcher, not more of them.

In one local run on a 12-core machine, the API accepted about 460 orders per second, the publisher sent about 230 events per second, matching kept up at about 215 orders per second that didn't trade, and the worker delivered about 135 jobs per second. These are floors on one machine, not capacity claims.

## Kubernetes: shutdown and health

This project has no manifests. It is built to run under Kubernetes as follows.

**Graceful shutdown.** On `SIGTERM` or `SIGINT`, a role reports not ready, stops taking new work, finishes what is in flight, commits offsets only for completed work, leaves its group, releases its lock and exits with status 0. `SHUTDOWN_TIMEOUT_MS`, 8 s by default, bounds all of this. Past the deadline it exits with status 1, and a second signal ends it at once. Both are safe, because a forced exit is the same as a crash and durable records drive replay. Measured with `docker compose stop`: 0.3 s with status 0 with healthy dependencies, and 8.3 s with status 1 for a consumer whose broker hung.

**Health checks.** Liveness reports that the process runs and never looks at a dependency, so an outage doesn't cause a restart loop. Readiness reports whether the role can do its work now.

| Role | Port | Ready when |
|---|---|---|
| API | 8080 | The pool can run a query. Kafka isn't needed to accept orders |
| Publisher | 8081 | It owns the role, and Kafka answers |
| Matching | 8082 | It owns the role, its books are restored, it has partitions, and none is held on a failing event |
| External | 8083 | It owns the role, and it has partitions. The destination being down doesn't count |

**Mapping to Kubernetes:**

- Set `terminationGracePeriodSeconds` above `SHUTDOWN_TIMEOUT_MS`, and add a short `preStop` delay on the API so endpoints update before it stops listening.
- Map probes to `/health/live` and `/health/ready`, and set `HOST` and `HEALTH_HOST` to `0.0.0.0`.
- Run the API as a Deployment with several replicas. Run one replica each of the publisher, matching and external, and let Kubernetes restart them. They have no client traffic, so they need no Service. A standby is correctly not ready, so a two-replica Deployment reports one unavailable replica forever.
- Run `migrate` and `kafka-init` as Jobs before the roles start.
- An ingress must not route `/health/*` or `/ops/*`, because they have no authentication.
- Use more Kafka brokers, with matching replication and `min.insync.replicas`. The local stack has one broker with a replication factor of 1.

`GET /ops/backlog` reports the Outbox, matching lag, notifications and Kafka lag, and answers even with Kafka down. Logs carry `requestId`, `orderId`, `eventId` and `symbol` across all four roles. See [Operations](operations.md).

## Key decisions and tradeoffs

| Decision | Why | What it costs |
|---|---|---|
| Commit first, match asynchronously | A confirmed order survives any crash, and acceptance outlives Kafka | Clients poll for the result |
| Outbox, not a dual write or CDC | Atomic with the order, with no extra infrastructure | A polling publisher and one more table |
| One active owner per role, by advisory lock | Simple correct ordering, and a standby can take over | Throughput is one process per role. This is the main limit of this version |
| In-memory book as a cache of the database | Fast matching, and any doubt is resolved by rebuilding | Restore time at start, and a brief stop on failure |
| One commit per message | After a crash, at most one message per partition has been handled and not committed, and the code stays easy to reason about | A round trip per message, accepted at this scale. Batching is the first optimization if it's ever the bottleneck |
| One event per symbol per publisher pass | Ordering is correct by construction, and a failed head holds back only its symbol | Publisher throughput of about 230 events per second |
| Notifications as durable jobs in their own group | A destination outage can't stall a partition or matching. Attempts and due times survive restarts | A jobs table and a worker, and notifications aren't ordered among themselves |
| A dedicated lock connection beside the pool, with guarded writes | The pool can reconnect freely without losing ownership, and each write is still checked | A frozen process could overlap its successor for a moment, and the guards then make it fail. This isn't full fencing |
| Backlog limited only by PostgreSQL's disk | Refusing orders would give up acceptance outliving Kafka. At about 1.4 KiB per order, a free gibibyte holds roughly 750,000 | A deployment must alert on `outbox.pending` and disk space |
| Exact `bigint` units, with excess precision rejected | Money is never rounded | Market scales can't change once a market has orders |

## Limits

- **Throughput:** one publisher, one matcher and one serial notification worker. A standby takes over but doesn't share the load.
- **Blocked events:** a blocked event stops the symbols on its partition. A lane that reaches `MATCHING_LANE_CAPACITY` holds back every partition, because the client can only pause its whole stream.
- **Notifications:** they aren't ordered among themselves, can arrive before the order is matched, and rely on a real destination remembering keys for longer than the retry budget of about a day. First start notifies every order still retained in the topic.
- **Retention:** Kafka keeps events for 30 days. Beyond that, or after losing Kafka's data, an operator [republishes from the Outbox](operations.md#lost-or-missing-events).
- **Growth:** idempotency keys, jobs and quarantine rows are never deleted automatically.
- **Local setup:** one Kafka broker, one PostgreSQL with passwordless loopback access, and no authentication on any endpoint. `mock-api` is for local use only.
- **No alerting or metrics endpoint:** the backlog report and error logs are the signal.
- **Deferred features:** market orders, cancel and amend, wallets and settlement, authentication, and multiple owners of one order book.
