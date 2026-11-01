# ADR 0003: External notifications

- **Status:** accepted
- **Date:** 2026-10-31

## Context

Notifications use a separate consumer group, `external-notification-v1`, a transaction that writes the Inbox row and a job before the offset advances, one worker, a stable key `external-notify:<eventId>`, a mock destination that keeps its idempotency record and effect atomically in durable storage, and an inspectable terminal state with manual redrive. This ADR records the choices behind that design, and reviews target idempotency and independence from matching.

## Decision: no new dependencies

Every need here is met by the runtime and what is already installed: `fetch` for the client, `node:sqlite` for the mock's storage, and Fastify for the mock's server.

## Decision: one process for intake and delivery

[external.ts](../../src/entrypoints/external.ts) runs the consumer and the worker side by side. Neither loop rejects, because each resumes from durable state until shutdown, so Kafka being down stops no delivery and the destination being down stops no intake.

The role holds its own PostgreSQL session advisory lock, exactly as the publisher and matcher do ([ADR 0002](0002-messaging-and-matching.md#decision-single-ownership)). A standby does nothing until it has the lock. The worker checks the lock's connection before each pass.

## Decision: job states

`external_jobs` has three states, and the constraints tie `next_attempt_at` to `PENDING` and `completed_at` to the two finished states.

| State | Meaning |
|---|---|
| `PENDING` | Waiting for `next_attempt_at` |
| `SUCCEEDED` | The destination answered with a 2xx. Scheduling a job is never enough |
| `FAILED` | Rejected permanently, or out of attempts. Stays until an [operator redrives it](../operations.md#redrive-failed-jobs) |

- **No in-progress state or lease.** Leases and owner tokens are needed only if workers become concurrent. A job whose attempt was cut short is still `PENDING` and due, so the next owner reclaims it with no separate step.
- **A crash does not count as an attempt.** `attempts` counts recorded results only, so an attempt cut short by a crash uses none of the budget.
- **Results are compare-and-set** on the status and attempt count that the attempt read. A worker that froze and lost the lock therefore cannot overwrite its successor's record, and its call used the same key, so it causes no second effect.
- **The payload is the message as it arrived,** kept in a `json` column because `jsonb` would rewrite it. Every attempt therefore sends the same bytes, which is what the same key must mean.
- **Stored errors** lose any NUL, which PostgreSQL refuses in `text`, and are cut to 500 characters, because they contain text from outside this system.

## Decision: HTTP client

Native `fetch`, with `AbortSignal.timeout`, which also bounds reading the response.

- **`connection: close` on every call.** [ADR 0002](0002-messaging-and-matching.md#consequences) recorded that `fetch` stalled when it reused an idle keep-alive connection. A probe of 20 `POST`s a second apart against a bare `node:http` server reproduced it on Node.js 26.7.0, where reused connections took up to 2 s per request and `connection: close` at most 22 ms. On the image's 26.13.0, neither stalled. The supported range includes both, so the header stays. A new connection per attempt costs little at this rate.
- **`redirect: 'manual'`,** because a followed redirect can turn a `POST` into a `GET`. A 3xx is reported as a permanent failure.
- **Only the first 500 characters of an error body are read.** The timeout bounds time but not size, so a destination could otherwise fill memory within one attempt, and a process killed for that would never record the attempt or use up its budget. The rest of the body is cancelled.

## Decision: retry budget

| Setting | Variable | Default | Reason |
|---|---|---|---|
| Timeout per attempt | `EXTERNAL_TIMEOUT_MS` | 5 s | Ample for a small `POST`, and bounds how long one job can hold up the serial worker and shutdown. Shorter than the 8 s shutdown deadline |
| Poll interval | `EXTERNAL_POLL_INTERVAL_MS` | 500 ms | Sets the delay before a new job's first attempt when the worker is idle |
| First retry delay | `EXTERNAL_RETRY_BASE_MS` | 1 s | Recovers quickly from a blip, doubling on each attempt |
| Delay cap | `EXTERNAL_RETRY_CAP_MS` | 5 min | Reached at the 10th attempt. Notices a recovered destination within minutes, without pressing it during an outage |
| Jitter | `EXTERNAL_RETRY_JITTER_PERCENT` | 50 | Removes up to half of each delay at random, so that jobs which failed together spread out |
| Attempts | `EXTERNAL_MAX_ATTEMPTS` | 400 | Lasts about a day, below |

With these values, the 399 delays between 400 attempts add up to between 16.3 and 32.6 hours, 24.5 on average, not counting the attempts themselves. An outage of up to about a day therefore heals with no action. A longer one ends in `FAILED`, where the jobs are visible and can be redriven.

`Retry-After`, in seconds or as an HTTP date, is a minimum, and is honoured up to the cap so that a destination cannot park a job indefinitely.

## Decision: classifying failures

| Result | Class | Reason |
|---|---|---|
| Network error or timeout | Transient | The request may or may not have arrived. Repeating it under the same key cannot repeat the effect |
| 408, 425, 429, 500, 502, 503, 504 | Transient | Documented as temporary |
| 409 | Transient | The usual answer while another request with this key is still running, which is what an earlier attempt that timed out can be |
| Any other status, including 3xx, 400, 401, 403, 404, 422 | Permanent | Repeating the same request cannot help. If the cause is on our side, such as credentials, fix it and redrive |

An unresolvable host or a refused connection is transient too, which means a wrong `EXTERNAL_API_URL` looks like an outage until the budget runs out. The URL's syntax is checked at startup to narrow this.

No result touches the order. The role never writes to `orders`, so infrastructure failure cannot change an order's outcome.

## Decision: malformed events

**Policy:** a message that fails `parseOrderAccepted` is written to `quarantine`, the system's dead-letter table, with its consumer, topic, partition, offset, raw value and error, and only then does the offset advance. If that write fails, the offset holds and the session restarts. This covers invalid JSON, a tombstone, a wrong shape and a future `schemaVersion`.

- **Why not block:** notifications do not depend on each other, so blocking would hold back good notifications and protect nothing.
- **Why a table, not a Kafka topic:** PostgreSQL is already a dependency, and a dead-letter topic would need its own producer and failure handling for the same result.
- **Matching uses the same policy:** the groups keep separate offsets, so each records the message under its own consumer name. See [ADR 0002](0002-messaging-and-matching.md#decision-unreadable-messages).
- **No notification is sent** for a quarantined message.
- **A redelivery adds no second row:** a crash between the insert and the offset commit replays the message, and a unique key on the consumer, position and a hash of the value absorbs it. The hash keeps a different message at a reused position, because Kafka's offsets restart if its data is lost.
- **`raw_value` is close to the original, not exact:** the consumer reads values as UTF-8, so invalid sequences appear as replacement characters. The recorded position finds the original in Kafka while it is retained.

## Decision: the mock destination

[mock-api/](../../mock-api) imports nothing from `src` and shares no storage with the application, because its idempotency must stand on its own.

| Candidate | Why it was or was not chosen |
|---|---|
| `node:sqlite` | **Chosen.** Real transactions, durable with WAL and `synchronous=FULL`, and no new dependency. It loads without an experimental warning on Node.js 26.7.0 and 26.13.0 |
| A second PostgreSQL database | Ties the destination to the server the application depends on, which weakens the independence the mock exists to show |
| `better-sqlite3` | A native module for what the runtime already provides |
| A JSON file | Would need hand-written atomic writes for what SQLite gives by default |

- **Atomicity:** the idempotency record and the effect commit in one transaction, so a crash leaves both or neither.
- **Concurrency:** storage calls are synchronous on one thread, so two requests with the same key cannot interleave, and no in-progress state or 409 is needed.
- **Same key, same bytes:** a body is compared by SHA-256 of its exact bytes, and a different body gets `422 IDEMPOTENCY_KEY_REUSED`. A replay returns the stored response with `idempotent-replayed: true`.
- **Faults live in memory,** because they are instructions to the running mock, not results. `/__control` injects them and reads effects, so production code and tests share one contract.

## Review: target idempotency

The conclusion is that a retry cannot repeat the effect, for as long as the destination remembers the key.

- The key depends only on `eventId`, is stored with the job at handoff, and survives restarts and redrives unchanged. The body is stored too.
- The local Inbox prevents a second job and nothing more. It cannot close the gap between a remote success and the local record of it. The stable key closes that gap, and the tests cover it by losing the response after the remote commit, then restarting the mock.
- **The assumption to check with a real destination:** this mock never forgets a key. A real API may expire keys, and a retry or redrive after that would repeat the effect. Its retention must outlast the retry budget of about a day, plus however long an operator may wait to redrive.

## Review: independence from matching

The conclusion is that the two workflows can delay each other only through what they already share, PostgreSQL and Kafka's availability.

- **Separate progress:** each group has its own offsets, and the Inbox name is fixed per consumer, so neither one's handled events can be mistaken for the other's.
- **No shared writes:** the role reads and writes only `consumer_inbox` under its own name, `external_jobs` and its own rows in `quarantine`. Matching reads none of those, and writes only its own `quarantine` rows, which never conflict with this role's.
- **Proven in both directions:** matching continues through a destination outage, and a notification is delivered for an event that matching is blocked behind.
- **Shared code:** both consumers run on the same `consumer-session.ts`, which holds no state between them.
- **Acceptance, not matching:** a notification can arrive before the order is matched, and says nothing about the outcome.

## Consequences

- **Serial delivery:** if the destination stops answering altogether, each due job costs the full timeout, so 100 due jobs take over 8 minutes per pass and there is no circuit breaker. New jobs wait behind that. This affects only notifications.
- **No order between notifications:** a retried job can arrive after a later one. The body carries `acceptanceSequence` for a destination that needs order.
- **First start reads back:** a new group starts from the earliest retained event, so deploying this role notifies every accepted order still in the topic, up to 30 days. Older orders get no notification.
- **Trouble is reported, not alerted:** `FAILED` jobs and quarantine rows are logged as errors when they happen, and counted by `GET /ops/backlog` ([ADR 0004](0004-shutdown-health-and-visibility.md#decision-the-backlog-report)). Nothing alerts on them.
- **Tables grow:** jobs and quarantine rows are never deleted, like the other durable records.
