# ADR 0004: Shutdown, health and visibility

- **Status:** accepted
- **Date:** 2026-10-31

## Context

Running under a supervisor such as Kubernetes needs readiness that goes false first on `SIGTERM`, bounded draining, a shutdown deadline inside the termination grace period, liveness that ignores remote dependencies, readiness by role, and visibility of the Outbox, matching, external jobs and Kafka lag. This ADR records the choices behind those, and the measurements behind each one. The resulting behavior is described in the [architecture document](../architecture.md).

## Decision: no new dependencies

The health server uses `node:http`, the backlog report uses SQL, and Kafka lag uses the installed client's admin API. `@fastify/under-pressure` and a metrics library stay out until there is evidence for them.

## Decision: the shutdown deadline

| Setting | Value | Reason |
|---|---|---|
| `SHUTDOWN_TIMEOUT_MS` | 8 s | Longer than the 5 s limit on one external attempt, so a normal attempt can finish |
| Compose `stop_grace_period` | 10 s | Longer than the deadline, so a role always stops by itself before `SIGKILL` |

- **Past the deadline, the process exits with status 1.** It logs an error first, so a forced exit can be told apart from a clean one.
- **A second signal ends the process at once,** which lets an operator stop a stuck shutdown without waiting.
- **A forced exit is safe,** because it is the same as a crash: uncommitted work rolls back, and no offset has passed durable work. A test forces one and proves that the work is replayed once.
- **The timer doesn't keep a process alive.** A shutdown that finishes early exits immediately.

### Closing a consumer doesn't interrupt a join

A role told to stop while joining a group can't leave early. Closing the consumer during a join in flight took 4.9 s in a test, waiting out the dead member's session. The likely cause is that the leave request queues behind the parked `JoinGroup` on the same connection, because a broker answers one request per connection at a time. That cause is an inference from the protocol, and wasn't traced in the broker.

We don't add code to work around this, because it wouldn't help. The deadline bounds the case, and it can occur only just after a crash, not in a normal rolling restart.

### A hung broker reaches the deadline

With Kafka paused, so that connections stay open and nothing answers, matching and external each took 8.3 s to stop and exited with status 1. The publisher stopped in 0.3 s with status 0 when it was idle, and can take up to the deadline when parked in a send, because the client's request timeout is 30 s. We accept this, because replay makes it safe and this failure is rare.

## Decision: health

| Decision | Reason |
|---|---|
| Each ownership role has its own small HTTP server on its own port | They have no API to add routes to, and `node:http` covers two fixed paths |
| The health server starts before waiting for the lock | A standby must be able to answer that it's alive and not ready |
| A role that can't bind its port exits with status 1 | A process running with no way to observe it is worse than one that stops visibly. A second instance on one host needs its own `HEALTH_PORT` |
| Readiness checks are limited to 2 s | Shorter than the 3 s that Compose gives a check, so a slow dependency reports a reason rather than a failed probe |
| Answers carry fixed reasons, never error text | The port has no authentication, and error text can contain connection details |
| The API is ready without Kafka | Acceptance doesn't need it. Reporting otherwise would take a working API out of service |
| The API checks admission capacity through its pool | A pool that can't lend a connection within its 2 s timeout is out of capacity for new requests too, so one query covers both |
| Kafka being down makes the other roles not ready but alive | They can't do their work, but a restart wouldn't help. The publisher's check asks the broker directly, not the client's cached metadata |
| The destination being down affects neither check | It is a delivery condition, and says nothing about whether the role works |
| Compose counts failures only after a 60 s `start_period`, then flips after 3 | The earlier `retries: 30` kept a role showing `healthy` for a minute into an outage. With this setting, an outage showed as `unhealthy` within 10 s, and a slow start isn't called unhealthy. Compose restarts nothing for it |

## Decision: the backlog limit

Acceptance during a Kafka outage needs a stated limit. Refusing orders because of backlog is one option, and it needs evidence before it earns its cost, so this is the decision:

**The limit is the disk available to PostgreSQL, and nothing refuses orders because of backlog.** Readiness covers capacity to admit a request, through the pool, and not the size of the backlog. Refusing orders during an outage would give up the property that acceptance outlives Kafka, in exchange for protecting a resource that a simple alert can watch.

- **Cost per order:** 5,000 accepted orders with the other roles stopped grew the database by 1.4 KiB per order, including indexes and excluding WAL. A free gibibyte holds roughly 750,000 orders.
- **How to watch it:** `GET /ops/backlog` reports `outbox.pending` and `outbox.oldestPendingAgeMs`.
- **What to do:** restore Kafka. Publishing drains in sequence, at about 230 events per second in the same local run.
- **When to revisit:** if a deployment's acceptance rate could fill its disk within the time it takes to respond to an alert, add a limit on `outbox.pending` at admission.

## Decision: the backlog report

- **On the API, at `GET /ops/backlog`:** an operator already has this port, and the role has a pool. A separate role would add a process for one query.
- **One snapshot:** the database figures come from one read-only transaction, so they agree with each other.
- **Counts and ages only:** the report returns no payloads and no `last_error` text, which can contain remote content.
- **Cheap to read:** each query uses a partial index or a small table, and it took 32 ms with 5,000 events pending.
- **Kafka lag opens a short-lived admin client per call.** This is meant for an operator or a slow scrape, not a tight loop.

### Failure is an answer

Kafka being down is when this report matters most, so lag failure returns `{"available": false}` with `200`. The first version set the client's `timeout` option and hung past 8 s when Kafka was paused, because the request limit is a separate option, `requestTimeout`, which defaults to 30 s. Setting it brought the answer to 2 s, and a test now uses a server that accepts and never answers.

### Limits of the figures

- **Lag for a group with no commit counts from offset 0,** which over-counts once retention has removed old messages.
- **Matching lag is the symbol's last sequence minus its last applied one,** so it counts orders still in the Outbox as well as those in Kafka.
- **No alerting exists.** The report and the error logs are the signal, and a deployment connects them to its own alerting.

## Decision: log fields

Every step logs `role`, `requestId`, `orderId`, `eventId`, `symbol` and `acceptanceSequence`. The event already carried the API's request ID as `correlationId`, so roles reuse it as `requestId`. The worker reads these values from the job's stored payload. A test follows one request across all four roles, and checks that its idempotency key appears in no log.

## Decision: what the crash tests prove

The tests assert durable results (orders, trades, external effects and FIFO) after a real process dies at a known point. Three boundaries need a specific method:

| Boundary | How it's reached | Why it needs this method |
|---|---|---|
| Matching transaction open, no commit | Hold the progress row, then kill | `COMMIT` is never sent, so the transaction rolls back |
| Handoff transaction open, no commit | Hold the Inbox row, then kill | The same reasoning |
| Remote effect exists, local record doesn't | A preload that kills the process before it sends the result | Parking the result's `UPDATE` on a lock wouldn't work. It is a single statement, so PostgreSQL still commits it after the client dies |

## Consequences

- **Restart after a crash takes up to one Kafka session timeout,** because the group waits for the dead member. This is 10 s by default, and the whole stack came back in 24 s after a kill of every container.
- **A second instance on one host needs its own `HEALTH_PORT`,** and `docker compose up --scale` conflicts with the published health ports. A standby is therefore a host or test setup, not the default Compose stack.
- **`/health/*` and `/ops/*` have no authentication,** like the rest of this demo. An ingress must not route them.
- **Every published port is below Linux's ephemeral range,** 32768 to 60999, and bound to loopback. Inside that range, a closed client connection that used the port as its source holds it in `TIME_WAIT` for up to 60 s, and Docker's bind then fails with `address already in use` while no process holds the port. This happened in practice right after a test run. `HOST_PORT_*` variables move a port that is taken on a reviewer's machine.
- **After a failed bind, rerunning `up` isn't safe.** Docker starts the leftover container with no network. `mock-api` then reports `healthy` and `up` exits 0, because its check uses loopback, while nothing can reach it. `docker compose down` followed by `up` recovers and keeps the data.
