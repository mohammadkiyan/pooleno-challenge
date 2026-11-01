# Operations

How to watch the running stack, cause failures by hand, repair what needs an operator, and configure it. All of this assumes the stack from `./review.sh` or `docker compose up --build -d --wait`.

**Contents:** [See it fail](#see-it-fail) · [Health](#health) · [Backlog](#backlog) · [Logs](#logs) · [Repairs](#repairs) · [Stopping](#stopping) · [Run on the host](#run-on-the-host) · [Configuration](#configuration)

## See it fail

`./review.sh` already kills processes for you. These three walkthroughs show the other failures.

### Acceptance outlasts a Kafka outage

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

### A lost response causes no second effect

The mock can commit its effect and then drop the connection, which is the gap that a local Inbox cannot close. This queues that fault, followed by a `503` whose `Retry-After` holds the job long enough to restart the mock in between:

```bash
curl -i http://127.0.0.1:18080/__control/faults \
  -H 'content-type: application/json' \
  -d '{"mode":"lost-response"}'
curl -i http://127.0.0.1:18080/__control/faults \
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
curl -s http://127.0.0.1:18080/__control/effects | grep -o 'external-notify:[0-9a-f-]*' | sort | uniq -c
```

The job is `SUCCEEDED` after three attempts: the lost response, the `503`, and the replay. The log shows `applied` and then, after the restart, `replayed` for one key, and the last command counts every key once.

### A failed job is redriven

A permanent rejection fails the job at once and leaves it for an operator. Replace the event ID with the one the query prints:

```bash
curl -i http://127.0.0.1:18080/__control/faults \
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

The job is `SUCCEEDED` moments later, under the same key.

## Health

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

## Backlog

`GET /ops/backlog` reports counts and ages only. It answers even with Kafka down, and then reports `"kafka": {"available": false}`. This is its answer 10 s into the outage above:

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
    "symbols": [{ "symbol": "ETH-USD", "lag": 1, "oldestUnappliedAgeMs": 10053 }],
    "quarantined": 0
  },
  "external": { "pending": 0, "retrying": 0, "failed": 0, "oldestPendingAgeMs": null, "quarantined": 0 },
  "kafka": { "available": false }
}
```

With Kafka up, `kafka` is `{"available": true, "groups": [...]}`, with the lag of each group and partition.

| Question | Field |
|---|---|
| How much is waiting to reach Kafka, and for how long? | `outbox.pending`, `outbox.oldestPendingAgeMs` |
| Is sending failing? | `outbox.retrying`, `outbox.totalAttempts` |
| Which symbols are held back? | `outbox.blockedSymbols` |
| How many accepted orders are unmatched, and for how long? | `matching.lag`, `matching.symbols` |
| Are notifications behind or stuck? | `external.pending`, `external.retrying`, `external.oldestPendingAgeMs`, `external.failed` |
| Did a message get set aside? | `matching.quarantined`, `external.quarantined` |
| How far is each group behind the topic? | `kafka.groups` |

Matching lag and Kafka lag answer different questions. Matching lag counts every accepted order not yet applied, including those still in the Outbox. Kafka lag counts only what reached the topic, so during an outage it can read zero while matching lag grows.

`/health/*` and `/ops/*` have no authentication, so an ingress must not route them. No alerting exists: this report and the error logs are the signal, and a deployment connects them to its own alerting.

## Logs

Each role writes JSON lines that carry `role`, `requestId`, `orderId`, `eventId`, `symbol` and `acceptanceSequence`. Raw idempotency keys and connection details stay out. This follows the most recently matched order across all four roles:

```bash
REQUEST_ID=$(docker compose logs --no-log-prefix matching | grep -o '"requestId":"[^"]*"' | tail -1)
docker compose logs --no-log-prefix api publisher matching external | grep -F "$REQUEST_ID"
```

## Repairs

A sequence gap blocks matching on purpose, and the error log names the sequence involved. Quarantined messages need no repair to keep matching moving.

### Inspect failed jobs and quarantined messages

```bash
docker compose exec postgres psql -U pooleno -d pooleno -c "
  SELECT event_id, attempts, completed_at, last_error
    FROM external_jobs WHERE status = 'FAILED' ORDER BY completed_at;"
docker compose exec postgres psql -U pooleno -d pooleno -c "
  SELECT consumer, topic, kafka_partition, kafka_offset,
         encode(raw_value, 'escape') AS raw_value, error
    FROM quarantine ORDER BY id;"
```

### Redrive failed jobs

This returns them to `PENDING` with a new budget of attempts, the same event, key and payload, and `last_error` kept until the next result. It refuses a job that has not failed, and exits with status 1 if any ID did nothing.

```bash
docker compose run --rm --no-deps external node dist/entrypoints/redrive-external.js <eventId>
```

### Read quarantined messages again

Do this once the role can accept them, such as after adding a new `schemaVersion`. The Inbox makes every event already handled a no-op, and any message that still fails is quarantined again.

```bash
docker compose stop external
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka:19092 --group external-notification-v1 \
  --topic orders.accepted:<partition> --reset-offsets --to-offset <offset> --execute
docker compose start external
```

### Lost or missing events

The Outbox still holds every accepted event, so the publisher can send those again. Retention can remove an older event while newer ones remain, and the client then moves an expired position to the oldest record it still has. Matching blocks there on the sequence guard, ahead of anything republished, so the group must move to the end of the topic first. Everything unapplied is then sent again behind that position, in order, and the publisher must be stopped throughout so that no new event lands ahead of its predecessors.

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

If Kafka's data is gone altogether, run `docker compose run --rm kafka-init` before the reset, which needs the topic to exist. End-to-end tests cover both repairs through the admin API, and these commands were also run against the Compose stack, including a run with Kafka's volume deleted.

## Stopping

`docker compose stop` sends `SIGTERM`. A role drains and exits with status 0, normally within a second. Past `SHUTDOWN_TIMEOUT_MS` it exits with status 1, which is safe because durable records drive replay. A second signal ends it at once.

A worker role also stops by itself when it fails or loses its lock, within the same limit. It logs `shutdown deadline passed; exiting` if it had to be forced. Its supervisor restarts it, and it then competes for the lock again.

`./review.sh down` removes the stack and all its data. `docker compose down` removes the containers and keeps the data.

After a failed port bind, run `docker compose down` and then `up`, because a plain rerun of `up` starts the leftover container with no network.

## Run on the host

You need Node.js 26 or newer. Each process runs in its own terminal, after stopping its container and preparing the dependencies:

```bash
npm ci
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

## Configuration

Compose sets its own values. Running on the host reads `.env`, copied from [.env.example](../.env.example).

### Published ports

Every published port is bound to loopback. If one is taken, set its variable for both `./review.sh` and `docker compose`, for example `HOST_PORT_API=9080 ./review.sh`.

| Variable | Service | Default |
|---|---|---|
| `HOST_PORT_API` | API | `8080` |
| `HOST_PORT_PUBLISHER` | Publisher health | `8081` |
| `HOST_PORT_MATCHING` | Matching health | `8082` |
| `HOST_PORT_EXTERNAL` | External health | `8083` |
| `HOST_PORT_POSTGRES` | PostgreSQL | `15432` |
| `HOST_PORT_KAFKA` | Kafka | `29092` |
| `HOST_PORT_MOCK_API` | Mock destination | `18080` |

### Process settings

| Variable | Used by | Description | Default |
|---|---|---|---|
| `DATABASE_URL` | API, publisher, matching, external, `migrate`, `external:redrive` | PostgreSQL connection string | Required |
| `LOG_LEVEL` | All | pino log level | `info` |
| `SHUTDOWN_TIMEOUT_MS` | API, publisher, matching, external | Longest wait for work in flight after a signal, a role failure or lost ownership. Keep it below the supervisor's grace period | `8000` |
| `HEALTH_HOST` | Publisher, matching, external | Listen address of the health server | `127.0.0.1` |
| `HEALTH_PORT` | Publisher, matching, external | Health port; `0` picks a free one | `8081`, `8082`, `8083` |
| `HOST` | API | Listen address | `127.0.0.1` |
| `PORT` | API | Listen port; `0` picks a free one | `8080` |
| `ACCEPTANCE_LOCK_TIMEOUT_MS` | API | Longest wait for a lock during acceptance, at least 1 | `1000` |
| `KAFKA_BROKERS` | API (only to report lag), publisher, matching, external, `kafka:init` | Comma-separated `host:port` list | `127.0.0.1:29092` |
| `KAFKA_TOPIC` | API (only to report lag), publisher, matching, external, `kafka:init` | Topic for accepted orders | `orders.accepted` |
| `OUTBOX_POLL_INTERVAL_MS` | Publisher | Pause when nothing is due | `200` |
| `OUTBOX_RETRY_BASE_MS` | Publisher | Delay after the first failed send, doubling on each attempt | `500` |
| `OUTBOX_RETRY_CAP_MS` | Publisher | Longest delay between attempts | `30000` |
| `MATCHING_GROUP_ID` | API (only to report lag), matching | Kafka consumer group | `matching-v1` |
| `MATCHING_SESSION_TIMEOUT_MS` | Matching | Kafka session timeout, at least 6000 | `10000` |
| `MATCHING_RESTART_DELAY_MS` | Matching | Pause before restoring after a failure | `1000` |
| `MATCHING_STATEMENT_TIMEOUT_MS` | Matching | Longest one database statement may run, at least 1. An order whose fills take longer holds its partition until this is raised | `60000` |
| `MATCHING_LANE_CAPACITY` | Matching | Messages buffered per partition, at least 1. A full lane holds back every partition | `10000` |
| `EXTERNAL_GROUP_ID` | API (only to report lag), external | Kafka consumer group | `external-notification-v1` |
| `EXTERNAL_SESSION_TIMEOUT_MS` | External | Kafka session timeout, at least 6000 | `10000` |
| `EXTERNAL_RESTART_DELAY_MS` | External | Pause after a failed session or delivery pass | `1000` |
| `EXTERNAL_API_URL` | External | Where notifications go, as an absolute `http` or `https` URL | `http://127.0.0.1:18080/v1/notifications` |
| `EXTERNAL_TIMEOUT_MS` | External | Limit on one attempt | `5000` |
| `EXTERNAL_POLL_INTERVAL_MS` | External | Pause when no job is due | `500` |
| `EXTERNAL_RETRY_BASE_MS` | External | Delay after the first failed attempt, doubling on each attempt | `1000` |
| `EXTERNAL_RETRY_CAP_MS` | External | Longest delay between attempts, and the limit on `Retry-After` | `300000` |
| `EXTERNAL_RETRY_JITTER_PERCENT` | External | Largest share of a delay removed at random, 0 to 100 | `50` |
| `EXTERNAL_MAX_ATTEMPTS` | External | Attempts before a job fails, which lasts about a day by default | `400` |
| `MOCK_API_DATABASE` | Mock API | SQLite file that holds its effects | Required |
| `MOCK_API_HOST` | Mock API | Listen address | `127.0.0.1` |
| `MOCK_API_PORT` | Mock API | Listen port | `18080` |
