# Testing

**Contents:** [Run the tests](#run-the-tests) · [Results](#results) · [How failures are injected](#how-failures-are-injected) · [Failure tables](#failure-tables) · [Required results](#required-results)

## Run the tests

`./review.sh --full` runs all of them in a container and needs only Docker. To run them on the host, with Node.js 26 or newer:

```bash
npm ci
docker compose up -d --wait postgres kafka
npm run check
```

| Command | What it runs | Needs |
|---|---|---|
| `npm run check` | Typecheck, linter and all tests | PostgreSQL and Kafka |
| `npm run test:unit` | Matching engine, decimals, configuration and other pure logic | Nothing |
| `npm run test:integration` | Integration tests and end-to-end tests with real processes | PostgreSQL and Kafka |
| `npm run demo` | The walkthrough, against a running stack with an empty `ETH-USD` book | The whole stack |

Tests that need them use real PostgreSQL, recreating a separate `pooleno_test` database from the migrations on each run, and real Kafka, with a new topic for each test. Set `TEST_DATABASE_URL` or `TEST_KAFKA_BROKERS` to use other servers.

The engine is also compared with a simple sorted-list reference matcher over 15,000 seeded random orders.

## Results

From one run of `./review.sh --full` on 2026-11-01, on Linux, with Node.js 26.13, PostgreSQL 18 and Kafka 4.2.2 in Docker, starting from a copy of the source with no `node_modules` or `dist`:

| Step | Result | Time |
|---|---|---|
| Typecheck | Clean | 4 s |
| Linter | Clean | 7 s |
| Unit tests | 211 passed in 10 files | 11 s |
| Integration and e2e tests | 221 passed in 15 files | 4 m |
| Walkthrough | 14 of 14 checks passed | Seconds |
| Crash proof, including the `SIGKILL`s and the restart | 9 of 9 checks passed, and the repeated walkthrough changed nothing | About 30 s, 26 s of it the restart |
| The whole command | Passed | 5 m 23 s with the images already built |

`./review.sh` without `--full` runs the same walkthrough and crash proof and skips the first four steps. It took 1 m 04 s, including 16 s to build the scenario viewer's image for the first time.

`review.sh` is written for bash 3.2, which macOS ships. Its flow and failure paths also passed under bash 3.2.57 with a stand-in for Docker, but the whole command has only run on Linux. It has not run on macOS, or on Windows through Git Bash or WSL.

## How failures are injected

Production code has no test hooks. Each failure is injected from outside, and you can use the same methods by hand:

- **Kill a real process:** `docker compose kill -s SIGKILL <service>`.
- **Stop or hang a dependency:** `docker compose stop kafka`, or `docker compose pause kafka` for a broker that accepts connections and never answers.
- **Make the destination misbehave:** `POST /__control/faults` on the mock, as in [the lost-response walkthrough](operations.md#a-lost-response-causes-no-second-effect).
- **Die at an exact point:** run a role on the host with a preload from `tests/`, which kills the process just before a chosen call. After `cp .env.example .env` and `docker compose stop matching`, the following dies as it commits the offset that you name: `CRASH_BEFORE_COMMITTING_OFFSET=<offset> node --env-file=.env --import tsx --import ./tests/e2e/crash-before-offset-commit.preload.ts src/entrypoints/matching.ts`. After a walkthrough, name 1 more than the largest `endOffset` in `GET /ops/backlog`, then send an `ETH-USD` order. The process exits with status 137, and `docker compose start matching` replays the event without applying it twice.
- **Park a transaction:** hold a row in `psql` with `SELECT ... FOR UPDATE`, as the tests do with the progress and Inbox rows, then kill or signal the role.
- **Fail a write:** add a database trigger that raises on the table.

## Failure tables

Each table lists a failure, how it is produced, and what the test proves.

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
| Malformed message | The test produces one directly to the topic | It is quarantined, and the orders behind it on its partition still trade |
| Value PostgreSQL would refuse | A copy of an event with a NUL in its symbol, between two real orders | It is quarantined before any write, the orders trade, and the partition isn't held |
| Quarantine write fails | A database trigger raises on the insert | The partition holds until the message is recorded |
| Damaged real order | The test produces a truncated event, with a valid one behind it | It is quarantined, and the sequence guard then holds its symbol rather than skip the order |
| Blocked database write | The test holds BTC's progress row | ETH on another partition finishes while BTC waits, and BTC then completes in order |
| Repeated failure on one symbol | A trigger rejects BTC's trades | Only BTC's partition is held and readiness is false. ETH finishes, with no session restart, and BTC completes once the failure clears |
| Lost Kafka event | The test marks an event as published without sending it, with a later event behind it in the topic | Matching blocks rather than skips. The operator procedure republishes unapplied events from the Outbox, and matching resumes in order with no repeated trade |

### External notifications

| Failure | How it is injected | Result proven |
|---|---|---|
| Remote effect commits, response lost | The mock commits and resets the connection, and is then killed and restarted | The retry uses the same key, the mock replays, and there is one effect |
| Remote effect commits, then a timeout | The mock commits and never answers | The retry uses the same key, with one effect |
| Crash during the call | `SIGKILL` of the worker while the mock withholds its answer | The next owner reclaims the job, with one effect |
| Crash after the answer, before recording it | A preload that sends `SIGKILL` just before the result is written | The job is still due with no attempt used, and the retry is replayed, with one effect |
| Destination outage | The mock is not running | Matching continues, and attempts and due times survive a restart of the role |
| Matching blocked | A sequence gap holds its partition in matching | Valid events behind it are still notified, because the groups are separate |
| Kafka unreachable | The role points at a closed port | Existing jobs are still delivered |
| Retries exhausted | The mock is not running, with a small budget | The job is `FAILED`, and the order is unchanged |
| Crash inside the handoff | The test holds the Inbox row to park the transaction, then sends `SIGKILL` | No job and no offset are kept, and the restart delivers once |
| Crash between the handoff and the offset | A preload that sends `SIGKILL` as the offset is committed | The redelivery creates no second job |
| `SIGTERM` or a rebalance during the handoff | The test holds the Inbox row, then signals or joins the group | The transaction finishes, and no offset passes durable work |
| Database failure in the handoff | A database trigger raises on the insert | The offset holds until the job is stored |
| Malformed message | Invalid JSON, a wrong shape and a tombstone on the topic | Each is quarantined, then the valid event behind them is delivered |
| Value PostgreSQL would refuse | A copy of an event with a NUL in its symbol | It is quarantined with no job, and the valid event behind it is delivered |
| Stored job PostgreSQL can't read values from | Rows with a NUL, a lone surrogate, and a NUL under a duplicate key | Every due job is still attempted, with its log context |
| Quarantine write fails | A database trigger raises on the insert | The offset holds until the message is recorded |
| Result arrives after the job changed | The job's attempts move during the call | Nothing claims the outcome, and a warning says it went unrecorded |
| `Retry-After` | The mock answers 503 with the header | The next attempt waits at least that long, up to the cap |
| Conflicting body | A different body under the same key | `422`, a `FAILED` job, and the earlier effect is untouched |
| Permanent status | The mock answers 400 | The job is `FAILED` at once, and the redrive command delivers it under the same key |
| `SIGTERM` during an attempt | The mock withholds its answer | The attempt finishes within its timeout, and the process exits cleanly |

### Ownership

| Failure | How it is injected | Result proven |
|---|---|---|
| Second process started | A real second publisher, matcher or external role | It waits, does nothing, and takes over after `SIGKILL` of the owner |
| Ownership connection lost | `pg_terminate_backend` on the lock's connection | The role stops and exits with status 1 |
| Answer to a lock attempt lost | A proxy drops replies, so the attempt runs on the server and takes the lock | The attempt times out, its connection is dropped, and a successor gets the lock |

### Lifecycle, health and visibility

| Situation | How it is produced | Result proven |
|---|---|---|
| `SIGTERM` during work | The test parks a transaction, then signals the role | Readiness turns false while liveness still answers, and the work finishes |
| Work outlasts the deadline | A transaction stays parked past `SHUTDOWN_TIMEOUT_MS` | Exit status 1 at the deadline, and the abandoned work is replayed once |
| Standby's connection goes quiet | A proxy drops replies while the standby waits for the lock | Exit status 1, and a successor takes the lock |
| Owner's connection goes quiet | A proxy swallows everything, so closing can't finish | Exit status 1 at the deadline, and a successor takes the lock |
| Lost ownership with work parked | The lock's connection is terminated during a parked transaction | Exit status 1 at the deadline |
| Role fails with no signal | A role body that rejects | The deadline starts before anything closes |
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

## Required results

The failures the design must survive, the result each must have, and the test that proves it. Tests that claim a delivery guarantee run against real PostgreSQL and real Kafka.

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

Matching edge cases are covered in [matching.test.ts][engine] unless noted:

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
