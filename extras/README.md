# Live scenario viewer

The viewer executes the actual backend. **Run scenario** sends HTTP requests to the compiled Order API and observes the real PostgreSQL, Outbox publisher, Kafka consumers, matching engine and notification worker. There are no authored result frames or fallback success responses.

From the repository root:

```bash
docker compose up -d --build --wait postgres kafka viewer
```

Open [the viewer](http://127.0.0.1:8084). `./review.sh` also builds and opens it after its terminal checks. If another project uses the default ports, set `HOST_PORT_POSTGRES`, `HOST_PORT_KAFKA` and `HOST_PORT_VIEWER` to free ports.

## What runs

Each run gets a generated PostgreSQL database, Kafka topic, consumer groups and destination SQLite file. Separate Node processes execute the same compiled API, publisher, matching and external entry points used by the application. Migrations and topic initialization use the existing entry points too. The runner orchestrates requests and checks results; it contains no matching, idempotency or delivery implementation.

The six primary scenarios and nine additional cases exercise:

- Price priority, FIFO, partial execution, SELL takers, and a partially filled taker resting at its limit.
- Non-crossing orders that remain open.
- Equivalent request retries, including 25 actual concurrent requests.
- Conflicting keys and invalid price/quantity requests with no acceptance writes.
- A destination that applies an effect and loses its response; a transient 503 with Retry-After; permanent 400 followed by the real redrive CLI; and retry-budget exhaustion.
- Kafka connection interruption, durable acceptance and Outbox recovery.
- A matching process killed with SIGKILL, restarted, then given the original persisted event through Kafka. The runner waits for the duplicate record's committed offset and verifies unchanged trades and quantities. This is a crash after an observed commit, not injection into the exact commit/offset window.

The exhaustion case explicitly sets `EXTERNAL_MAX_ATTEMPTS=2`. Other cases retain the application's 400-attempt default. Retry timing is shortened in the sandbox and jitter is disabled so the run is inspectable.

The external destination remains the challenge's existing **mock API**. It receives real HTTP notifications and stores real idempotent effects in its own SQLite database. Its fault controls drive actual worker retries; the frontend never fabricates their outcome.

## Evidence and controls

The flow, book, executions and counters update from backend observations. Both diagram views use those observations. Internal boxes explain the implementation; they do not claim to be per-function traces. The runner captures API responses, PostgreSQL rows, destination effects, process logs, and each assertion's actual/expected values. Captures across services are taken at nearby times, not in one distributed transaction. A run is verified only after its checks and cleanup finish. Errors remain visible with the evidence captured before failure.

The small playback controls review captured observations. They do not pause the backend. **Run again** performs a fresh execution. Reloading the page reconnects to the latest run. One run executes at a time; other requests get 409. The latest ten run captures are retained in memory until the viewer restarts; each has a **Full run JSON** link.

Finally, the runner stops its child processes, removes only its generated database/topic and deletes its temporary destination file. Existing application orders, topics and destination effects are untouched. A failed cleanup is shown as a failed run. Abruptly killing the viewer itself can leave generated `viewer_*` databases or `viewer.orders.*` topics; graceful Compose shutdown runs cleanup.

Kafka uses a separate `VIEWER` listener advertising the viewer's TCP forwarding address. The outage scenario destroys this link's sockets and rejects new connections, then reconnects them. The broker and the application's normal INTERNAL/HOST listeners keep running. No Docker socket or host filesystem is mounted into the viewer. The runner is a local development tool, bound to loopback through Compose; it rejects cross-origin controls and accepts only fixed scenario/case IDs.

## Develop and verify

Install the root and extras dependencies. With the Docker viewer running, Vite proxies `/live` to it:

```bash
cd extras
npm ci
npm run dev
```

The default runner target is `http://127.0.0.1:8084`; set `VIEWER_URL` when using a different port. `npm run live` can run the Node runner on the host after building the root backend and frontend, but the Kafka interruption case requires the Docker VIEWER listener.

```bash
npm test
npm run build
VIEWER_URL=http://127.0.0.1:8084 npm run test:browser
```

Install Playwright's Chromium once with `npx playwright install chromium`. Browser tests click the real UI, verify actual order/trade UUIDs, exercise all live scenarios/cases, check offline errors, and reject concurrent/cross-origin controls. They require the Docker runner; they do not stub successful backend results.

The optional viewer stays outside `src/`. The backend's own build and runtime do not import it.
