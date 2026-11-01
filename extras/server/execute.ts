import { Admin } from "@platformatic/kafka";
import { Sandbox, until } from "./sandbox.ts";
import type { LiveRun } from "../shared/live.ts";

export async function execute(run: LiveRun, signal: AbortSignal, link?: { setOnline: (value: boolean) => void }) {
  const box = new Sandbox(run, signal);
  try {
    await box.setup();
    await box.record({ title: "Real backend ready", active: ["api", "database", "kafka", "matching", "worker"], detail: "Fresh sandbox database and Kafka topic. The compiled application API, publisher, matcher and external worker are running as separate processes.", source: "src/entrypoints/" });
    switch (run.scenarioId) {
      case "matching": await matching(box); break;
      case "non-crossing": {
        const sell = await box.fixture("S1", "SELL", "2", "101");
        const buy = await box.fixture("B1", "BUY", "1", "100");
        const snapshot = await box.snapshot();
        box.check("Non-crossing orders remain OPEN", await Promise.all([sell, buy].map(async id => (await box.order(id)).status)), ["OPEN", "OPEN"]);
        box.check("No executions outside the limit", snapshot.counts.trades, 0);
        break;
      }
      case "duplicate": await duplicate(box); break;
      case "conflict": await conflict(box); break;
      case "lost-response": await delivery(box); break;
      case "kafka-outage": {
        if (!link) throw new Error("Kafka connection control is unavailable. Run the Docker viewer with its VIEWER listener.");
        link.setOnline(false);
        await box.record({ title: "Sandbox Kafka connection cut", active: ["outbox"], blocked: ["kafka"], phase: "publish-retry", source: "extras/server/kafka-link.ts", detail: "Existing TCP sockets were destroyed and new connections are rejected. Other Kafka listeners stay available." });
        const order = await box.post("S1", "SELL", "1", "200");
        box.check("API accepts while Kafka cannot be reached", order.status, 202);
        const snapshot = await until(async () => {
          const s = await box.snapshot();
          return s.outbox.some(e => e.attempts > 0 && e.publishedAt === null) ? s : undefined;
        }, "Publisher did not record a failed send during the disconnection.", signal);
        box.check("Order remains durably QUEUED", snapshot.orders[0]?.status, "QUEUED");
        await box.record({ title: "Acceptance and failed publication persisted", active: ["database", "outbox"], blocked: ["kafka"], phase: "publish-retry", source: "src/modules/messaging/publish-outbox.ts" });
        link.setOnline(true);
        await box.settled(order.orderId);
        await delivered(box, order.orderId);
        const recovered = await box.snapshot();
        box.check("Recovered order rests in the book", recovered.orders[0]?.status, "OPEN");
        box.check("Outbox is marked published after recovery", recovered.outbox.every(e => e.publishedAt !== null), true);
        await box.record({ title: "Kafka connection restored; work recovered", active: ["kafka", "matching", "worker"], phase: "match-book", source: "src/modules/messaging/publish-outbox.ts" });
        break;
      }
    }
    // Wait for both independent consumers before taking the final capture.
    if (run.variantId !== "retry-exhaustion") {
      await until(async () => {
        const s = await box.snapshot();
        return s.orders.every(o => o.status !== "QUEUED") && s.jobs.length === s.orders.length && s.jobs.every(j => j.status === "SUCCEEDED") && s.outbox.every(e => e.publishedAt !== null) ? true : undefined;
      }, "The independent matching/publication/delivery branches did not finish.", signal);
    }
    await box.record({ title: "Backend results verified", active: ["database"], source: "extras/server/execute.ts", detail: "All listed checks passed against actual HTTP responses and persisted records. These captures are retained after the disposable sandbox is removed." });
  } catch (error) {
    run.error = error instanceof Error ? error.message : String(error);
  } finally {
    link?.setOnline(true);
    try { await box.cleanup(); }
    catch (error) { run.cleanupError = String(error); run.error ??= "Sandbox cleanup failed; inspect cleanupError."; }
    run.status = run.error ? "failed" : "passed";
    run.finishedAt = new Date().toISOString();
  }
}
async function matching(box: Sandbox) {
  const sellTaker = box.run.variantId === "sell-taker";
  const makerSide = sellTaker ? "BUY" : "SELL";
  const prefix = sellTaker ? "B" : "S";
  const takerLabel = sellTaker ? "S1" : "B1";
  const partial = box.run.variantId === "partial-taker";
  const first = await box.fixture(`${prefix}1`, makerSide, partial ? "1" : "2", partial ? "99" : "100");
  const second = await box.fixture(`${prefix}2`, makerSide, partial ? "1" : "3", partial ? "105" : "100");
  if (!partial) await box.fixture(`${prefix}3`, makerSide, "1", sellTaker ? "101" : "99");
  const taker = await box.fixture(takerLabel, sellTaker ? "SELL" : "BUY", partial ? "3" : "4", "100");
  const actual = await box.order(taker);
  box.check("Taker status", actual.status, partial ? "PARTIALLY_FILLED" : "FILLED");
  box.check("Execution prices and quantities in order", actual.trades.map(t => [t.price, t.quantity]), partial ? [["99.00", "1.0000"]] : [[sellTaker ? "101.00" : "99.00", "1.0000"], ["100.00", "2.0000"], ["100.00", "1.0000"]]);
  box.check("Maker remainder", (await box.order(second)).remainingQuantity, partial ? "1.0000" : "2.0000");
  box.check("First maker filled", (await box.order(first)).status, "FILLED");
  if (partial) box.check("Unfilled taker quantity rests at its own limit", actual.remainingQuantity, "2.0000");
  await box.record({ title: "Price, FIFO and quantities read from committed trades", active: ["matching", "database"], phase: "match-book", source: "src/modules/matching/apply-accepted-order.ts" });
  if (box.run.variantId === "crash-replay") {
    const before = await box.snapshot();
    await box.stop("matching", "SIGKILL");
    await box.record({ title: "Matching process killed with SIGKILL", active: ["database"], blocked: ["matching"], bookState: "discarded", source: "src/modules/matching/restore-book.ts" });
    box.start("matching");
    await box.ready(18102);
    await box.record({ title: "Matcher restarted and restored its book", active: ["matching", "database"], phase: "match-restore", source: "src/modules/matching/restore-book.ts" });
    const event = before.outbox.find(e => e.orderId === taker);
    if (!event) throw new Error("Missing taker event");
    await box.redeliver(taker);
    await until(async () => {
      const admin = new Admin({ clientId: "viewer-replay-proof", bootstrapBrokers: box.env.KAFKA_BROKERS!.split(","), timeout: 5000, retries: 0 });
      try {
        const [group] = await admin.listConsumerGroupOffsets({ groups: [box.env.MATCHING_GROUP_ID!] });
        const committed = group?.topics.find(t => t.name === box.topic)?.partitions.reduce((n,p) => n + Math.max(0, Number(p.committedOffset)), 0) ?? 0;
        return committed >= before.counts.events + 1 ? true : undefined;
      } finally { await admin.close(); }
    }, "Matcher did not acknowledge the duplicate Kafka record.", box.signal);
    const after = await box.snapshot();
    box.check("Redelivery creates no new trade", after.trades, before.trades);
    box.check("Restored order quantities unchanged", after.orders.map(o => [o.orderId, o.status, o.remainingQuantity]), before.orders.map(o => [o.orderId, o.status, o.remainingQuantity]));
    await box.record({ title: "Duplicate Kafka record acknowledged without another execution", active: ["matching", "kafka"], phase: "match-replay", source: "src/modules/matching/apply-accepted-order.ts" });
  }
}
async function duplicate(box: Sandbox) {
  const key = `viewer-${box.run.id}`;
  if (box.run.variantId === "concurrent-retries") {
    // All requests start before any is awaited. Only the capture is batched afterward.
    const body = { symbol: "ETH-USD", side: "SELL", type: "LIMIT", price: "200", quantity: "1" };
    const replies = await Promise.all(Array.from({ length: 25 }, () => box.request("http://127.0.0.1:18100/v1/orders", { method: "POST", headers: { "content-type": "application/json", "idempotency-key": key }, body: JSON.stringify(body) })));
    box.check("All 25 requests accepted", replies.map(r => r.status), Array(25).fill(202));
    box.check("All receipts identify one order", new Set(replies.map(r => JSON.stringify(r.body))).size, 1);
    await box.record({ title: "25 concurrent HTTP responses received", active: ["client", "api"], phase: "api-reply", request: `25 × POST /v1/orders\nIdempotency-Key: ${key}\n${JSON.stringify(body, null, 2)}`, response: JSON.stringify(replies, null, 2), source: "src/modules/orders/accept-order.ts" });
  } else {
    const first = await box.post("order-A", "SELL", "1", "200", key);
    const retry = await box.post("order-A", "SELL", "1.0000", "200.00", key);
    box.check("Retry replays the original receipt", { status: retry.status, body: retry.body }, { status: first.status, body: first.body });
  }
  const snapshot = await box.snapshot();
  box.check("Exactly one order, event and receipt", [snapshot.counts.orders, snapshot.counts.events, snapshot.counts.receipts], [1, 1, 1]);
}
async function conflict(box: Sandbox) {
  const variant = box.run.variantId;
  if (variant === "invalid-precision" || variant === "invalid-quantity") {
    const reply = await box.post("invalid", "BUY", variant === "invalid-quantity" ? "0" : "1", variant === "invalid-precision" ? "100.001" : "100");
    box.check("Validation rejects the request", reply.status, 400);
    box.check("Expected validation code", reply.body.code, variant === "invalid-quantity" ? "INVALID_QUANTITY" : "INVALID_PRICE");
    const snapshot = await box.snapshot();
    box.check("Validation performs no acceptance writes", snapshot.counts, { orders: 0, events: 0, receipts: 0, trades: 0 });
    return;
  }
  const key = `viewer-${box.run.id}`;
  const original = await box.post("order-A", "SELL", "1", "200", key);
  const reply = await box.post("changed-request", "SELL", "2", "200", key);
  box.check("Changed request rejected", [reply.status, reply.body.code], [422, "IDEMPOTENCY_KEY_REUSED"]);
  box.check("Original request still replays", (await box.post("order-A", "SELL", "1", "200", key)).body, original.body);
  const snapshot = await box.snapshot();
  box.check("Conflict does not create another acceptance", [snapshot.counts.orders, snapshot.counts.events, snapshot.counts.receipts], [1, 1, 1]);
}
async function delivered(box: Sandbox, orderId: string) {
  return until(async () => {
    const snapshot = await box.snapshot();
    const job = snapshot.jobs.find(j => j.orderId === orderId);
    return job?.status === "SUCCEEDED" ? { snapshot, job } : undefined;
  }, "Notification was not confirmed by the worker.", box.signal);
}
async function delivery(box: Sandbox) {
  const variant = box.run.variantId;
  await box.fault(variant === "temporary-503" ? { mode: "status", status: 503, retryAfter: "2" } : variant === "permanent-redrive" ? { mode: "status", status: 400 } : variant === "retry-exhaustion" ? { mode: "status", status: 503, times: 2 } : { mode: "lost-response" });
  const order = await box.post("S1", "SELL", "1", "200");
  box.check("Order accepted", order.status, 202);
  const first = await until(async () => {
    const s = await box.snapshot();
    return s.jobs[0] && s.jobs[0].attempts >= 1 ? s : undefined;
  }, "Worker did not persist its first failed delivery.", box.signal);
  box.check("First delivery attempt persisted", first.jobs[0]?.attempts, 1);
  if (!variant) box.check("Lost response already caused one remote effect", first.effects.length, 1);
  await box.record({ title: "Worker recorded the destination failure", active: ["worker", "database"], phase: "notify-retry", source: "src/modules/external/notification-worker.ts" });
  if (variant === "retry-exhaustion") {
    const failed = await until(async () => { const s = await box.snapshot(); return s.jobs[0]?.status === "FAILED" ? s : undefined; }, "Retry budget did not exhaust.", box.signal);
    box.check("Sandbox two-attempt budget exhausted", [failed.jobs[0]?.status, failed.jobs[0]?.attempts, failed.effects.length], ["FAILED", 2, 0]);
    await box.record({ title: "Two-attempt sandbox retry budget exhausted", active: ["worker", "database"], phase: "notify-complete", detail: "This run explicitly sets EXTERNAL_MAX_ATTEMPTS=2. The application default is 400.", source: "src/modules/external/notification-worker.ts" });
    return;
  }
  if (variant === "permanent-redrive") {
    box.check("Permanent error fails the job", first.jobs[0]?.status, "FAILED");
    const key = first.jobs[0]!.targetKey;
    await box.command("redrive-external", [first.jobs[0]!.eventId]);
    await box.record({ title: "Actual redrive CLI executed", active: ["worker", "database"], request: `node dist/entrypoints/redrive-external.js ${first.jobs[0]!.eventId}`, source: "src/entrypoints/redrive-external.ts" });
    const after = await delivered(box, order.orderId);
    box.check("Redrive preserves the destination key", after.job.targetKey, key);
    box.check("One successful attempt after redrive", after.job.attempts, 1);
  } else {
    const after = await delivered(box, order.orderId);
    box.check("Worker persisted both delivery attempts", after.job.attempts, 2);
    if (variant === "temporary-503") {
      const retryLog = box.run.logs.filter(l => l.role === "external").map(l => { try { return JSON.parse(l.line) as { msg?: string; retryDelayMs?: number; time?: number }; } catch { return {}; } }).find(l => l.msg === "delivery failed; will retry");
      box.check("Worker honors the two-second Retry-After minimum", (retryLog?.retryDelayMs ?? 0) >= 2000, true);
    }
  }
  const final = await box.snapshot();
  box.check("Destination contains exactly one effect", final.effects.length, 1);
  box.check("Effect belongs to this order", (JSON.parse(final.effects[0]!.body) as { orderId: string }).orderId, order.orderId);
  await box.record({ title: "Delivery confirmed by worker and destination", active: ["worker", "external", "database"], phase: "notify-complete", source: "src/modules/external/notification-worker.ts" });
}
