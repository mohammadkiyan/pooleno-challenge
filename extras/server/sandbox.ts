import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { setTimeout as pause } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { Pool } from "pg";
import { Admin, Producer, ProduceAcks, compatibilityPartitioner, stringSerializers } from "@platformatic/kafka";
import type { ActualOrder, LiveFrame, LiveRun, Snapshot } from "../shared/live.ts";
import type { NodeId, PhaseId } from "../frontend/flow-types.ts";

const ROOT = fileURLToPath(new URL("../../", import.meta.url));
const ADMIN_URL = process.env.VIEWER_DATABASE_URL ?? "postgres://pooleno@127.0.0.1:15432/postgres";
const BROKERS = (process.env.VIEWER_KAFKA_BROKERS ?? "127.0.0.1:29092").split(",");
const API = "http://127.0.0.1:18100";
const DESTINATION = "http://127.0.0.1:18180";
const units = (value: string, scale: number) => {
  const digits = BigInt(value).toString().padStart(scale + 1, "0");
  return `${digits.slice(0, -scale)}.${digits.slice(-scale)}`;
};
export async function until<T>(read: () => Promise<T | undefined>, message: string, signal: AbortSignal) {
  const deadline = Date.now() + 60_000;
  while (Date.now() < deadline) {
    signal.throwIfAborted();
    const value = await read();
    if (value !== undefined) return value;
    await pause(150, undefined, { signal });
  }
  throw new Error(message);
}
export class Sandbox {
  run: LiveRun;
  signal: AbortSignal;
  database: string;
  topic: string;
  pool: Pool;
  admin: Pool;
  children = new Map<string, ChildProcess>();
  directory = "";
  labels = new Map<string, string>();
  databaseCreated = false;
  env: NodeJS.ProcessEnv;
  constructor(run: LiveRun, signal: AbortSignal) {
    this.run = run;
    this.signal = signal;
    this.database = `viewer_${run.id.replaceAll("-", "")}`;
    this.topic = `viewer.orders.${run.id}`;
    const url = new URL(ADMIN_URL);
    url.pathname = `/${this.database}`;
    this.pool = new Pool({ connectionString: url.href, connectionTimeoutMillis: 3000, query_timeout: 5000 });
    this.admin = new Pool({ connectionString: ADMIN_URL, max: 1, connectionTimeoutMillis: 3000, query_timeout: 5000 });
    this.env = {
      ...process.env, DATABASE_URL: url.href, KAFKA_BROKERS: BROKERS.join(","), KAFKA_TOPIC: this.topic,
      HOST: "127.0.0.1", PORT: "18100", HEALTH_HOST: "127.0.0.1", LOG_LEVEL: "info",
      MATCHING_GROUP_ID: `viewer-matching-${run.id}`, EXTERNAL_GROUP_ID: `viewer-external-${run.id}`,
      EXTERNAL_API_URL: `${DESTINATION}/v1/notifications`, EXTERNAL_TIMEOUT_MS: "1200",
      EXTERNAL_POLL_INTERVAL_MS: "100", EXTERNAL_RETRY_BASE_MS: "1500", EXTERNAL_RETRY_CAP_MS: "3000",
      EXTERNAL_RETRY_JITTER_PERCENT: "0", EXTERNAL_MAX_ATTEMPTS: run.variantId === "retry-exhaustion" ? "2" : "400",
      OUTBOX_POLL_INTERVAL_MS: "100", OUTBOX_RETRY_BASE_MS: "500", OUTBOX_RETRY_CAP_MS: "1500",
      SHUTDOWN_TIMEOUT_MS: "1500", MOCK_API_HOST: "127.0.0.1", MOCK_API_PORT: "18180",
    };
  }
  start(role: string, entry = role, args: string[] = []) {
    this.signal.throwIfAborted();
    const child = spawn(process.execPath, [resolve(ROOT, `dist/${entry === "mock-api" ? "mock-api/server" : `entrypoints/${entry}`}.js`), ...args], {
      cwd: ROOT, env: { ...this.env, HEALTH_PORT: role === "publisher" ? "18101" : role === "matching" ? "18102" : "18103" },
      stdio: ["ignore", "pipe", "pipe"],
    });
    this.children.set(role, child);
    const stop = () => child.kill("SIGKILL");
    this.signal.addEventListener("abort", stop, { once: true });
    child.once("exit", () => this.signal.removeEventListener("abort", stop));
    for (const stream of [child.stdout, child.stderr]) {
      let buffer = "";
      stream?.on("data", (data: Buffer) => {
        buffer += data.toString();
        const lines = buffer.split("\n");
        buffer = lines.pop() ?? "";
        for (const line of lines) {
          this.run.logs.push({ role, line: line.slice(0, 8000) });
          if (this.run.logs.length > 1200) this.run.logs.shift();
        }
      });
    }
    child.on("error", error => this.run.logs.push({ role, line: error.message }));
    return child;
  }
  async command(role: string, args: string[] = []) {
    const child = this.start(role, role, args);
    const code = await new Promise<number | null>((resolve, reject) => {
      child.once("error", reject); child.once("exit", resolve);
    });
    this.children.delete(role);
    if (code !== 0) throw new Error(`${role} exited with ${code}; inspect the process logs.`);
  }
  async stop(role: string, signal: NodeJS.Signals = "SIGTERM") {
    const child = this.children.get(role);
    if (!child) return;
    if (child.exitCode === null && child.signalCode === null) {
      const exited = new Promise<void>(resolve => child.once("exit", () => resolve()));
      child.kill(signal);
      const timer = setTimeout(() => child.kill("SIGKILL"), 2500);
      await exited.finally(() => clearTimeout(timer));
    }
    this.children.delete(role);
  }
  async ready(port: number, path = "/health/ready") {
    await until(async () => {
      try { return (await this.request(`http://127.0.0.1:${port}${path}`)).status === 200 ? true : undefined; }
      catch { return undefined; }
    }, `Process on port ${port} did not become ready; inspect the process logs.`, this.signal);
  }
  async setup() {
    // Names are generated internally. No request controls a database identifier or SQL text.
    if (!/^viewer_[a-f0-9]{32}$/.test(this.database)) throw new Error("Invalid sandbox database name");
    this.directory = await mkdtemp(join(tmpdir(), "pooleno-viewer-"));
    this.env.MOCK_API_DATABASE = join(this.directory, "destination.sqlite");
    await this.admin.query(`CREATE DATABASE "${this.database}"`);
    this.databaseCreated = true;
    await this.command("migrate");
    await this.command("init-kafka");
    this.start("mock-api");
    await this.ready(18180, "/__control/effects");
    this.start("api");
    this.start("publisher");
    this.start("matching");
    this.start("external");
    await Promise.all([18100, 18101, 18102, 18103].map(port => this.ready(port)));
  }
  async request(url: string, init: RequestInit = {}) {
    const response = await fetch(url, { ...init, headers: { connection: "close", ...init.headers }, signal: AbortSignal.any([this.signal, AbortSignal.timeout(5000)]) });
    const text = await response.text();
    return { status: response.status, body: text ? JSON.parse(text) as Record<string, unknown> : {} };
  }
  async post(label: string, side: "BUY" | "SELL", quantity: string, price: string, key = `viewer-${randomUUID()}`) {
    const body = { symbol: "ETH-USD", side, type: "LIMIT", price, quantity };
    const reply = await this.request(`${API}/v1/orders`, {
      method: "POST", headers: { "content-type": "application/json", "idempotency-key": key }, body: JSON.stringify(body),
    });
    if (typeof reply.body.orderId === "string") this.labels.set(reply.body.orderId, label);
    await this.record({ title: `${label}: HTTP ${reply.status}`, detail: "Response received from the real Order API.", active: ["api", "client"], phase: "api-reply", request: `POST /v1/orders\nIdempotency-Key: ${key}\n${JSON.stringify(body, null, 2)}`, response: `${reply.status}\n${JSON.stringify(reply.body, null, 2)}`, source: "src/modules/orders/orders.controller.ts" });
    return { ...reply, key, orderId: String(reply.body.orderId ?? "") };
  }
  async order(orderId: string) {
    const reply = await this.request(`${API}/v1/orders/${orderId}`);
    if (reply.status !== 200) throw new Error(`GET order returned ${reply.status}`);
    return reply.body as unknown as ActualOrder;
  }
  async settled(orderId: string) {
    return until(async () => {
      const order = await this.order(orderId);
      return order.status !== "QUEUED" ? order : undefined;
    }, `Order ${orderId} stayed QUEUED.`, this.signal);
  }
  async fixture(label: string, side: "BUY" | "SELL", quantity: string, price: string) {
    const reply = await this.post(label, side, quantity, price);
    this.check(`${label} accepted`, reply.status, 202);
    await this.settled(reply.orderId);
    await this.record({ title: `${label} processed by matcher`, active: ["matching", "database"], phase: "match-book", source: "src/modules/matching/apply-accepted-order.ts" });
    return reply.orderId;
  }
  check(claim: string, actual: unknown, expected: unknown) {
    const passed = JSON.stringify(actual) === JSON.stringify(expected);
    this.run.checks.push({ claim, passed, actual, expected });
    if (!passed) throw new Error(`${claim}: expected ${JSON.stringify(expected)}, got ${JSON.stringify(actual)}`);
  }
  async snapshot(): Promise<Snapshot> {
    const client = await this.pool.connect();
    let rows: { orderIds: string[]; trades: Snapshot["trades"]; outbox: Snapshot["outbox"]; jobs: Snapshot["jobs"]; inbox: Snapshot["inbox"]; receipts: number };
    try {
      await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
      const orders = await client.query<{ id: string }>("SELECT id FROM orders ORDER BY acceptance_seq");
      const trades = await client.query<{ id: string; maker_id: string; taker_id: string; price_units: string; quantity_units: string }>("SELECT * FROM trades ORDER BY executed_at, event_id, execution_index");
      const outbox = await client.query<Snapshot["outbox"][number]>('SELECT event_id AS "eventId", order_id AS "orderId", published_at AS "publishedAt", attempts, last_error AS "lastError" FROM outbox_events ORDER BY acceptance_seq');
      const jobs = await client.query<Snapshot["jobs"][number]>('SELECT event_id AS "eventId", payload->>\'orderId\' AS "orderId", target_key AS "targetKey", status, attempts, next_attempt_at AS "nextAttemptAt", last_error AS "lastError" FROM external_jobs ORDER BY created_at, event_id');
      const inbox = await client.query<Snapshot["inbox"][number]>("SELECT consumer_name, event_id FROM consumer_inbox ORDER BY consumer_name, event_id");
      const receipts = await client.query<{ count: string }>("SELECT count(*) FROM idempotency_requests");
      rows = { orderIds: orders.rows.map(o => o.id), trades: trades.rows.map(t => ({ tradeId: t.id, makerId: t.maker_id, takerId: t.taker_id, price: units(t.price_units, 2), quantity: units(t.quantity_units, 4) })), outbox: outbox.rows, jobs: jobs.rows, inbox: inbox.rows, receipts: Number(receipts.rows[0]?.count) };
      await client.query("COMMIT");
    } catch (error) { await client.query("ROLLBACK"); throw error; }
    finally { client.release(); }
    // Each API order response is itself a consistent order/trades snapshot. This capture
    // combines observations across API, PostgreSQL and destination; it is not a distributed snapshot.
    const orders = await Promise.all(rows.orderIds.map(id => this.order(id)));
    const { body } = await this.request(`${DESTINATION}/__control/effects`);
    return { orders, trades: rows.trades, outbox: rows.outbox, jobs: rows.jobs, inbox: rows.inbox, effects: body.effects as Snapshot["effects"], counts: { orders: orders.length, events: rows.outbox.length, receipts: rows.receipts, trades: rows.trades.length } };
  }
  async record(input: { title: string; active: NodeId[]; phase?: PhaseId; detail?: string; request?: string; response?: string; source: string; blocked?: NodeId[]; bookState?: "available" | "discarded" }) {
    const snapshot = await this.snapshot();
    const label = (id: string) => this.labels.get(id) ?? id;
    const frame: LiveFrame = {
      observedAt: new Date().toISOString(), snapshot, title: input.title,
      detail: input.detail ?? "Observed from the running backend and persisted records. Inspect the captured evidence below.",
      active: input.active, blocked: input.blocked ?? [], parallel: [], phase: input.phase ?? null, transfers: [],
      book: snapshot.orders.filter(o => o.status === "OPEN" || o.status === "PARTIALLY_FILLED").sort((a,b) => a.side.localeCompare(b.side) || (a.side === "SELL" ? Number(a.price) - Number(b.price) : Number(b.price) - Number(a.price)) || Number(BigInt(a.acceptanceSequence) - BigInt(b.acceptanceSequence))).map(o => ({ id: label(o.orderId), side: o.side, price: o.price, quantity: o.remainingQuantity })),
      trades: snapshot.trades.map(t => ({ maker: label(t.makerId), taker: label(t.takerId), price: t.price, quantity: t.quantity })),
      facts: [["Persisted orders", String(snapshot.counts.orders)], ["Persisted trades", String(snapshot.counts.trades)], ["Pending Outbox", String(snapshot.outbox.filter(e => !e.publishedAt).length)], ["Delivery attempts", String(snapshot.jobs.reduce((n,j) => n + j.attempts, 0))]],
      request: input.request ?? "Read API orders, PostgreSQL records and destination effects",
      response: input.response ?? JSON.stringify(snapshot, null, 2), requestLabel: input.title,
      decision: "Display observed data; fail the run if a checked result differs from the scenario contract.", source: input.source,
      ...(input.bookState ? { bookState: input.bookState } : {}),
    };
    this.run.frames.push(frame);
  }
  async fault(body: object) {
    const result = await this.request(`${DESTINATION}/__control/faults`, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    this.check("Destination fault configured", result.status, 204);
  }
  async redeliver(orderId: string) {
    const { rows } = await this.pool.query<{ payload: string }>("SELECT payload::text FROM outbox_events WHERE order_id = $1", [orderId]);
    const value = rows[0]?.payload;
    if (!value) throw new Error("No persisted event to redeliver");
    const producer = new Producer({ clientId: "viewer-redelivery", bootstrapBrokers: BROKERS, serializers: stringSerializers, idempotent: true, retries: 3, acks: ProduceAcks.ALL, partitioner: compatibilityPartitioner });
    try { await producer.send({ messages: [{ topic: this.topic, key: "ETH-USD", value }] }); }
    finally { await producer.close(); }
  }
  async cleanup() {
    await Promise.all([...this.children.keys()].map(role => this.stop(role)));
    await this.pool.end();
    const failures: string[] = [];
    try {
      if (this.databaseCreated) await this.admin.query(`DROP DATABASE "${this.database}" WITH (FORCE)`);
    } catch (e) { failures.push(String(e)); }
    finally { await this.admin.end(); }
    const admin = new Admin({ clientId: "viewer-cleanup", bootstrapBrokers: BROKERS, timeout: 5000, retries: 0 });
    try { await admin.deleteTopics({ topics: [this.topic] }); }
    catch (e) { failures.push(String(e)); }
    finally { await admin.close(); }
    if (this.directory) await rm(this.directory, { recursive: true, force: true });
    if (failures.length) throw new Error(failures.join("; "));
  }
}
