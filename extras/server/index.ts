import Fastify from "fastify";
import { randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import { extname, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { execute } from "./execute.ts";
import { createKafkaLink } from "./kafka-link.ts";
import { liveScenarios, liveVariants } from "../shared/live.ts";
import type { LiveRun, ScenarioId } from "../shared/live.ts";

const app = Fastify({ logger: true, bodyLimit: 2048 });
const runs = new Map<string, LiveRun>();
let active: LiveRun | undefined;
let task: Promise<void> | undefined;
let abort: AbortController | undefined;
let closing = false;
const link = process.env.VIEWER_KAFKA_UPSTREAM ? await createKafkaLink(19094, { host: process.env.VIEWER_KAFKA_UPSTREAM, port: 19094 }) : undefined;
const dist = fileURLToPath(new URL("../dist/", import.meta.url));
const mime: Record<string, string> = { ".html": "text/html; charset=utf-8", ".js": "text/javascript", ".css": "text/css", ".svg": "image/svg+xml", ".png": "image/png", ".ico": "image/x-icon" };
app.addHook("onRequest", (request, reply, done) => {
  void reply.header("cache-control", "no-store").header("x-content-type-options", "nosniff");
  const hostName = (request.headers.host ?? "").split(":")[0] ?? "";
  if (!["127.0.0.1", "localhost", "viewer"].includes(hostName)) {
    void reply.code(403).send({ error: "Use the local viewer address." }); return;
  }
  const origin = request.headers.origin;
  const host = request.headers.host;
  if (origin && origin !== `http://${host}` && origin !== `https://${host}`) {
    void reply.code(403).send({ error: "Cross-origin control requests are not allowed." });
    return;
  }
  if (request.headers["sec-fetch-site"] === "cross-site") {
    void reply.code(403).send({ error: "Cross-site control requests are not allowed." }); return;
  }
  done();
});
app.get("/live/health", () => ({ status: "ready", activeRunId: active?.status === "running" ? active.id : null, kafkaControl: !!link }));
app.get("/live/active", () => active ?? null);
app.post<{ Body: { scenarioId: ScenarioId; variantId?: string } }>("/live/runs", {
  schema: { body: { type: "object", additionalProperties: false, required: ["scenarioId"], properties: { scenarioId: { type: "string", enum: liveScenarios.map(s => s.id) }, variantId: { type: "string", maxLength: 64 } } } },
}, (request, reply) => {
  if (closing) return reply.code(503).send({ error: "Runner is shutting down." });
  if (active?.status === "running") return reply.code(409).send({ error: "A scenario is already running. Wait for cleanup to finish.", runId: active.id });
  const { scenarioId, variantId = "" } = request.body;
  if (variantId && !liveVariants[scenarioId].some(v => v.id === variantId)) return reply.code(400).send({ error: "Unsupported scenario case." });
  if (scenarioId === "kafka-outage" && !link) return reply.code(503).send({ error: "Kafka control requires the Docker viewer's VIEWER listener." });
  const run: LiveRun = { id: randomUUID(), scenarioId, variantId, status: "running", startedAt: new Date().toISOString(), frames: [], checks: [], logs: [] };
  active = run;
  runs.set(run.id, run);
  if (runs.size > 10) runs.delete(runs.keys().next().value!);
  abort = new AbortController();
  const timer = setTimeout(() => abort?.abort(new Error("Scenario exceeded its 150-second execution limit.")), 150_000);
  task = execute(run, abort.signal, link).finally(() => clearTimeout(timer));
  return reply.code(202).send(run);
});
app.get<{ Params: { id: string } }>("/live/runs/:id", (request, reply) => {
  const run = runs.get(request.params.id);
  return run ?? reply.code(404).send({ error: "Run not retained. The viewer keeps its latest ten captures in memory." });
});
app.get("/*", async (request, reply) => {
  const pathname = decodeURIComponent(request.url.split("?")[0] ?? "/");
  const filename = resolve(dist, pathname === "/" ? "index.html" : `.${pathname}`);
  if (!filename.startsWith(resolve(dist) + sep)) return reply.code(404).send();
  try { return reply.type(mime[extname(filename)] ?? "application/octet-stream").send(await readFile(filename)); }
  catch { return reply.code(404).send({ error: "Asset unavailable. Build the frontend first." }); }
});
for (const signal of ["SIGINT", "SIGTERM"] as const) process.once(signal, () => {
  closing = true;
  abort?.abort(new Error("Runner shutting down."));
  void (async () => { await task; await link?.close(); await app.close(); })();
});
await app.listen({ host: process.env.VIEWER_HOST ?? "127.0.0.1", port: Number(process.env.VIEWER_PORT ?? 4173) });
