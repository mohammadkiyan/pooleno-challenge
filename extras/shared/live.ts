import type { Frame } from "../frontend/flow-types.ts";

export const liveScenarios = [
  { id: "matching", title: "Price, time & partial fills", category: "Matching", description: "Submit resting makers, then a BUY taker. Verify price priority, FIFO and persisted partial fills." },
  { id: "non-crossing", title: "Prices that never cross", category: "Matching", description: "Submit a seller at 101 and a buyer at 100. Read both orders back and verify zero executions." },
  { id: "duplicate", title: "One order, two requests", category: "Reliability", description: "Repeat a real HTTP request under the same key. Compare receipts and database counts." },
  { id: "conflict", title: "Same key, different order", category: "Reliability", description: "Change a request under an accepted key and observe the actual API rejection." },
  { id: "lost-response", title: "Delivered. Response lost.", category: "Reliability", description: "Make the destination apply a notification and drop its response. Observe the real worker retry." },
  { id: "kafka-outage", title: "Kafka connection interrupted", category: "Reliability", description: "Disconnect this sandbox from Kafka, accept an order, inspect its pending Outbox, reconnect and verify recovery." },
] as const;
export type ScenarioId = typeof liveScenarios[number]["id"];
export const liveVariants: Record<ScenarioId, { id: string; label: string }[]> = {
  matching: [
    { id: "sell-taker", label: "SELL taker: highest bid, then FIFO" },
    { id: "partial-taker", label: "Partially filled taker rests at its limit" },
    { id: "crash-replay", label: "Kill matcher, restore and redeliver committed event" },
  ],
  "non-crossing": [],
  duplicate: [{ id: "concurrent-retries", label: "25 concurrent requests under one key" }],
  conflict: [
    { id: "invalid-precision", label: "Invalid price precision → 400, no writes" },
    { id: "invalid-quantity", label: "Zero quantity → 400, no writes" },
  ],
  "lost-response": [
    { id: "temporary-503", label: "503 + Retry-After → retry → success" },
    { id: "permanent-redrive", label: "Permanent 400 → FAILED → CLI redrive" },
    { id: "retry-exhaustion", label: "Two-attempt sandbox budget → FAILED" },
  ],
  "kafka-outage": [],
};
export interface ActualOrder {
  orderId: string;
  symbol: string;
  side: "BUY" | "SELL";
  price: string;
  status: string;
  remainingQuantity: string;
  acceptanceSequence: string;
  trades: { tradeId: string; role: string; price: string; quantity: string }[];
}
export interface Snapshot {
  orders: ActualOrder[];
  trades: { tradeId: string; makerId: string; takerId: string; price: string; quantity: string }[];
  outbox: { eventId: string; orderId: string; publishedAt: string | null; attempts: number; lastError: string | null }[];
  jobs: { eventId: string; orderId: string; targetKey: string; status: string; attempts: number; nextAttemptAt: string | null; lastError: string | null }[];
  effects: { idempotencyKey: string; body: string }[];
  inbox: { consumer_name: string; event_id: string }[];
  counts: { orders: number; events: number; receipts: number; trades: number };
}
export interface LiveFrame extends Frame {
  observedAt: string;
  snapshot: Snapshot;
}
export interface LiveRun {
  id: string;
  scenarioId: ScenarioId;
  variantId: string;
  status: "running" | "passed" | "failed";
  startedAt: string;
  finishedAt?: string;
  frames: LiveFrame[];
  checks: { claim: string; passed: boolean; actual: unknown; expected: unknown }[];
  logs: { role: string; line: string }[];
  error?: string;
  cleanupError?: string;
}
