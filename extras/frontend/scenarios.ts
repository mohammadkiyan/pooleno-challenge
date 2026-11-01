export type NodeId =
  | "client"
  | "api"
  | "database"
  | "outbox"
  | "kafka"
  | "matching"
  | "worker"
  | "external";
export type PhaseId =
  | "api-validate"
  | "api-key"
  | "api-save"
  | "api-reply"
  | "match-guard"
  | "match-price"
  | "match-fill"
  | "match-save"
  | "match-book"
  | "match-restore"
  | "match-replay"
  | "publish-read"
  | "publish-send"
  | "publish-mark"
  | "publish-retry"
  | "notify-inbox"
  | "notify-send"
  | "notify-retry"
  | "notify-response"
  | "notify-complete";
export interface BookOrder {
  id: string;
  side: "BUY" | "SELL";
  price: string;
  quantity: string;
}
export interface Trade {
  taker?: string;
  maker: string;
  price: string;
  quantity: string;
}
export interface Frame {
  title: string;
  detail: string;
  active: NodeId[];
  blocked: NodeId[];
  parallel: NodeId[];
  book: BookOrder[];
  bookState?: "available" | "discarded";
  trades: Trade[];
  facts: [string, string][];
  request: string;
  response: string;
  requestLabel: string;
  decision: string;
  source: string;
  phase: PhaseId | null;
  transfers: [NodeId, NodeId][];
}
export interface Scenario {
  id: string;
  title: string;
  subtitle: string;
  category: "Matching" | "Reliability";
  description: string;
  guarantee: string;
  outcome: string;
  tags: string[];
  frames: Frame[];
}
const api = "src/modules/orders/";
const match = "src/modules/matching/";
const messaging = "src/modules/messaging/";
const external = "src/modules/external/";
const fixture: BookOrder[] = [
  { id: "S3", side: "SELL", price: "99.00", quantity: "1.0000" },
  { id: "S1", side: "SELL", price: "100.00", quantity: "2.0000" },
  { id: "S2", side: "SELL", price: "100.00", quantity: "3.0000" },
];
const remainder: BookOrder = {
  id: "S2",
  side: "SELL",
  price: "100.00",
  quantity: "2.0000",
};
const trades: Trade[] = [
  { maker: "S3", price: "99.00", quantity: "1.0000" },
  { maker: "S1", price: "100.00", quantity: "2.0000" },
  { maker: "S2", price: "100.00", quantity: "1.0000" },
];

// Authoring helpers preserve the last committed state; they do not execute backend logic.
export function next(
  frames: Frame[],
  title: string,
  detail: string,
  active: NodeId[],
  decision: string,
  source: string,
  changes: Partial<Frame> = {},
) {
  const previous = frames.at(-1);
  if (!previous) throw new Error("A journey must start with a request");
  frames.push({
    ...previous,
    title,
    detail,
    active,
    decision,
    source,
    phase: null,
    parallel: [],
    transfers: [],
    response: "",
    ...changes,
  });
}
export function accept(
  orderId: string,
  side: "BUY" | "SELL",
  price: string,
  quantity: string,
  precondition: string,
  book: BookOrder[] = [],
  blocked: NodeId[] = [],
) {
  const key = `demo-${orderId.toLowerCase()}-request-0001`;
  const request =
    `POST /v1/orders\nIdempotency-Key: ${key}\n\n` +
    JSON.stringify(
      { symbol: "ETH-USD", side, type: "LIMIT", price, quantity },
      null,
      2,
    );
  const receipt = `202 Accepted\n{ "orderId": "${orderId}", "status": "QUEUED" }`;
  const frames: Frame[] = [
    {
      title: `Client submits ${orderId}`,
      detail: precondition,
      active: ["client"],
      blocked,
      parallel: [],
      book,
      trades: [],
      facts: [
        ["Order", `${side} ${quantity} ETH`],
        ["Limit", `${price} USD`],
      ],
      request,
      response: "Not accepted yet.",
      requestLabel: `R1 · ${orderId} · ${side} ${quantity} @ ${price}`,
      decision: "A new order intention gets its own idempotency key.",
      source: api + "order.routes.ts → POST /v1/orders",
      phase: null,
      transfers: [],
    },
  ];
  next(
    frames,
    "Validate the incoming request",
    "The API validates headers and the body, checks the market, and converts decimal strings into exact integer units. This valid request passes.",
    ["client", "api"],
    "Valid price and quantity → continue; malformed or off-increment input would return 400.",
    api + "canonicalize-order.ts → canonicalizeOrder",
    { phase: "api-validate", transfers: [["client", "api"]] },
  );
  next(
    frames,
    "Claim the request key",
    "Fingerprint the normalized request and scope. In the acceptance transaction, claim this previously unused key.",
    ["api", "database"],
    "Unused key → create a new acceptance; an existing key would compare fingerprints.",
    api + "accept-order.ts → claimKey",
    { phase: "api-key", transfers: [["api", "database"]] },
  );
  next(
    frames,
    "Commit acceptance atomically",
    "Advance the symbol sequence and save the order, receipt and Outbox event together. PostgreSQL commits all of them or rolls everything back.",
    ["api", "database"],
    "Commit succeeds → it is safe to acknowledge the order.",
    api + "accept-order.ts → withTransaction",
    {
      phase: "api-save",
      transfers: [["api", "database"]],
      response: "COMMIT: order + receipt + sequence + Outbox event",
    },
  );
  next(
    frames,
    "Return 202 to the client",
    "The saved receipt returns to the caller. This confirms durable acceptance, not matching or notification. Async publication can start any time after commit; this is one illustrative interleaving.",
    ["api", "client"],
    "Return the receipt after commit; Kafka availability is not required.",
    api + "order.routes.ts → reply.code(status)",
    { phase: "api-reply", transfers: [["api", "client"]], response: receipt },
  );
  return frames;
}
export function publish(frames: Frame[]) {
  next(
    frames,
    "Read the earliest pending event",
    "The publisher selects the earliest unpublished event for this symbol from the database-backed Outbox.",
    ["database", "outbox"],
    "Preserve per-symbol order; do not skip an earlier pending event.",
    messaging + "publish-outbox.ts",
    { phase: "publish-read", transfers: [["database", "outbox"]] },
  );
  next(
    frames,
    "Send the acceptance event to Kafka",
    "Send the stored event keyed by symbol and wait for broker acknowledgement. Publication has not yet been marked in PostgreSQL.",
    ["outbox", "kafka"],
    "Broker acknowledges → mark published; an uncertain send may replay the same event.",
    messaging + "publish-outbox.ts",
    { phase: "publish-send", transfers: [["outbox", "kafka"]] },
  );
  next(
    frames,
    "Mark the acknowledged event published",
    "Kafka has acknowledged the send. Now record published_at in PostgreSQL. If this write fails, a successor can resend the same event and consumers deduplicate it.",
    ["outbox", "database"],
    "Only broker acknowledgement permits marking; no distributed transaction spans the database and Kafka.",
    messaging + "publish-outbox.ts → markPublished",
    { phase: "publish-mark" },
  );
}
export function guard(frames: Frame[]) {
  next(
    frames,
    "Dispatch to independent consumer groups",
    "Matching and notification receive the acceptance independently. Either branch may finish first. This walkthrough follows matching next.",
    ["kafka", "matching", "worker"],
    "Separate groups → a notification failure does not block matching.",
    messaging + "matching-consumer.ts",
    {
      parallel: ["matching", "worker"],
      transfers: [
        ["kafka", "matching"],
        ["kafka", "worker"],
      ],
    },
  );
  next(
    frames,
    "Deduplicate and check the sequence",
    "The matching transaction claims its Inbox event, locks progress, and checks the next acceptance sequence and the in-memory cursor.",
    ["matching", "database"],
    "Unseen event + next sequence → compute. A handled event is a no-op; a gap blocks.",
    match + "apply-accepted-order.ts",
    { phase: "match-guard", transfers: [["matching", "database"]] },
  );
}
function finishRest(frames: Frame[], order: BookOrder) {
  next(
    frames,
    "Commit the open order",
    "Persist the open order, Inbox and matching progress atomically. The in-memory book still represents the preceding committed state.",
    ["matching", "database"],
    "No executable maker → persist OPEN with its full remainder.",
    match + "apply-accepted-order.ts → persistMatch",
    { phase: "match-save", transfers: [["matching", "database"]] },
  );
  const before = frames.at(-1)!.book;
  next(
    frames,
    "Rest the order and advance the offset",
    "After the database commit, add the remainder to the in-memory book at its own limit. The consumer may then advance its offset.",
    ["matching"],
    "Rest the remaining quantity; never trade outside the limit.",
    match + "matching-engine.ts → applyMatch",
    {
      phase: "match-book",
      book: [...before, order],
      facts: [
        ["Order status", "OPEN"],
        ["Trades", "0"],
      ],
    },
  );
}

const matching = accept(
  "B1",
  "BUY",
  "100",
  "4",
  "Precondition: S1 and S2 were accepted and matched earlier at 100, in that order. Later S3 rested at 99. We now follow B1 from the client.",
  fixture,
);
publish(matching);
guard(matching);
next(
  matching,
  "Compare the best ask with B1’s limit",
  "The best ask is S3 at 99, below B1’s limit of 100. Price priority wins even though S3 arrived last.",
  ["matching"],
  "99 ≤ 100 → crossing is allowed; start with S3.",
  match + "matching-engine.ts → crosses",
  { phase: "match-price" },
);
next(
  matching,
  "Compute S3’s fill at 99",
  "Take min(4, 1) = 1 ETH from S3. B1 has 3 left to match. This is a computation plan; the committed book below has not changed.",
  ["matching"],
  "Best price first → 1 ETH executes at the maker’s price of 99.",
  match + "matching-engine.ts → matchOrder",
  {
    phase: "match-fill",
    facts: [
      ["Planned fill", "S3 · 1 @ 99"],
      ["B1 left to match", "3.0000"],
    ],
  },
);
next(
  matching,
  "Compute FIFO fills at 100",
  "At 100, S1 is older than S2. Plan 2 ETH from S1, then 1 ETH from S2. B1 is fully matched; S2 will have 2 left.",
  ["matching"],
  "Same price → FIFO. Fill only the smaller remaining quantity.",
  match + "matching-engine.ts → matchOrder",
  {
    phase: "match-fill",
    facts: [
      ["Planned fills", "S1: 2 · S2: 1"],
      ["S2 planned remainder", "2.0000"],
    ],
  },
);
next(
  matching,
  "Commit the match and keep the remainder",
  "Commit all three trades, maker/taker updates, Inbox and progress together. The RAM book is applied only after this transaction succeeds.",
  ["matching", "database"],
  "Atomic commit → no partially persisted match.",
  match + "apply-accepted-order.ts → persistMatch",
  { phase: "match-save", trades, transfers: [["matching", "database"]] },
);
next(
  matching,
  "Apply committed fills, then advance the offset",
  "Update the book after commit. S2 retains its original FIFO position with 2 ETH. Only after durable work and the book update can the consumer commit its Kafka offset.",
  ["matching"],
  "Keep S2’s queue position; no second trade on event redelivery.",
  match + "matching-engine.ts → applyMatch",
  {
    phase: "match-book",
    book: [remainder],
    trades,
    facts: [
      ["B1 status", "FILLED"],
      ["S2 remaining", "2.0000"],
    ],
    response:
      "B1: FILLED\nS2: PARTIALLY_FILLED\nTrades: 1 @ 99, 2 @ 100, 1 @ 100",
  },
);

const nonCross = accept(
  "B1",
  "BUY",
  "100",
  "1",
  "Precondition: a previously accepted sell order S1 is already resting at 101. Follow the new buy request B1.",
  [{ id: "S1", side: "SELL", price: "101.00", quantity: "2.0000" }],
);
publish(nonCross);
guard(nonCross);
next(
  nonCross,
  "The matcher checks the boundary",
  "The best ask is 101; B1 will pay at most 100. No maker can execute.",
  ["matching"],
  "101 > 100 → stop; never cross this price boundary.",
  match + "matching-engine.ts → crosses",
  {
    phase: "match-price",
    facts: [
      ["Comparison", "101 > 100"],
      ["Trades", "0"],
    ],
  },
);
finishRest(nonCross, {
  id: "B1",
  side: "BUY",
  price: "100.00",
  quantity: "1.0000",
});

function retry(conflict: boolean) {
  const frames = accept(
    "order-A",
    "SELL",
    "200",
    "1",
    "Precondition: this key has never been accepted. We first follow R1, then a second client request under the same key.",
  );
  next(
    frames,
    conflict
      ? "Client changes the quantity"
      : "Client retries the same intention",
    conflict
      ? "R2 asks for 2 ETH under R1’s key. This is a new HTTP request, not a new accepted order."
      : "R2 resends the same key with equivalent decimal spelling. This is a new HTTP request for the same order intention.",
    ["client"],
    "Retry must re-enter through validation; no silent updates to accepted orders.",
    api + "order.routes.ts",
    {
      requestLabel: conflict ? "R2 · changed order" : "R2 · retry same order",
      request: frames[0]!.request.replace(
        '"quantity": "1"',
        conflict ? '"quantity": "2"' : '"quantity": "1.0000"',
      ),
      facts: [
        ["HTTP requests", "2"],
        ["Accepted orders", "1"],
      ],
    },
  );
  next(
    frames,
    "Validate the second request",
    "The retry is structurally valid and is normalized before fingerprint comparison.",
    ["client", "api"],
    "Valid request → compare its normalized meaning.",
    api + "canonicalize-order.ts",
    { phase: "api-validate", transfers: [["client", "api"]] },
  );
  next(
    frames,
    "Compare the saved fingerprint",
    conflict
      ? "The key exists but the quantity changes its normalized meaning."
      : "The key exists and the normalized meaning matches R1.",
    ["api", "database"],
    conflict
      ? "Different fingerprint → reject; do not write a new order or event."
      : "Same fingerprint → load the exact saved receipt; skip new acceptance.",
    api + "accept-order.ts → replayReceipt",
    { phase: "api-key", transfers: [["api", "database"]] },
  );
  next(
    frames,
    conflict
      ? "Reject the changed request with 422"
      : "Replay the original receipt",
    conflict
      ? "Return IDEMPOTENCY_KEY_REUSED. The original order and receipt remain unchanged."
      : "Return R1’s exact original 202 receipt, even if the order has since progressed. No new order or acceptance event is created.",
    ["api", "client"],
    conflict
      ? "422: preserve original intention."
      : "Replay receipt: no second acceptance.",
    api + "accept-order.ts → replayReceipt",
    {
      phase: "api-reply",
      transfers: [["api", "client"]],
      response: conflict
        ? '422 Unprocessable Entity\n{ "code": "IDEMPOTENCY_KEY_REUSED" }'
        : '202 Accepted\n{ "orderId": "order-A", "status": "QUEUED" }',
      facts: [
        ["HTTP requests", "2"],
        ["Orders / events", "1 / 1"],
      ],
    },
  );
  return frames;
}
const lost = accept(
  "S1",
  "SELL",
  "200",
  "1",
  "Precondition: the demo destination is configured to apply its next notification and then drop the response. Follow the new order request S1.",
);
publish(lost);
next(
  lost,
  "Hand off to notification processing",
  "The external consumer receives the acceptance event independently of matching and saves an Inbox entry and durable delivery job.",
  ["kafka", "worker", "database"],
  "Persist the job before advancing this consumer’s offset.",
  external + "schedule-notification.ts",
  { phase: "notify-inbox", transfers: [["kafka", "worker"]] },
);
next(
  lost,
  "Send the notification",
  "The worker sends the job under external-notify:event-A. The destination stores one effect and its replay result.",
  ["worker", "external"],
  "Use a stable destination key for all attempts of this job.",
  external + "external-client.ts",
  {
    phase: "notify-send",
    requestLabel: "N1 · notification for S1",
    request: "POST /v1/notifications\nIdempotency-Key: external-notify:event-A",
    transfers: [["worker", "external"]],
    facts: [
      ["Delivery attempts", "1"],
      ["Destination effects", "1"],
    ],
  },
);
next(
  lost,
  "The response never arrives",
  "The effect exists remotely, but the worker cannot confirm it. Store a retry due time and leave the job pending. Matching can continue independently.",
  ["worker", "database"],
  "Unknown remote outcome → retry the same job and key; do not assume it failed remotely.",
  external + "notification-worker.ts",
  {
    phase: "notify-retry",
    blocked: ["external"],
    response: "Connection closed; local outcome uncertain",
  },
);
next(
  lost,
  "Retry with the original key",
  "When due, the worker retries the same payload and key. The destination replays its stored result without applying another effect.",
  ["worker", "external"],
  "Retry starts at the worker, not the client or Kafka.",
  external + "notification-worker.ts",
  {
    phase: "notify-send",
    blocked: [],
    transfers: [["worker", "external"]],
    facts: [
      ["Delivery attempts", "2"],
      ["Destination effects", "1"],
    ],
    response: "200 OK\nidempotent-replayed: true",
  },
);
next(
  lost,
  "Destination returns the saved result",
  "The destination returns its saved success response without applying another effect. This response travels back to the notification worker.",
  ["external", "worker"],
  "Acknowledge the original effect; do not create another one.",
  external + "external-client.ts",
  {
    phase: "notify-response",
    transfers: [["external", "worker"]],
    response: "200 OK\nidempotent-replayed: true",
  },
);
next(
  lost,
  "Record successful delivery",
  "After receiving confirmation, mark the durable job SUCCEEDED. A local Inbox alone could not resolve the lost-response gap; destination idempotency did.",
  ["worker", "database"],
  "Two attempts, one effect → delivery confirmed.",
  external + "external-job.repository.ts → markJobSucceeded",
  {
    phase: "notify-complete",
    facts: [
      ["Delivery attempts", "2"],
      ["Destination effects", "1"],
    ],
    response: "Job: SUCCEEDED\nDestination effects: 1",
  },
);

const outage = accept(
  "S1",
  "SELL",
  "200",
  "1",
  "Precondition: Kafka is offline; PostgreSQL and the API are available. Follow a new non-crossing sell request. ",
  [],
  ["kafka"],
);
next(
  outage,
  "Read pending work during the outage",
  "The publisher finds the committed event in the Outbox. Acceptance has already returned 202.",
  ["database", "outbox"],
  "Database work is durable even though Kafka is unavailable.",
  messaging + "publish-outbox.ts",
  { phase: "publish-read", transfers: [["database", "outbox"]] },
);
next(
  outage,
  "Publication cannot reach Kafka",
  "The publisher’s send fails. Keep the event pending, record the retry, and hold back later events for this symbol.",
  ["outbox"],
  "Send failure → retain and retry; never mark the event published.",
  messaging + "publish-outbox.ts",
  {
    phase: "publish-retry",
    facts: [
      ["Order status", "QUEUED"],
      ["Pending events", "1"],
    ],
  },
);
next(
  outage,
  "Kafka recovers; retry publication",
  "The earliest pending event is resent. Broker acknowledgement now permits the publisher to mark it published.",
  ["outbox", "kafka"],
  "Retry starts at the Outbox; no second client order is needed.",
  messaging + "publish-outbox.ts",
  { phase: "publish-send", blocked: [], transfers: [["outbox", "kafka"]] },
);
next(
  outage,
  "Mark the recovered event published",
  "After Kafka acknowledges, persist the publication marker. The accepted order itself was already durable.",
  ["outbox", "database"],
  "Broker ACK first, published_at second.",
  messaging + "publish-outbox.ts → markPublished",
  { phase: "publish-mark" },
);

guard(outage);
next(
  outage,
  "No opposing maker exists",
  "The restored book has no executable buy for S1. The full quantity must rest at its own limit.",
  ["matching"],
  "No crossing liquidity → OPEN, not a rejected order.",
  match + "matching-engine.ts → matchOrder",
  { phase: "match-price" },
);
finishRest(outage, {
  id: "S1",
  side: "SELL",
  price: "200.00",
  quantity: "1.0000",
});

export const scenarios: [Scenario, ...Scenario[]] = [
  {
    id: "matching",
    title: "Price, time & partial fills",
    subtitle: "Follow B1 through matching",
    category: "Matching",
    description:
      "Follow B1 from the client through acceptance, publication, price/FIFO decisions and committed fills.",
    guarantee:
      "Best price first. Oldest at that price next. Trades execute at the maker’s price.",
    outcome:
      "B1 fills 4 ETH in three trades. S2 keeps its queue position with 2 ETH remaining.",
    tags: ["Price priority", "FIFO", "Partial execution"],
    frames: matching,
  },
  {
    id: "non-crossing",
    title: "Prices that never cross",
    subtitle: "A valid order need not trade",
    category: "Matching",
    description:
      "Follow a buy at 100 while a preloaded seller asks 101. See exactly why the order rests.",
    guarantee: "Buy only at or below the limit; sell only at or above it.",
    outcome:
      "Both orders stay OPEN. The spread is 1.00 and no trade is created.",
    tags: ["Limit boundary", "No execution"],
    frames: nonCross,
  },
  {
    id: "duplicate",
    title: "One order, two requests",
    subtitle: "Follow acceptance, then a retry",
    category: "Reliability",
    description:
      "Follow R1’s acceptance, then R2’s client retry under the same key and meaning.",
    guarantee: "Same key and normalized meaning replay the original receipt.",
    outcome: "Two HTTP requests resolve to one order and one acceptance event.",
    tags: ["Idempotency", "Safe retry"],
    frames: retry(false),
  },
  {
    id: "conflict",
    title: "Same key, different order",
    subtitle: "Follow a conflicting retry",
    category: "Reliability",
    description:
      "Follow R1’s acceptance, then see why a changed quantity under the same key returns 422.",
    guarantee:
      "An idempotency key cannot change an accepted request’s meaning.",
    outcome:
      "The changed request receives 422. The original order and receipt remain unchanged.",
    tags: ["Conflict detection", "422 response"],
    frames: retry(true),
  },
  {
    id: "lost-response",
    title: "Delivered. Response lost.",
    subtitle: "Follow one uncertain delivery",
    category: "Reliability",
    description:
      "Follow a new order from the client, then follow its notification through a lost response and safe retry.",
    guarantee:
      "Stable delivery keys plus destination deduplication preserve one effect.",
    outcome:
      "Two delivery attempts produce one destination effect. The job eventually succeeds.",
    tags: ["Uncertain outcome", "Destination idempotency"],
    frames: lost,
  },
  {
    id: "kafka-outage",
    title: "Kafka pauses. Orders don’t.",
    subtitle: "Follow accepted work through recovery",
    category: "Reliability",
    description:
      "Follow a client request while Kafka is offline, then see why the Outbox retry resumes processing safely.",
    guarantee:
      "A confirmed order survives the broker outage; publication keeps per-symbol order.",
    outcome:
      "The pending acceptance event publishes after recovery, and the order eventually becomes OPEN.",
    tags: ["Outbox", "Recovery", "Ordering"],
    frames: outage,
  },
];
export const firstScenario = scenarios[0];
